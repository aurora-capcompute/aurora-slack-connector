package connector

// One mention, one aurora process: build its input from the thread, start
// it, keep its status message current, and post how it ended.

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// handleMessage runs a mention as a process and reports its progress back into
// the thread. It blocks the worker until the process finishes, which is exactly
// the per-thread serialization we want.
func (c *Connector) handleMessage(ctx context.Context, t *thread, w work) {
	input, err := c.threadInput(ctx, t, w)
	if err != nil {
		c.logger.Warn("read thread", "thread", t.threadTS, "error", err)
		if _, perr := c.slack.PostMessage(ctx, c.cfg.ChannelID, t.threadTS,
			"❌ I couldn't read this thread: "+oneLine(err.Error())); perr != nil {
			c.logger.Warn("post thread error", "error", perr)
		}
		return
	}
	if input == "" {
		return // a bare mention with nothing before it in the thread
	}
	statusTS, err := c.slack.PostMessage(ctx, c.cfg.ChannelID, t.threadTS, statusHeader(StatusQueued, false))
	if err != nil {
		c.logger.Warn("post status message", "thread", t.threadTS, "error", err)
		// Without a status message we can still run; progress just won't render.
	}

	proc, err := c.spawn(ctx, t.sessionID, input)
	if err != nil {
		c.report(ctx, t, statusTS, "❌ I couldn't start the investigation: "+oneLine(err.Error()))
		return
	}
	c.pollProcess(ctx, t, proc.ID, statusTS)
}

// threadInput is a mention's process input: the thread's messages since the
// bot was last mentioned there, whole, then the request. The thread is read
// from Slack at the mention, so a connector restart loses none of it. The bot's
// own posts are left out, because its answers are already in the session's
// history. The previous mention's input carried everything before it.
func (c *Connector) threadInput(ctx context.Context, t *thread, w work) (string, error) {
	messages, err := c.slack.Replies(ctx, c.cfg.ChannelID, t.threadTS)
	if err != nil {
		return "", err
	}
	var since []string
	for _, m := range messages {
		if !tsBefore(m.TS, w.ts) {
			break
		}
		if c.botUserID != "" && m.User == c.botUserID {
			continue
		}
		if c.mentionsBot(m.Text) {
			since = since[:0]
			continue
		}
		since = append(since, fmt.Sprintf("[%s] %s: %s", tsTime(m.TS), author(m), m.Text))
	}
	if len(since) == 0 {
		return w.text, nil
	}
	request := w.text
	if request == "" {
		request = "(none: you were mentioned to look at the thread)"
	}
	return "Thread messages since you were last mentioned:\n" + strings.Join(since, "\n") + "\n\nRequest: " + request, nil
}

// author names a thread message's sender: a user mention, or a bot's name.
func author(m slackMessage) string {
	switch {
	case m.User != "":
		return "<@" + m.User + ">"
	case m.Username != "":
		return m.Username
	default:
		return "bot " + m.BotID
	}
}

// tsBefore orders two Slack message timestamps ("1700000000.000100").
func tsBefore(a, b string) bool {
	aSec, aFrac, _ := strings.Cut(a, ".")
	bSec, bFrac, _ := strings.Cut(b, ".")
	if len(aSec) != len(bSec) {
		return len(aSec) < len(bSec)
	}
	if aSec != bSec {
		return aSec < bSec
	}
	return aFrac < bFrac
}

// tsTime renders a Slack message timestamp as a UTC time.
func tsTime(ts string) string {
	sec, _, _ := strings.Cut(ts, ".")
	n, err := strconv.ParseInt(sec, 10, 64)
	if err != nil {
		return ts
	}
	return time.Unix(n, 0).UTC().Format(time.RFC3339)
}

// handleRetry resumes an interrupted process and follows it to its outcome, like
// a new message.
func (c *Connector) handleRetry(ctx context.Context, t *thread, processID string) {
	statusTS, err := c.slack.PostMessage(ctx, c.cfg.ChannelID, t.threadTS, statusHeader(StatusQueued, false))
	if err != nil {
		c.logger.Warn("post status message", "thread", t.threadTS, "error", err)
	}
	if _, err := c.aurora.Retry(ctx, processID); err != nil {
		c.report(ctx, t, statusTS, "❌ I couldn't retry the investigation: "+oneLine(err.Error()))
		return
	}
	c.pollProcess(ctx, t, processID, statusTS)
}

// spawn creates the process, tolerating a transient "session already has an
// active process" conflict (e.g. a process resumed after a restart) by waiting
// for the session to go idle and retrying.
func (c *Connector) spawn(ctx context.Context, sessionID, text string) (ProcessSnapshot, error) {
	deadline := time.Now().Add(c.cfg.ProcessTimeout)
	for {
		proc, err := c.aurora.CreateProcess(ctx, sessionID, text, c.cfg.Manifest)
		if err == nil {
			return proc, nil
		}
		if !IsConflict(err) || !time.Now().Before(deadline) {
			return ProcessSnapshot{}, err
		}
		if !c.waitSessionIdle(ctx, sessionID, deadline) {
			return ProcessSnapshot{}, err
		}
	}
}

// waitSessionIdle polls until the session has no active process (or the deadline
// or context expires). Returns true if the session became idle.
func (c *Connector) waitSessionIdle(ctx context.Context, sessionID string, deadline time.Time) bool {
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
		if !time.Now().Before(deadline) {
			return false
		}
		log, err := c.aurora.GetSessionLog(ctx, sessionID)
		if err != nil {
			c.logger.Warn("wait session idle", "session", sessionID, "error", err)
			continue
		}
		if log.Session.ActiveProcessID == "" {
			return true
		}
	}
}

// pollProcess watches a running process and keeps the thread's status message
// current: the cheap process poll drives the loop, and the session log is
// fetched when the journal grows, the process is parked on a task, or it ends —
// both to render the syscall timeline and to surface human-in-the-loop
// approvals. On a terminal status it posts the final answer or error.
func (c *Connector) pollProcess(ctx context.Context, t *thread, processID, statusTS string) {
	deadline := time.Now().Add(c.cfg.ProcessTimeout)
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()

	lastJournalLen := -1
	lastText := ""
	var entries []JournalEntry
	var tasks []Task
	prompted := map[string]bool{} // task ids we've already posted an approval prompt for

	for {
		snap, err := c.aurora.GetProcess(ctx, processID)
		if err != nil {
			c.logger.Warn("poll process", "process", processID, "error", err)
		} else {
			// Refresh the log when the journal grows or the process ends. Parking
			// on a task (approval or timer) always appends its syscall's journal
			// entry, and aurora reports the parked status only once that entry —
			// and the task — are committed, so a journal-length change catches a
			// new approval without polling the whole log every tick during a wait.
			// The first poll always refreshes (lastJournalLen starts at -1).
			if snap.JournalLength != lastJournalLen || IsTerminal(snap.Status) {
				if log, lerr := c.aurora.GetSessionLog(ctx, t.sessionID); lerr != nil {
					c.logger.Warn("fetch session log", "session", t.sessionID, "error", lerr)
				} else if pl := log.Process(processID); pl != nil {
					entries = pl.Entries
					tasks = pl.Tasks
				}
				lastJournalLen = snap.JournalLength
			}

			// Surface any pending human approvals as interactive prompts (once
			// each), and hold the timeout open while a human is deciding — a
			// person taking their time is not the connector stalling.
			awaiting := false
			for _, tk := range tasks {
				if !tk.NeedsHumanDecision() {
					continue
				}
				awaiting = true
				if !prompted[tk.ID] {
					c.postApprovalPrompt(ctx, t, tk)
					prompted[tk.ID] = true
				}
			}
			if awaiting {
				deadline = time.Now().Add(c.cfg.ProcessTimeout)
			}

			running := !IsTerminal(snap.Status)
			if text := renderStatus(statusHeader(snap.Status, awaiting), entries, running); text != lastText && statusTS != "" {
				if err := c.slack.UpdateMessage(ctx, c.cfg.ChannelID, statusTS, text); err != nil {
					c.logger.Warn("update status", "error", err)
				} else {
					lastText = text
				}
			}
			if IsTerminal(snap.Status) {
				c.postOutcome(ctx, t, snap)
				return
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if !time.Now().Before(deadline) {
			if _, err := c.slack.PostMessage(ctx, c.cfg.ChannelID, t.threadTS,
				"⏳ This is taking longer than expected — I'll stop live updates, but the investigation is still running in aurora. Send another message and I'll check back in."); err != nil {
				c.logger.Warn("post timeout notice", "error", err)
			}
			return
		}
	}
}

// postOutcome posts the process's final result as a fresh message in the thread.
func (c *Connector) postOutcome(ctx context.Context, t *thread, snap ProcessSnapshot) {
	var msg string
	switch snap.Status {
	case StatusCompleted:
		if answer := strings.TrimSpace(snap.Answer); answer != "" {
			msg = answer
		} else {
			msg = "✅ Done — but the investigation returned no written answer."
		}
	case StatusFailed:
		// A failed LLM call leaves its section open, so a resume calls the LLM
		// again: offer the retry here too.
		c.postWithRetry(ctx, t, snap, "❌ The investigation failed: "+oneLine(snap.Error)+". Retry resumes it.")
		return
	case StatusStopped:
		msg = "🛑 The investigation was stopped."
	case StatusInterrupted:
		c.postWithRetry(ctx, t, snap, "⏸️ The investigation stopped before it finished: it ran past the server's time limit, or the server restarted. Retry continues from where it stopped.")
		return
	default:
		return
	}
	if _, err := c.slack.PostMessage(ctx, c.cfg.ChannelID, t.threadTS, msg); err != nil {
		c.logger.Warn("post outcome", "thread", t.threadTS, "error", err)
	}
}

// postWithRetry posts text about a process that ended without an answer, with
// a Retry button that resumes the process.
func (c *Connector) postWithRetry(ctx context.Context, t *thread, snap ProcessSnapshot, text string) {
	value, _ := json.Marshal(buttonValue{Process: snap.ID, Session: t.sessionID, Thread: t.threadTS})
	blocks := []map[string]any{
		{"type": "section", "text": map[string]any{"type": "mrkdwn", "text": text}},
		{"type": "actions", "block_id": "duty_retry", "elements": []map[string]any{
			button(actionRetry, "Retry", "primary", string(value)),
		}},
	}
	if _, err := c.slack.PostBlockMessage(ctx, c.cfg.ChannelID, t.threadTS, text, blocks); err != nil {
		c.logger.Warn("post retry notice", "thread", t.threadTS, "error", err)
	}
}

// report updates the status message if we have one, else posts a new message.
func (c *Connector) report(ctx context.Context, t *thread, statusTS, text string) {
	if statusTS != "" {
		if err := c.slack.UpdateMessage(ctx, c.cfg.ChannelID, statusTS, text); err == nil {
			return
		}
	}
	if _, err := c.slack.PostMessage(ctx, c.cfg.ChannelID, t.threadTS, text); err != nil {
		c.logger.Warn("report", "thread", t.threadTS, "error", err)
	}
}
