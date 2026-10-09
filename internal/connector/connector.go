package connector

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// inboxBuffer bounds the messages queued for one thread's worker.
	inboxBuffer = 64
	// maxMessageBytes caps one message from Slack.
	maxMessageBytes = 1 << 20
	// seenCapacity bounds the event-dedup memory.
	seenCapacity = 4096
	// idleTimeout retires a thread worker after a spell of no messages; its
	// aurora session survives and is reattached by name on the next message.
	idleTimeout = 30 * time.Minute
)

// Connector wires Slack to a local aurora-dist. It receives Slack events over a
// Socket Mode connection, maps each thread to an aurora session, and runs each
// mention of the bot as a process (an "aurora spawn") in that session —
// serialized per thread so the session's single-active-process rule holds and
// history is shared across mentions. While a process runs it polls aurora and
// narrates the syscalls back into the thread.
type Connector struct {
	cfg       Config
	aurora    *AuroraClient
	slack     *SlackClient
	botUserID string
	logger    *slog.Logger

	ctx context.Context

	mu      sync.Mutex
	threads map[string]*thread
	seen    *seenSet

	// mentionRE matches a Slack user mention, e.g. "<@U123>". The bot's own is
	// stripped from a mention before it becomes aurora input.
	mentionRE *regexp.Regexp
}

// thread is one Slack thread's live state: the aurora session serving it and a
// mailbox its worker goroutine drains one item at a time.
type thread struct {
	threadTS  string
	sessionID string
	inbox     chan work
}

// work is one item for a thread's worker: a mention to run as a new process, or
// an ended process to retry.
type work struct {
	// text is the mention's request, without the bot's mention, and ts the
	// mention message's timestamp.
	text    string
	ts      string
	retryID string
}

// New builds a Connector from config and its two clients.
func New(cfg Config, aurora *AuroraClient, slack *SlackClient, logger *slog.Logger) *Connector {
	return &Connector{
		cfg:       cfg,
		aurora:    aurora,
		slack:     slack,
		botUserID: cfg.BotUserID,
		logger:    logger,
		// Start installs the cancellable context.
		ctx:     context.Background(),
		threads: make(map[string]*thread),
		seen:    newSeenSet(seenCapacity),
		// A Slack user mention: "<@U123>" or the labelled form "<@U123|name>".
		mentionRE: regexp.MustCompile(`<@[A-Z0-9]+(?:\|[^>]*)?>`),
	}
}

// Start resolves the bot's own identity (best effort) and records the base
// context whose cancellation retires all workers.
func (c *Connector) Start(ctx context.Context) {
	c.ctx = ctx
	if c.botUserID == "" {
		if id, err := c.slack.AuthTest(ctx); err != nil {
			c.logger.Warn("slack auth.test failed; self-message filtering relies on bot_id only", "error", err)
		} else {
			c.botUserID = id
			c.logger.Info("resolved bot identity", "bot_user_id", id)
		}
	}
}

// --- Slack event ingestion ---

// slackEnvelope is the payload of an events_api message: an event_callback
// that wraps one event.
type slackEnvelope struct {
	Type  string     `json:"type"`
	Event slackEvent `json:"event"`
}

type slackEvent struct {
	Type     string `json:"type"`
	Subtype  string `json:"subtype"`
	User     string `json:"user"`
	BotID    string `json:"bot_id"`
	Text     string `json:"text"`
	TS       string `json:"ts"`
	ThreadTS string `json:"thread_ts"`
	Channel  string `json:"channel"`
}

// dispatchEvent filters an inbound event to the mentions this duty bot acts on
// and routes each to its thread's worker. Only a direct mention of the bot
// starts work, and Slack sends an app_mention event for exactly those. Other
// messages in a thread reach aurora when the bot is next mentioned there: the
// mention's input carries them (see threadInput).
func (c *Connector) dispatchEvent(env slackEnvelope) {
	ev := env.Event
	// A duty bot owns exactly one channel. Edits, every other message subtype
	// and anything a bot posted (ourselves included) are ignored, to avoid loops.
	if ev.Type != "app_mention" || ev.Channel != c.cfg.ChannelID ||
		ev.Subtype != "" || ev.BotID != "" || ev.User == "" || ev.User == c.botUserID {
		return
	}
	// Dedup: Slack delivers an envelope again until it is acknowledged, and the
	// copy carries the same (channel, ts).
	if !c.seen.add(ev.Channel + ":" + ev.TS) {
		return
	}
	// A reply carries thread_ts; a top-level mention opens a new thread keyed by
	// its own ts.
	threadTS := ev.ThreadTS
	if threadTS == "" {
		threadTS = ev.TS
	}
	c.deliver(threadTS, work{text: c.cleanText(ev.Text), ts: ev.TS})
}

// deliver routes a mention to its thread's worker, creating the thread's aurora
// session or reattaching it by name.
func (c *Connector) deliver(threadTS string, w work) {
	// Fast path: a live worker already serves this thread.
	c.mu.Lock()
	if t, ok := c.threads[threadTS]; ok {
		t.submit(w, c.logger)
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	sessionID, err := c.aurora.EnsureSession(c.ctx, sessionName(threadTS), map[string]string{
		"source":    "slack",
		"channel":   c.cfg.ChannelID,
		"thread_ts": threadTS,
	})
	if err != nil {
		c.logger.Error("ensure session", "thread", threadTS, "error", err)
		if _, perr := c.slack.PostMessage(c.ctx, c.cfg.ChannelID, threadTS,
			"❌ I couldn't reach aurora to start a session: "+oneLine(err.Error())); perr != nil {
			c.logger.Warn("post session error", "error", perr)
		}
		return
	}
	c.startWorker(threadTS, sessionID, w)
}

// startWorker registers a thread and launches its worker, or hands the item to
// an existing worker if one raced into place.
func (c *Connector) startWorker(threadTS, sessionID string, first work) {
	c.mu.Lock()
	if t, ok := c.threads[threadTS]; ok {
		t.submit(first, c.logger)
		c.mu.Unlock()
		return
	}
	t := &thread{threadTS: threadTS, sessionID: sessionID, inbox: make(chan work, inboxBuffer)}
	c.threads[threadTS] = t
	t.submit(first, c.logger)
	c.mu.Unlock()

	c.logger.Info("serving thread", "thread", threadTS, "session", sessionID)
	go c.runThread(t)
}

// submit enqueues an item for the worker without blocking the caller; a full
// mailbox drops the item with a warning rather than stalling event handling.
func (t *thread) submit(w work, logger *slog.Logger) {
	select {
	case t.inbox <- w:
	default:
		logger.Warn("thread inbox full; dropping message", "thread", t.threadTS)
	}
}

// runThread drains a thread's mailbox one message at a time — the serialization
// that keeps at most one active process per session and lets each message see
// the history of the ones before it.
func (c *Connector) runThread(t *thread) {
	idle := time.NewTimer(idleTimeout)
	defer idle.Stop()
	for {
		select {
		case w := <-t.inbox:
			idle.Reset(idleTimeout)
			if w.retryID != "" {
				c.handleRetry(c.ctx, t, w.retryID)
			} else {
				c.handleMessage(c.ctx, t, w)
			}
		case <-idle.C:
			// Retire only when nothing is queued, under the lock so a concurrent
			// submit cannot be lost to a dead worker.
			c.mu.Lock()
			if len(t.inbox) > 0 {
				c.mu.Unlock()
				idle.Reset(idleTimeout)
				continue
			}
			delete(c.threads, t.threadTS)
			c.mu.Unlock()
			c.logger.Info("retired idle thread worker", "thread", t.threadTS, "session", t.sessionID)
			return
		case <-c.ctx.Done():
			return
		}
	}
}

// --- one message → one aurora process ---

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

// --- text helpers ---

// cleanText strips the bot's own mention — the bytes that reach aurora are the
// human's actual request. Other mentions stay: they name people. Horizontal
// whitespace within a line is collapsed, but line breaks are preserved so a
// pasted log or stack trace keeps its structure.
func (c *Connector) cleanText(text string) string {
	return normalizeInput(c.mentionRE.ReplaceAllStringFunc(text, func(mention string) string {
		if c.isBotMention(mention) {
			return " "
		}
		return mention
	}))
}

// mentionsBot reports whether text contains a mention of this bot.
func (c *Connector) mentionsBot(text string) bool {
	for _, mention := range c.mentionRE.FindAllString(text, -1) {
		if c.isBotMention(mention) {
			return true
		}
	}
	return false
}

// isBotMention reports whether one mention ("<@U123>" or the labelled
// "<@U123|name>") names this bot.
func (c *Connector) isBotMention(mention string) bool {
	return c.botUserID != "" &&
		(mention == "<@"+c.botUserID+">" || strings.HasPrefix(mention, "<@"+c.botUserID+"|"))
}

func sessionName(threadTS string) string { return "slack:" + threadTS }

func statusHeader(status string, awaitingApproval bool) string {
	if awaitingApproval {
		return "⏸️ *Duty bot — waiting for your approval*"
	}
	switch status {
	case StatusCompleted:
		return "✅ *Duty bot — done*"
	case StatusFailed:
		return "❌ *Duty bot — failed*"
	case StatusStopped:
		return "🛑 *Duty bot — stopped*"
	case StatusInterrupted:
		return "⏸️ *Duty bot — interrupted*"
	case StatusWaitingTask:
		return "🛎️ *Duty bot — working* _(waiting on a timer)_"
	default:
		return "🛎️ *Duty bot — working…*"
	}
}

// normalizeInput collapses horizontal whitespace within each line but keeps the
// line breaks, then trims — so multi-line input reaches aurora intact.
func normalizeInput(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.Join(strings.Fields(line), " ")
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// truncate bounds a string to limit runes (not bytes, so it never splits a
// multi-byte rune), appending an ellipsis when it cuts.
func truncate(s string, limit int) string {
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit]) + "…"
}

// oneLine flattens every run of whitespace to one space and bounds the result,
// for an error or detail on a chat line.
func oneLine(s string) string {
	return cmp.Or(truncate(strings.Join(strings.Fields(s), " "), 500), "(no detail)")
}
