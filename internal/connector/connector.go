package connector

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Reaction emoji the bot uses to acknowledge a message it is working on: eyes
// while it runs, then a check on success or a cross on any non-success terminal.
const (
	reactionWorking = "eyes"
	reactionDone    = "white_check_mark"
	reactionFailed  = "x"
)

const (
	// inboxBuffer bounds the messages queued for one thread's worker.
	inboxBuffer = 64
	// maxBodyBytes caps an inbound Slack request body.
	maxBodyBytes = 1 << 20
	// seenCapacity bounds the event-dedup memory.
	seenCapacity = 4096
	// idleTimeout retires a thread worker after a spell of no messages; its
	// aurora session survives and is reattached by name on the next message.
	idleTimeout = 30 * time.Minute
)

// Connector wires Slack to a local aurora-dist. It receives Slack events over a
// Socket Mode WebSocket, maps each thread to an aurora session, and runs each
// user message as a process (an "aurora spawn") in that session — serialized per
// thread so the session's single-active-process rule holds and history is shared
// across messages. While a process runs it polls aurora and narrates the
// syscalls back into the thread.
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

	// mentionRE matches a Slack user mention, e.g. "<@U123>" — stripped from a
	// message before it becomes aurora input.
	mentionRE *regexp.Regexp
}

// thread is one Slack thread's live state: the aurora session serving it and a
// mailbox its worker goroutine drains one message at a time.
type thread struct {
	threadTS  string
	sessionID string
	inbox     chan inboundMsg
}

// inboundMsg is one message routed to a thread worker: the cleaned text that
// becomes the aurora process input, plus the ts of the originating Slack message
// so the worker can acknowledge it with reactions (👀 → ✅/❌).
type inboundMsg struct {
	text      string
	messageTS string
}

// New builds a Connector from config and its two clients.
func New(cfg Config, aurora *AuroraClient, slack *SlackClient, logger *slog.Logger) *Connector {
	if logger == nil {
		logger = slog.Default()
	}
	return &Connector{
		cfg:       cfg,
		aurora:    aurora,
		slack:     slack,
		botUserID: cfg.BotUserID,
		logger:    logger,
		// Background until Start installs the cancellable context, so an event
		// that somehow arrives before Start never dereferences a nil context.
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

// Run opens the Socket Mode connection and delivers events and interactions into
// the connector until ctx is cancelled. Start must have been called first (it
// records the base context and resolves the bot identity).
func (c *Connector) Run(ctx context.Context) {
	sm := NewSocketMode(c.cfg.SlackAppToken, c.cfg.HTTPTimeout, c.logger, c.handleEventPayload, c.handleInteractionPayload)
	sm.SetAPIBaseURL(c.cfg.SlackAPIBaseURL)
	sm.Run(ctx)
}

// Handler returns the connector's HTTP mux — a health check only. Events arrive
// over the outbound Socket Mode connection (see Run), not an inbound endpoint.
func (c *Connector) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// --- Slack event ingestion ---

type slackEnvelope struct {
	Type    string     `json:"type"`
	EventID string     `json:"event_id"`
	Event   slackEvent `json:"event"`
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
	// Reaction events (reaction_added / reaction_removed).
	Reaction string       `json:"reaction"`
	Item     reactionItem `json:"item"`
}

// reactionItem is the target of a reaction event — the message it was added to.
type reactionItem struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	TS      string `json:"ts"`
}

// handleEventPayload receives one Socket Mode events_api payload (the standard
// Events API body) and routes it. The Socket Mode loop has already acknowledged
// the envelope, so the actual work runs on its own goroutine — a slow aurora
// call must not stall the socket reader.
func (c *Connector) handleEventPayload(payload []byte) {
	var env slackEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		c.logger.Warn("decode event payload", "error", err)
		return
	}
	if env.Type != "event_callback" {
		return
	}
	go c.dispatchEvent(env)
}

// dispatchEvent filters an inbound event to the messages this duty bot acts on
// and routes it to a thread worker.
func (c *Connector) dispatchEvent(env slackEnvelope) {
	ev := env.Event

	// A trigger reaction on a message starts an investigation of that message.
	if ev.Type == "reaction_added" {
		c.handleReaction(ev)
		return
	}
	// reaction_removed is subscribed for completeness but drives no behavior.
	if ev.Type == "reaction_removed" {
		return
	}

	// Otherwise, only the two message event kinds we act on.
	if ev.Type != "app_mention" && ev.Type != "message" {
		return
	}
	// A duty bot owns exactly one channel.
	if ev.Channel != c.cfg.ChannelID {
		return
	}
	// Ignore edits/deletions/joins and every other message subtype; ignore
	// anything posted by a bot (including ourselves) to avoid loops.
	if ev.Subtype != "" || ev.BotID != "" {
		return
	}
	if c.botUserID != "" && ev.User == c.botUserID {
		return
	}
	if ev.User == "" || strings.TrimSpace(ev.Text) == "" {
		return
	}

	// Dedup: a single user message can arrive as both app_mention and message,
	// and Slack retries duplicate deliveries — all share (channel, ts).
	if !c.seen.add(ev.Channel + ":" + ev.TS) {
		return
	}

	// A threaded reply carries thread_ts; a top-level message opens a new thread
	// keyed by its own ts.
	isReply := ev.ThreadTS != ""
	threadTS := ev.ThreadTS
	if !isReply {
		threadTS = ev.TS
	}
	text := c.cleanText(ev.Text)
	if text == "" {
		return
	}
	// Decide "trigger" from the message text (a native bot mention or the
	// keyword), not the event type. Slack delivers one native @-mention as both
	// an app_mention and a message with the same (channel, ts); dedup keeps only
	// one, so a type-dependent decision would fire nondeterministically. The
	// app_mention type is kept only as a fallback for when we couldn't resolve
	// our own user id.
	trigger := c.mentionsBot(ev.Text) || c.hasTrigger(ev.Text) ||
		(c.botUserID == "" && ev.Type == "app_mention")
	c.deliver(threadTS, text, ev.TS, trigger, isReply)
}

// handleReaction starts an investigation when the configured trigger emoji is
// added to a channel message: it reads that message and runs its text as a duty
// process in a thread rooted at it. A reaction on a message the bot can't read
// (a thread reply, since conversations.history returns only top-level messages)
// or on a bot/system message is ignored.
func (c *Connector) handleReaction(ev slackEvent) {
	if c.cfg.TriggerReaction == "" || ev.Reaction != c.cfg.TriggerReaction {
		return
	}
	if ev.Item.Type != "message" || ev.Item.Channel != c.cfg.ChannelID {
		return
	}
	if c.botUserID != "" && ev.User == c.botUserID {
		return // the bot's own acknowledgement reaction, not a human trigger
	}
	// Only the first trigger reaction on a message starts an investigation;
	// further reactions (by anyone) on the same message are no-ops.
	if !c.seen.add("react:" + ev.Item.Channel + ":" + ev.Item.TS) {
		return
	}
	msg, found, err := c.slack.GetMessage(c.ctx, ev.Item.Channel, ev.Item.TS)
	if err != nil {
		c.logger.Warn("reaction: fetch message", "ts", ev.Item.TS, "error", err)
		return
	}
	if !found {
		c.logger.Info("reaction trigger on an unreadable message (e.g. a thread reply); skipping", "ts", ev.Item.TS)
		return
	}
	if msg.BotID != "" || msg.Subtype != "" || (c.botUserID != "" && msg.User == c.botUserID) {
		return // don't investigate bot or non-plain messages
	}
	text := c.cleanText(msg.Text)
	if text == "" {
		return
	}
	// Root the investigation thread at the reacted message; acknowledge that
	// same message (item ts) with the working/done reactions.
	threadTS := msg.TS
	if msg.ThreadTS != "" {
		threadTS = msg.ThreadTS
	}
	c.deliver(threadTS, text, msg.TS, true, threadTS != msg.TS)
}

// deliver routes a cleaned message to the right thread worker, (re)attaching the
// aurora session as needed. A trigger (a mention or the configured keyword)
// starts a new duty thread; a plain message is served only if the thread is
// already ours (an active worker, or an existing session by name).
func (c *Connector) deliver(threadTS, text, messageTS string, trigger, isReply bool) {
	msg := inboundMsg{text: text, messageTS: messageTS}
	// Fast path: a live worker already serves this thread.
	c.mu.Lock()
	if t, ok := c.threads[threadTS]; ok {
		t.submit(msg, c.logger)
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	if !trigger {
		// Not a trigger and no live worker. Only a threaded reply can belong to a
		// bot thread whose worker has retired; a fresh top-level message never
		// does, so ordinary channel chatter is dropped without a session lookup.
		if !isReply {
			return
		}
		sessionID, err := c.aurora.FindSessionByName(c.ctx, sessionName(threadTS))
		if err != nil {
			c.logger.Warn("lookup session by name", "thread", threadTS, "error", err)
			return
		}
		if sessionID == "" {
			return
		}
		c.startWorker(threadTS, sessionID, msg)
		return
	}

	// A trigger: create the session (or reattach if it already exists).
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
	c.startWorker(threadTS, sessionID, msg)
}

// startWorker registers a thread and launches its worker, or hands the message
// to an existing worker if one raced into place.
func (c *Connector) startWorker(threadTS, sessionID string, first inboundMsg) {
	c.mu.Lock()
	if t, ok := c.threads[threadTS]; ok {
		t.submit(first, c.logger)
		c.mu.Unlock()
		return
	}
	t := &thread{threadTS: threadTS, sessionID: sessionID, inbox: make(chan inboundMsg, inboxBuffer)}
	c.threads[threadTS] = t
	t.submit(first, c.logger)
	c.mu.Unlock()

	c.logger.Info("serving thread", "thread", threadTS, "session", sessionID)
	go c.runThread(t)
}

// submit enqueues a message for the worker without blocking the caller; a full
// mailbox drops the message with a warning rather than stalling event handling.
func (t *thread) submit(msg inboundMsg, logger *slog.Logger) {
	select {
	case t.inbox <- msg:
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
		case msg := <-t.inbox:
			resetTimer(idle, idleTimeout)
			c.handleMessage(c.ctx, t, msg)
		case <-idle.C:
			// Retire only when nothing is queued, under the lock so a concurrent
			// submit cannot be lost to a dead worker.
			c.mu.Lock()
			if len(t.inbox) > 0 {
				c.mu.Unlock()
				resetTimer(idle, idleTimeout)
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

// handleMessage runs a single user message as a process and reports its progress
// back into the thread. It blocks the worker until the process finishes, which
// is exactly the per-thread serialization we want.
func (c *Connector) handleMessage(ctx context.Context, t *thread, msg inboundMsg) {
	c.ackWorking(ctx, msg.messageTS)

	statusTS, err := c.slack.PostMessage(ctx, c.cfg.ChannelID, t.threadTS, statusHeader(StatusQueued, false))
	if err != nil {
		c.logger.Warn("post status message", "thread", t.threadTS, "error", err)
		// Without a status message we can still run; progress just won't render.
	}

	proc, err := c.spawn(ctx, t.sessionID, msg.text)
	if err != nil {
		c.report(ctx, t, statusTS, "❌ I couldn't start the investigation: "+oneLine(err.Error()))
		c.ackDone(ctx, msg.messageTS, StatusFailed)
		return
	}
	c.pollProcess(ctx, t, proc.ID, statusTS, msg.messageTS)
}

// ackWorking marks a message the bot has started on with the working reaction.
// Best effort — a failed reaction never blocks the investigation.
func (c *Connector) ackWorking(ctx context.Context, messageTS string) {
	if messageTS == "" {
		return
	}
	if err := c.slack.AddReaction(ctx, c.cfg.ChannelID, messageTS, reactionWorking); err != nil {
		c.logger.Warn("add working reaction", "ts", messageTS, "error", err)
	}
}

// ackDone swaps the working reaction for a terminal one: a check on success, a
// cross otherwise. Best effort.
func (c *Connector) ackDone(ctx context.Context, messageTS, status string) {
	if messageTS == "" {
		return
	}
	_ = c.slack.RemoveReaction(ctx, c.cfg.ChannelID, messageTS, reactionWorking)
	emoji := reactionFailed
	if status == StatusCompleted {
		emoji = reactionDone
	}
	if err := c.slack.AddReaction(ctx, c.cfg.ChannelID, messageTS, emoji); err != nil {
		c.logger.Warn("add done reaction", "ts", messageTS, "error", err)
	}
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
func (c *Connector) pollProcess(ctx context.Context, t *thread, processID, statusTS, messageTS string) {
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
				c.ackDone(ctx, messageTS, snap.Status)
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
		msg = "❌ The investigation failed: " + oneLine(snap.Error)
	case StatusStopped:
		msg = "🛑 The investigation was stopped."
	case StatusCompensated:
		msg = "↩️ The investigation was rolled back: " + oneLine(snap.Error)
	default:
		return
	}
	if _, err := c.slack.PostMessage(ctx, c.cfg.ChannelID, t.threadTS, msg); err != nil {
		c.logger.Warn("post outcome", "thread", t.threadTS, "error", err)
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

// cleanText strips bot mentions and a leading trigger keyword — the bytes that
// reach aurora are the human's actual request. Horizontal whitespace within a
// line is collapsed, but line breaks are preserved so a pasted log or stack
// trace keeps its structure.
func (c *Connector) cleanText(text string) string {
	text = normalizeInput(c.mentionRE.ReplaceAllString(text, " "))
	if kw := c.cfg.TriggerKeyword; kw != "" && strings.HasPrefix(strings.ToLower(text), strings.ToLower(kw)) {
		text = strings.TrimSpace(text[len(kw):])
	}
	return text
}

// hasTrigger reports whether raw message text contains the configured trigger
// keyword (case-insensitive).
func (c *Connector) hasTrigger(text string) bool {
	kw := c.cfg.TriggerKeyword
	return kw != "" && strings.Contains(strings.ToLower(text), strings.ToLower(kw))
}

// mentionsBot reports whether text contains a native @-mention of this bot
// (matching both "<@U123>" and the labelled "<@U123|name>"). Because both the
// app_mention and message deliveries of one user message carry the same text,
// deciding "trigger" from this — rather than the event type — makes the two
// siblings agree no matter which wins de-duplication.
func (c *Connector) mentionsBot(text string) bool {
	return c.botUserID != "" && strings.Contains(text, "<@"+c.botUserID)
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
	case StatusCompensated:
		return "↩️ *Duty bot — rolled back*"
	case StatusWaitingTask:
		return "🛎️ *Duty bot — working* _(waiting on a timer)_"
	default:
		return "🛎️ *Duty bot — working…*"
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// collapseSpaces flattens every run of whitespace (including newlines) to a
// single space — for single-line renderings like error details.
func collapseSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
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

// truncate bounds a string to max runes (not bytes, so it never splits a
// multi-byte rune), appending an ellipsis when it cuts.
func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

// oneLine flattens and bounds an error/detail string for a chat line.
func oneLine(s string) string {
	s = truncate(collapseSpaces(s), 500)
	if s == "" {
		return "(no detail)"
	}
	return s
}
