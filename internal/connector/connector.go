package connector

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
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

// Connector wires Slack to a local aurora-dist. It receives Slack events over
// HTTP, maps each thread to an aurora session, and runs each user message as a
// process (an "aurora spawn") in that session — serialized per thread so the
// session's single-active-process rule holds and history is shared across
// messages. While a process runs it polls aurora and narrates the syscalls back
// into the thread.
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
	inbox     chan string
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
		threads:   make(map[string]*thread),
		seen:      newSeenSet(seenCapacity),
		mentionRE: regexp.MustCompile(`<@[A-Z0-9]+>`),
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

// Handler returns the connector's HTTP mux: the Slack events endpoint plus a
// health check.
func (c *Connector) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+c.cfg.EventsPath, c.handleEvents)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// --- Slack event ingestion ---

type slackEnvelope struct {
	Type      string     `json:"type"`
	Challenge string     `json:"challenge"`
	EventID   string     `json:"event_id"`
	Event     slackEvent `json:"event"`
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

func (c *Connector) handleEvents(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	if err := VerifySlackSignature(c.cfg.SlackSigningSecret, r.Header, body, time.Now()); err != nil {
		c.logger.Warn("rejected slack request", "error", err)
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	var env slackEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	// The one-time endpoint verification handshake echoes the challenge.
	if env.Type == "url_verification" {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(env.Challenge))
		return
	}
	// Ack fast (Slack demands a 200 within 3s) and do the work asynchronously.
	w.WriteHeader(http.StatusOK)
	if env.Type == "event_callback" {
		c.dispatchEvent(env)
	}
}

// dispatchEvent filters an inbound event to the messages this duty bot acts on
// and routes it to a thread worker.
func (c *Connector) dispatchEvent(env slackEnvelope) {
	ev := env.Event

	// Only the two event kinds we subscribe to.
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

	threadTS := ev.ThreadTS
	if threadTS == "" {
		threadTS = ev.TS
	}
	text := c.cleanText(ev.Text)
	if text == "" {
		return
	}
	trigger := ev.Type == "app_mention" || c.hasTrigger(ev.Text)
	c.deliver(threadTS, text, trigger)
}

// deliver routes a cleaned message to the right thread worker, (re)attaching the
// aurora session as needed. A trigger (a mention or the configured keyword)
// starts a new duty thread; a plain message is served only if the thread is
// already ours (an active worker, or an existing session by name).
func (c *Connector) deliver(threadTS, text string, trigger bool) {
	// Fast path: a live worker already serves this thread.
	c.mu.Lock()
	if t, ok := c.threads[threadTS]; ok {
		t.submit(text, c.logger)
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	if !trigger {
		// Not a trigger and no live worker: reattach only if this thread already
		// has a session (the bot was invoked here before). Otherwise it's just
		// channel chatter — ignore it.
		sessionID, err := c.aurora.FindSessionByName(c.ctx, sessionName(threadTS))
		if err != nil {
			c.logger.Warn("lookup session by name", "thread", threadTS, "error", err)
			return
		}
		if sessionID == "" {
			return
		}
		c.startWorker(threadTS, sessionID, text)
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
	c.startWorker(threadTS, sessionID, text)
}

// startWorker registers a thread and launches its worker, or hands the message
// to an existing worker if one raced into place.
func (c *Connector) startWorker(threadTS, sessionID, firstText string) {
	c.mu.Lock()
	if t, ok := c.threads[threadTS]; ok {
		t.submit(firstText, c.logger)
		c.mu.Unlock()
		return
	}
	t := &thread{threadTS: threadTS, sessionID: sessionID, inbox: make(chan string, inboxBuffer)}
	c.threads[threadTS] = t
	t.submit(firstText, c.logger)
	c.mu.Unlock()

	c.logger.Info("serving thread", "thread", threadTS, "session", sessionID)
	go c.runThread(t)
}

// submit enqueues a message for the worker without blocking the caller; a full
// mailbox drops the message with a warning rather than stalling event handling.
func (t *thread) submit(text string, logger *slog.Logger) {
	select {
	case t.inbox <- text:
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
		case text := <-t.inbox:
			resetTimer(idle, idleTimeout)
			c.handleMessage(c.ctx, t, text)
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
func (c *Connector) handleMessage(ctx context.Context, t *thread, text string) {
	statusTS, err := c.slack.PostMessage(ctx, c.cfg.ChannelID, t.threadTS, statusHeader(StatusQueued))
	if err != nil {
		c.logger.Warn("post status message", "thread", t.threadTS, "error", err)
		// Without a status message we can still run; progress just won't render.
	}

	proc, err := c.spawn(ctx, t.sessionID, text)
	if err != nil {
		c.report(ctx, t, statusTS, "❌ I couldn't start the investigation: "+oneLine(err.Error()))
		return
	}
	c.pollProcess(ctx, t, proc.ID, statusTS)
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
// fetched only when the journal grows (or the process ends) to render the
// syscall timeline. On a terminal status it posts the final answer or error.
func (c *Connector) pollProcess(ctx context.Context, t *thread, processID, statusTS string) {
	deadline := time.Now().Add(c.cfg.ProcessTimeout)
	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()

	lastJournalLen := -1
	lastText := ""
	var entries []JournalEntry

	for {
		snap, err := c.aurora.GetProcess(ctx, processID)
		if err != nil {
			c.logger.Warn("poll process", "process", processID, "error", err)
		} else {
			if snap.JournalLength != lastJournalLen || IsTerminal(snap.Status) {
				if log, lerr := c.aurora.GetSessionLog(ctx, t.sessionID); lerr != nil {
					c.logger.Warn("fetch session log", "session", t.sessionID, "error", lerr)
				} else if pl := log.Process(processID); pl != nil {
					entries = pl.Entries
				}
				lastJournalLen = snap.JournalLength
			}
			running := !IsTerminal(snap.Status)
			if text := renderStatus(statusHeader(snap.Status), entries, running); text != lastText && statusTS != "" {
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

// cleanText strips bot mentions and a leading trigger keyword, then trims — the
// bytes that reach aurora are the human's actual request.
func (c *Connector) cleanText(text string) string {
	text = collapseSpaces(c.mentionRE.ReplaceAllString(text, " "))
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

func sessionName(threadTS string) string { return "slack:" + threadTS }

func statusHeader(status string) string {
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
		return "🛎️ *Duty bot — working* _(waiting on a task)_"
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

func collapseSpaces(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// oneLine flattens and bounds an error/detail string for a chat line.
func oneLine(s string) string {
	s = collapseSpaces(s)
	const max = 500
	if len(s) > max {
		s = s[:max] + "…"
	}
	if s == "" {
		return "(no detail)"
	}
	return s
}
