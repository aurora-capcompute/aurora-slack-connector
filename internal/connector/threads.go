package connector

// From a Slack event to a thread worker: each thread has one worker that runs
// its mentions one at a time.

import (
	"log/slog"
	"time"
)

const (
	// inboxBuffer bounds the messages queued for one thread's worker.
	inboxBuffer = 64
	// idleTimeout retires a thread worker after a spell of no messages. Its
	// aurora session stays, and the next mention reattaches it by name.
	idleTimeout = 30 * time.Minute
)

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

func sessionName(threadTS string) string { return "slack:" + threadTS }
