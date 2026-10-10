package connector

import (
	"context"
	"log/slog"
	"sync"
)

// Connector wires Slack to a local aurora-dist. It receives Slack events over a
// Socket Mode connection, maps each thread to an aurora session, and runs each
// mention of the bot as a process (an "aurora spawn") in that session —
// serialized per thread so the session's single-active-process rule holds and
// history is shared across mentions. While a process runs it polls aurora and
// narrates the syscalls back into the thread.
type Connector struct {
	cfg    Config
	aurora *AuroraClient
	slack  *SlackClient
	logger *slog.Logger
	// botUserID is the bot's own Slack user: from the config, or from
	// auth.test at Start.
	botUserID string
	// ctx is the base context. Start sets it, and its end retires every worker.
	ctx context.Context
	// seen drops an event Slack delivers twice.
	seen *seenSet

	// mu guards threads: the live worker of each thread, by thread ts.
	mu      sync.Mutex
	threads map[string]*thread
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
		ctx:       context.Background(),
		seen:      newSeenSet(seenCapacity),
		threads:   make(map[string]*thread),
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
