// Command aurora-slack-connector bridges one Slack channel to a local
// aurora-dist instance over Slack's Socket Mode, turning it into an on-call
// "duty bot": mention it in the channel and it opens an aurora session for the
// thread, runs your message as a process, and narrates the syscalls back into
// the thread while it works. Each later mention in the thread runs as a further
// process in the same session, with the thread's messages since the previous
// mention, so the investigation shares history.
//
// Configuration is entirely by environment variable — see LoadConfig and the
// README. The connector holds no secrets of its own: the LLM endpoint and the
// capability grants live in the aurora manifest the operator supplies.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aurora-capcompute/aurora-slack-connector/internal/connector"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := connector.LoadConfig()
	if err != nil {
		logger.Error("configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	aurora := connector.NewAuroraClient(cfg.AuroraBaseURL, cfg.HTTPTimeout)
	slack := connector.NewSlackClient(cfg.SlackBotToken, cfg.SlackAPIBaseURL, cfg.HTTPTimeout)

	// A reachable aurora at boot is not required (it may start after us), but a
	// failing health check is worth surfacing.
	healthCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if err := aurora.Healthz(healthCtx); err != nil {
		logger.Warn("aurora-dist not reachable at startup; will keep trying on demand", "base_url", cfg.AuroraBaseURL, "error", err)
	} else {
		logger.Info("aurora-dist reachable", "base_url", cfg.AuroraBaseURL)
	}
	cancel()

	conn := connector.New(cfg, aurora, slack, logger)
	conn.Start(ctx)

	logger.Info("aurora-slack-connector running", "channel", cfg.ChannelID, "aurora", cfg.AuroraBaseURL)
	conn.RunSocket(ctx)
	logger.Info("shut down cleanly")
}
