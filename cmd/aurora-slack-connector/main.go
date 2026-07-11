// Command aurora-slack-connector is a small HTTP server that bridges one Slack
// channel to a local aurora-dist instance, turning it into an on-call "duty
// bot": mention it (or use the configured trigger) in the channel and it opens
// an aurora session for the thread, runs your message as a process, and narrates
// the syscalls back into the thread while it works. Follow-up messages in the
// thread run as further processes in the same session, so the investigation
// shares history.
//
// Configuration is entirely by environment variable — see LoadConfig and the
// README. The connector holds no secrets of its own: the LLM endpoint and the
// capability grants live in the aurora manifest the operator supplies.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
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
	slack := connector.NewSlackClient(cfg.SlackBotToken, cfg.HTTPTimeout)
	slack.SetAPIBaseURL(cfg.SlackAPIBaseURL)

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

	// A small HTTP server for liveness only — Socket Mode is an outbound
	// WebSocket, so there is no inbound events endpoint to serve.
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           conn.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("health server", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("aurora-slack-connector connecting (socket mode)",
		"channel", cfg.ChannelID, "aurora", cfg.AuroraBaseURL, "health_addr", cfg.Addr)
	conn.Run(ctx) // blocks until the context is cancelled
	logger.Info("shut down cleanly")
}
