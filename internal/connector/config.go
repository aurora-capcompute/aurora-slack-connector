package connector

import (
	"cmp"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the connector's full configuration, supplied by the operator.
type Config struct {
	// --- Slack ---

	// SlackBotToken (xoxb-...) authorizes Web API calls: posting and updating
	// messages, and auth.test at startup.
	SlackBotToken string
	// SlackAppToken (xapp-..., with connections:write) opens the Socket Mode
	// connection that delivers events and button clicks. The connector makes
	// only outbound connections, so it needs no public URL.
	SlackAppToken string
	// ChannelID is the single channel this connector serves. Events from any
	// other channel are ignored — a duty bot owns exactly one room.
	ChannelID string
	// BotUserID is the bot's own Slack user id. Auto-detected via auth.test when
	// empty; used to strip the leading mention from a message and to ignore the
	// bot's own posts.
	BotUserID string
	// SlackAPIBaseURL is the Slack Web API base (default https://slack.com/api),
	// overridable for an enterprise gateway or local testing.
	SlackAPIBaseURL string

	// --- Aurora ---

	// AuroraBaseURL is the local aurora-dist address (e.g. http://localhost:8080).
	AuroraBaseURL string

	// --- Polling ---

	// PollInterval is how often a running process is polled for progress.
	PollInterval time.Duration
	// ProcessTimeout bounds how long the connector actively polls a single
	// process before giving up on live updates (the process keeps running in
	// aurora; only the Slack progress reporting stops).
	ProcessTimeout time.Duration
	// HTTPTimeout bounds each individual aurora/Slack HTTP request.
	HTTPTimeout time.Duration
}

// LoadConfig reads configuration from the environment. Required: SLACK_BOT_TOKEN,
// SLACK_APP_TOKEN and SLACK_CHANNEL_ID.
func LoadConfig() (Config, error) {
	cfg := Config{
		SlackBotToken:   strings.TrimSpace(os.Getenv("SLACK_BOT_TOKEN")),
		SlackAppToken:   strings.TrimSpace(os.Getenv("SLACK_APP_TOKEN")),
		ChannelID:       strings.TrimSpace(os.Getenv("SLACK_CHANNEL_ID")),
		BotUserID:       strings.TrimSpace(os.Getenv("SLACK_BOT_USER_ID")),
		SlackAPIBaseURL: cmp.Or(strings.TrimSpace(os.Getenv("SLACK_API_BASE_URL")), "https://slack.com/api"),
		AuroraBaseURL:   cmp.Or(strings.TrimSpace(os.Getenv("AURORA_BASE_URL")), "http://localhost:8080"),
	}

	var err error
	if cfg.PollInterval, err = durationEnv("POLL_INTERVAL", 2*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.ProcessTimeout, err = durationEnv("PROCESS_TIMEOUT", 15*time.Minute); err != nil {
		return Config{}, err
	}
	if cfg.HTTPTimeout, err = durationEnv("HTTP_TIMEOUT", 30*time.Second); err != nil {
		return Config{}, err
	}

	return cfg, cfg.Validate()
}

func durationEnv(key string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def, nil
	}
	// Allow a bare integer to mean seconds, otherwise a Go duration string.
	if n, err := strconv.Atoi(v); err == nil {
		return time.Duration(n) * time.Second, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid duration for %s: %q", key, v)
	}
	return d, nil
}

// Validate reports the first missing or malformed required field.
func (c Config) Validate() error {
	switch {
	case c.SlackBotToken == "":
		return fmt.Errorf("SLACK_BOT_TOKEN is required")
	case c.SlackAppToken == "":
		return fmt.Errorf("SLACK_APP_TOKEN is required")
	case c.ChannelID == "":
		return fmt.Errorf("SLACK_CHANNEL_ID is required")
	case c.PollInterval <= 0:
		return fmt.Errorf("POLL_INTERVAL must be positive")
	case c.ProcessTimeout <= 0:
		return fmt.Errorf("PROCESS_TIMEOUT must be positive")
	case c.HTTPTimeout <= 0:
		return fmt.Errorf("HTTP_TIMEOUT must be positive")
	}
	return nil
}
