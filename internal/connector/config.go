package connector

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the connector's full configuration. Everything is supplied by the
// operator — most importantly the aurora Manifest, which carries the LLM
// endpoint and API key and the capability grants the duty bot runs with. The
// connector never invents those: it passes the manifest through to aurora
// verbatim, so secrets stay in the operator's environment.
type Config struct {
	// --- Slack ---

	// SlackBotToken (xoxb-...) authorizes Web API calls: posting and updating
	// messages, adding reactions, reading a reacted message, and auth.test.
	SlackBotToken string
	// SlackAppToken (xapp-...) opens the Socket Mode connection
	// (apps.connections.open). It needs the connections:write scope and is
	// distinct from the bot token.
	SlackAppToken string
	// ChannelID is the single channel this connector serves. Events from any
	// other channel are ignored — a duty bot owns exactly one room.
	ChannelID string
	// TriggerKeyword optionally triggers a new duty thread when a top-level
	// channel message contains it (e.g. "@duty"). A native @-mention of the bot
	// (app_mention) always triggers regardless of this.
	TriggerKeyword string
	// TriggerReaction is the emoji name (no colons, e.g. "eyes") that, when added
	// to any channel message, tells the bot to investigate that message. Empty
	// disables reaction-triggered investigations.
	TriggerReaction string
	// BotUserID is the bot's own Slack user id. Auto-detected via auth.test when
	// empty; used to strip the leading mention from a message and to ignore the
	// bot's own posts.
	BotUserID string
	// SlackAPIBaseURL overrides the Slack Web API base (default
	// https://slack.com/api) — for an enterprise gateway or local testing.
	SlackAPIBaseURL string

	// --- Aurora ---

	// AuroraBaseURL is the local aurora-dist address (e.g. http://localhost:8080).
	AuroraBaseURL string
	// Manifest is the opaque aurora manifest applied to every process (aurora
	// spawn) this connector starts. Passed through verbatim.
	Manifest json.RawMessage

	// --- HTTP server (health only) ---

	// Addr is the listen address for the /healthz liveness endpoint (e.g. :3000).
	// Socket Mode is outbound, so no inbound events endpoint is served.
	Addr string

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
// SLACK_APP_TOKEN, SLACK_CHANNEL_ID, and a manifest (AURORA_MANIFEST inline JSON
// or AURORA_MANIFEST_FILE path).
func LoadConfig() (Config, error) {
	cfg := Config{
		SlackBotToken:   strings.TrimSpace(os.Getenv("SLACK_BOT_TOKEN")),
		SlackAppToken:   strings.TrimSpace(os.Getenv("SLACK_APP_TOKEN")),
		ChannelID:       strings.TrimSpace(os.Getenv("SLACK_CHANNEL_ID")),
		TriggerKeyword:  strings.TrimSpace(os.Getenv("SLACK_TRIGGER_KEYWORD")),
		TriggerReaction: strings.TrimSpace(os.Getenv("SLACK_TRIGGER_REACTION")),
		BotUserID:       strings.TrimSpace(os.Getenv("SLACK_BOT_USER_ID")),
		SlackAPIBaseURL: strings.TrimSpace(os.Getenv("SLACK_API_BASE_URL")),
		AuroraBaseURL:   strings.TrimSpace(os.Getenv("AURORA_BASE_URL")),
		Addr:            strings.TrimSpace(os.Getenv("ADDR")),
	}

	if cfg.AuroraBaseURL == "" {
		cfg.AuroraBaseURL = "http://localhost:8080"
	}
	if cfg.Addr == "" {
		cfg.Addr = ":3000"
	}
	if cfg.TriggerKeyword == "" {
		cfg.TriggerKeyword = "@duty"
	}
	if cfg.TriggerReaction == "" {
		cfg.TriggerReaction = "eyes"
	}
	// Tolerate a reaction written with surrounding colons (":eyes:").
	cfg.TriggerReaction = strings.Trim(cfg.TriggerReaction, ":")

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

	manifest, err := loadManifest()
	if err != nil {
		return Config{}, err
	}
	cfg.Manifest = manifest

	return cfg, cfg.Validate()
}

// loadManifest reads the manifest from AURORA_MANIFEST (inline JSON) or
// AURORA_MANIFEST_FILE (a path), and checks it parses as a JSON object.
func loadManifest() (json.RawMessage, error) {
	inline := strings.TrimSpace(os.Getenv("AURORA_MANIFEST"))
	path := strings.TrimSpace(os.Getenv("AURORA_MANIFEST_FILE"))
	var raw []byte
	switch {
	case inline != "":
		raw = []byte(inline)
	case path != "":
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read AURORA_MANIFEST_FILE: %w", err)
		}
		raw = b
	default:
		return nil, nil
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, fmt.Errorf("manifest is not a JSON object: %w", err)
	}
	// Re-marshal to a compact canonical form so it travels cleanly in the
	// process-create request body.
	compact, err := json.Marshal(probe)
	if err != nil {
		return nil, fmt.Errorf("canonicalize manifest: %w", err)
	}
	return compact, nil
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
		return fmt.Errorf("SLACK_APP_TOKEN is required (Socket Mode app-level token, xapp-…)")
	case c.ChannelID == "":
		return fmt.Errorf("SLACK_CHANNEL_ID is required")
	case len(c.Manifest) == 0:
		return fmt.Errorf("a manifest is required (set AURORA_MANIFEST or AURORA_MANIFEST_FILE)")
	case c.PollInterval <= 0:
		return fmt.Errorf("POLL_INTERVAL must be positive")
	case c.ProcessTimeout <= 0:
		return fmt.Errorf("PROCESS_TIMEOUT must be positive")
	}
	return nil
}
