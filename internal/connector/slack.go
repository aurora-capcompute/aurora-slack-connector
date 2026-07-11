package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// SlackClient calls the Slack Web API: auth.test to learn the bot's identity at
// startup, chat.postMessage / chat.update to speak in a thread, reactions.add /
// reactions.remove to acknowledge a message, and conversations.history to read a
// message a user reacted on. Just enough of the API for a single-channel duty
// bot, over the standard library — no SDK.
type SlackClient struct {
	token   string
	baseURL string // https://slack.com/api, overridable in tests
	http    *http.Client
}

// NewSlackClient builds a client for the given bot token (xoxb-...).
func NewSlackClient(token string, timeout time.Duration) *SlackClient {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &SlackClient{
		token:   token,
		baseURL: "https://slack.com/api",
		http:    &http.Client{Timeout: timeout},
	}
}

// SetAPIBaseURL overrides the Slack Web API base (default https://slack.com/api).
// Useful for an enterprise Slack gateway or a local test double; empty is
// ignored so the default stands.
func (c *SlackClient) SetAPIBaseURL(u string) {
	if u = strings.TrimSpace(u); u != "" {
		c.baseURL = strings.TrimRight(u, "/")
	}
}

// slackResponse is the subset of a Web API reply the simple methods read.
type slackResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
	// chat.postMessage / chat.update
	TS string `json:"ts"`
	// auth.test
	UserID string `json:"user_id"`
}

// slackMessage is one message from conversations.history.
type slackMessage struct {
	Type     string `json:"type"`
	Subtype  string `json:"subtype"`
	User     string `json:"user"`
	BotID    string `json:"bot_id"`
	Text     string `json:"text"`
	TS       string `json:"ts"`
	ThreadTS string `json:"thread_ts"`
}

// AuthTest returns the bot's own user id, so the connector can strip the leading
// mention from a message and ignore the bot's own posts.
func (c *SlackClient) AuthTest(ctx context.Context) (userID string, err error) {
	var out slackResponse
	if err := c.call(ctx, "auth.test", map[string]any{}, &out); err != nil {
		return "", err
	}
	return out.UserID, nil
}

// PostMessage posts text into a thread and returns the new message's ts (its
// handle for later chat.update calls). threadTS is the parent message's ts.
func (c *SlackClient) PostMessage(ctx context.Context, channel, threadTS, text string) (ts string, err error) {
	body := map[string]any{"channel": channel, "text": text}
	if threadTS != "" {
		body["thread_ts"] = threadTS
	}
	var out slackResponse
	if err := c.call(ctx, "chat.postMessage", body, &out); err != nil {
		return "", err
	}
	return out.TS, nil
}

// UpdateMessage edits an existing message in place — used to keep a single
// status line current as syscalls come and go, rather than flooding the thread.
// Passing text (and no blocks) also clears any blocks the message had, which is
// how an approval prompt's buttons are removed once the decision is made.
func (c *SlackClient) UpdateMessage(ctx context.Context, channel, ts, text string) error {
	body := map[string]any{"channel": channel, "ts": ts, "text": text, "blocks": []any{}}
	var out slackResponse
	return c.call(ctx, "chat.update", body, &out)
}

// PostBlockMessage posts a Block Kit message into a thread. text is the
// notification/accessibility fallback; blocks is the rich layout (e.g. an
// approval prompt with buttons). Returns the new message's ts.
func (c *SlackClient) PostBlockMessage(ctx context.Context, channel, threadTS, text string, blocks []map[string]any) (string, error) {
	body := map[string]any{"channel": channel, "text": text, "blocks": blocks}
	if threadTS != "" {
		body["thread_ts"] = threadTS
	}
	var out slackResponse
	if err := c.call(ctx, "chat.postMessage", body, &out); err != nil {
		return "", err
	}
	return out.TS, nil
}

// AddReaction adds an emoji reaction (name without colons, e.g. "eyes") to a
// message. An already-present reaction is treated as success — reaction acks are
// idempotent.
func (c *SlackClient) AddReaction(ctx context.Context, channel, timestamp, name string) error {
	body := map[string]any{"channel": channel, "timestamp": timestamp, "name": name}
	var out slackResponse
	err := c.call(ctx, "reactions.add", body, &out)
	if err != nil && strings.Contains(err.Error(), "already_reacted") {
		return nil
	}
	return err
}

// RemoveReaction removes one of the bot's reactions from a message. A reaction
// that is already absent is treated as success.
func (c *SlackClient) RemoveReaction(ctx context.Context, channel, timestamp, name string) error {
	body := map[string]any{"channel": channel, "timestamp": timestamp, "name": name}
	var out slackResponse
	err := c.call(ctx, "reactions.remove", body, &out)
	if err != nil && strings.Contains(err.Error(), "no_reaction") {
		return nil
	}
	return err
}

// GetMessage fetches a single channel message by its ts (the message a user
// reacted on). found is false when the timestamp names no message the bot can
// read — e.g. a thread reply, which conversations.history does not return.
func (c *SlackClient) GetMessage(ctx context.Context, channel, ts string) (msg slackMessage, found bool, err error) {
	body := map[string]any{
		"channel":   channel,
		"latest":    ts,
		"oldest":    ts,
		"inclusive": true,
		"limit":     1,
	}
	var out struct {
		slackResponse
		Messages []slackMessage `json:"messages"`
	}
	if err := c.call(ctx, "conversations.history", body, &out); err != nil {
		return slackMessage{}, false, err
	}
	for _, m := range out.Messages {
		if m.TS == ts {
			return m, true, nil
		}
	}
	return slackMessage{}, false, nil
}

// call performs one Web API method call and decodes the reply into out (any
// struct exposing the ok/error envelope). It fails on a transport error, a
// non-200, or an ok:false response.
func (c *SlackClient) call(ctx context.Context, method string, body map[string]any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/"+method, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("slack %s: %w", method, err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return fmt.Errorf("slack %s: read body: %w", method, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack %s: http %d: %s", method, resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	var status slackResponse
	if err := json.Unmarshal(payload, &status); err != nil {
		return fmt.Errorf("slack %s: decode: %w", method, err)
	}
	if !status.OK {
		return fmt.Errorf("slack %s: %s", method, status.Error)
	}
	if out != nil {
		if err := json.Unmarshal(payload, out); err != nil {
			return fmt.Errorf("slack %s: decode: %w", method, err)
		}
	}
	return nil
}
