package connector

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// SlackClient calls the Slack Web API: auth.test to learn the bot's identity at
// startup, and chat.postMessage / chat.update to speak in a thread. Just enough
// of the API for a single-channel duty bot, over the standard library — no SDK.
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

// slackResponse is the envelope every Web API method returns.
type slackResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
	// chat.postMessage / chat.update
	TS      string `json:"ts"`
	Channel string `json:"channel"`
	// auth.test
	UserID string `json:"user_id"`
	Team   string `json:"team"`
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
func (c *SlackClient) UpdateMessage(ctx context.Context, channel, ts, text string) error {
	body := map[string]any{"channel": channel, "ts": ts, "text": text}
	var out slackResponse
	return c.call(ctx, "chat.update", body, &out)
}

func (c *SlackClient) call(ctx context.Context, method string, body map[string]any, out *slackResponse) error {
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
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("slack %s: read body: %w", method, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack %s: http %d: %s", method, resp.StatusCode, strings.TrimSpace(string(payload)))
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return fmt.Errorf("slack %s: decode: %w", method, err)
	}
	if !out.OK {
		return fmt.Errorf("slack %s: %s", method, out.Error)
	}
	return nil
}

// VerifySlackSignature checks an inbound Events API request against the signing
// secret: HMAC-SHA256 over "v0:<timestamp>:<rawBody>", compared in constant
// time, with a five-minute freshness window to blunt replays. rawBody must be
// the exact bytes Slack sent (the signature is over the wire body).
func VerifySlackSignature(signingSecret string, header http.Header, rawBody []byte, now time.Time) error {
	timestamp := header.Get("X-Slack-Request-Timestamp")
	signature := header.Get("X-Slack-Signature")
	if timestamp == "" || signature == "" {
		return fmt.Errorf("missing slack signature headers")
	}
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("bad slack timestamp %q", timestamp)
	}
	if delta := now.Unix() - ts; delta > 300 || delta < -300 {
		return fmt.Errorf("stale slack timestamp (%ds skew)", delta)
	}
	mac := hmac.New(sha256.New, []byte(signingSecret))
	mac.Write([]byte("v0:"))
	mac.Write([]byte(timestamp))
	mac.Write([]byte(":"))
	mac.Write(rawBody)
	expected := "v0=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(signature)) {
		return fmt.Errorf("slack signature mismatch")
	}
	return nil
}
