package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// Socket Mode transport. Instead of Slack POSTing events to a public HTTPS URL,
// the connector opens an outbound WebSocket to Slack and receives events and
// interactive payloads over it — so no ingress, no Request URL, and no request
// signing (the socket is authenticated by the app-level token that opens it).
//
// The protocol: POST apps.connections.open with the app token (xapp-…) to get a
// short-lived wss URL, dial it, then read envelopes. Every envelope that carries
// an envelope_id must be acknowledged within a few seconds by sending that id
// back, or Slack redelivers it. Slack periodically asks us to reconnect (a
// `disconnect` envelope, or by closing the socket); we just dial a fresh URL.

const (
	// socketReadLimit bounds a single inbound frame — Slack envelopes are small;
	// this stops a hostile or buggy peer from making us buffer without bound.
	socketReadLimit = 1 << 20
	// ackTimeout bounds how long we wait to write an envelope acknowledgement.
	ackTimeout = 5 * time.Second
	// reconnectMax caps the backoff between reconnect attempts.
	reconnectMax = 30 * time.Second
)

// socketEnvelope is the Socket Mode frame wrapping each payload. type is one of
// hello, events_api, interactive, slash_commands, or disconnect.
type socketEnvelope struct {
	Type       string          `json:"type"`
	EnvelopeID string          `json:"envelope_id"`
	Payload    json.RawMessage `json:"payload"`
	Reason     string          `json:"reason"` // set on a disconnect envelope
}

// SocketMode runs the Socket Mode connection and hands each events_api /
// interactive payload to the connector. It owns reconnection; the caller runs it
// once with Run and it stays connected until the context is cancelled.
type SocketMode struct {
	appToken string
	apiBase  string
	http     *http.Client
	logger   *slog.Logger

	// dial opens the WebSocket for a resolved wss URL; overridable in tests.
	dial func(ctx context.Context, url string) (*websocket.Conn, error)

	onEvent       func(payload []byte)
	onInteractive func(payload []byte)
}

// NewSocketMode builds a Socket Mode client for the given app-level token
// (xapp-…). onEvent receives each events_api payload (the standard Events API
// body); onInteractive receives each interactive payload (a block_actions JSON).
func NewSocketMode(appToken string, timeout time.Duration, logger *slog.Logger, onEvent, onInteractive func([]byte)) *SocketMode {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	s := &SocketMode{
		appToken:      appToken,
		apiBase:       "https://slack.com/api",
		http:          &http.Client{Timeout: timeout},
		logger:        logger,
		onEvent:       onEvent,
		onInteractive: onInteractive,
	}
	s.dial = func(ctx context.Context, url string) (*websocket.Conn, error) {
		conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPClient: s.http})
		return conn, err
	}
	return s
}

// SetAPIBaseURL overrides the Web API base used for apps.connections.open
// (default https://slack.com/api) — for an enterprise gateway or a test double.
func (s *SocketMode) SetAPIBaseURL(u string) {
	if u = strings.TrimSpace(u); u != "" {
		s.apiBase = strings.TrimRight(u, "/")
	}
}

// Run keeps a Socket Mode connection alive until ctx is cancelled, reconnecting
// with capped backoff on any disconnect or error.
func (s *SocketMode) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		if err := s.connectOnce(ctx); err != nil && ctx.Err() == nil {
			s.logger.Warn("socket mode connection ended; reconnecting", "error", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > reconnectMax {
				backoff = reconnectMax
			}
			continue
		}
		backoff = time.Second // a clean session resets the backoff
	}
}

// connectOnce opens one WebSocket and reads it to completion (a disconnect, a
// read error, or ctx cancellation).
func (s *SocketMode) connectOnce(ctx context.Context) error {
	url, err := s.openConnection(ctx)
	if err != nil {
		return fmt.Errorf("apps.connections.open: %w", err)
	}
	conn, err := s.dial(ctx, url)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	conn.SetReadLimit(socketReadLimit)
	s.logger.Info("socket mode connected")
	return s.readLoop(ctx, conn)
}

// readLoop reads envelopes, acknowledges each, and dispatches events_api /
// interactive payloads. It returns when Slack asks to reconnect or the socket
// errors. Payload processing is handed off asynchronously by the callbacks, so a
// slow downstream never delays the acknowledgement Slack requires.
func (s *SocketMode) readLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		var env socketEnvelope
		if err := json.Unmarshal(data, &env); err != nil {
			s.logger.Warn("socket: bad envelope", "error", err)
			continue
		}
		if env.EnvelopeID != "" {
			s.ack(ctx, conn, env.EnvelopeID)
		}
		switch env.Type {
		case "events_api":
			if s.onEvent != nil {
				s.onEvent(env.Payload)
			}
		case "interactive":
			if s.onInteractive != nil {
				s.onInteractive(env.Payload)
			}
		case "disconnect":
			s.logger.Info("socket: server asked to reconnect", "reason", env.Reason)
			return nil
		case "hello":
			// Connection established; nothing to do.
		default:
			// slash_commands and anything else we don't subscribe to.
		}
	}
}

// ack sends the envelope acknowledgement Slack requires within a few seconds.
func (s *SocketMode) ack(ctx context.Context, conn *websocket.Conn, envelopeID string) {
	payload, err := json.Marshal(map[string]string{"envelope_id": envelopeID})
	if err != nil {
		return
	}
	writeCtx, cancel := context.WithTimeout(ctx, ackTimeout)
	defer cancel()
	if err := conn.Write(writeCtx, websocket.MessageText, payload); err != nil {
		s.logger.Warn("socket: ack failed", "envelope", envelopeID, "error", err)
	}
}

// openConnection asks Slack for a wss URL using the app-level token.
func (s *SocketMode) openConnection(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.apiBase+"/apps.connections.open", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer "+s.appToken)
	resp, err := s.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		OK    bool   `json:"ok"`
		URL   string `json:"url"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	if !out.OK {
		return "", fmt.Errorf("slack error: %s", out.Error)
	}
	if out.URL == "" {
		return "", fmt.Errorf("no url in response")
	}
	return out.URL, nil
}
