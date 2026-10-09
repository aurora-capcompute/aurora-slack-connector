package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// reconnectDelay is the pause before a new connection after a failed one.
const reconnectDelay = 5 * time.Second

// socketMessage is one message on a Socket Mode connection. A message with an
// envelope id carries an event or a button click, and Slack sends it again
// until the connector acknowledges that id.
type socketMessage struct {
	Type       string          `json:"type"`
	EnvelopeID string          `json:"envelope_id"`
	Payload    json.RawMessage `json:"payload"`
	Reason     string          `json:"reason"`
}

// RunSocket receives Slack events and button clicks over Socket Mode until ctx
// ends. Slack ends each connection from time to time, and the connector then
// opens a new one. After a failure it waits reconnectDelay first.
func (c *Connector) RunSocket(ctx context.Context) {
	for {
		err := c.serveConnection(ctx)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			continue // Slack asked for a new connection.
		}
		c.logger.Warn("slack socket mode connection", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectDelay):
		}
	}
}

// serveConnection runs one connection. It returns nil when Slack asks for a new
// connection.
func (c *Connector) serveConnection(ctx context.Context) error {
	url, err := c.slack.OpenConnection(ctx, c.cfg.SlackAppToken)
	if err != nil {
		return err
	}
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial slack socket: %w", err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(maxMessageBytes)
	for {
		var msg socketMessage
		if err := wsjson.Read(ctx, conn, &msg); err != nil {
			return fmt.Errorf("read slack socket: %w", err)
		}
		if msg.EnvelopeID != "" {
			// Acknowledge first: Slack wants the ack within 3 seconds, and the
			// work runs on its own goroutine.
			if err := wsjson.Write(ctx, conn, map[string]string{"envelope_id": msg.EnvelopeID}); err != nil {
				return fmt.Errorf("ack slack envelope: %w", err)
			}
		}
		if msg.Type == "disconnect" {
			c.logger.Info("slack asked for a new socket mode connection", "reason", msg.Reason)
			return nil
		}
		c.route(msg)
	}
}

// route acts on one message: an event goes to its thread, and a button click
// to its approval or retry.
func (c *Connector) route(msg socketMessage) {
	switch msg.Type {
	case "hello":
		c.logger.Info("slack socket mode connected")
	case "events_api":
		var env slackEnvelope
		if err := json.Unmarshal(msg.Payload, &env); err != nil {
			c.logger.Warn("decode slack event", "error", err)
			return
		}
		if env.Type == "event_callback" {
			go c.dispatchEvent(env)
		}
	case "interactive":
		var p interactionPayload
		if err := json.Unmarshal(msg.Payload, &p); err != nil {
			c.logger.Warn("decode slack interaction", "error", err)
			return
		}
		go c.handleInteraction(p)
	}
}
