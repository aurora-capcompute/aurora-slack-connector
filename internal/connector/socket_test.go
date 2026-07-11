package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestSocketModeReceivesAndAcks drives the real Socket Mode client against a
// local WebSocket server: it must open a connection (apps.connections.open),
// dial the returned wss URL, deliver an events_api payload to onEvent, and
// acknowledge the envelope by echoing its id.
func TestSocketModeReceivesAndAcks(t *testing.T) {
	acked := make(chan string, 1)
	hold := make(chan struct{})
	defer close(hold)

	mux := http.NewServeMux()
	mux.HandleFunc("/apps.connections.open", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Point the client back at this same server's /ws endpoint.
		_, _ = w.Write([]byte(`{"ok":true,"url":"ws://` + r.Host + `/ws"}`))
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		ctx := r.Context()
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello"}`))
		event := `{"type":"events_api","envelope_id":"env1","payload":{"type":"event_callback","event":{"type":"app_mention"}}}`
		if err := conn.Write(ctx, websocket.MessageText, []byte(event)); err != nil {
			return
		}
		// Read the acknowledgement the client must send back.
		_, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		var ack struct {
			EnvelopeID string `json:"envelope_id"`
		}
		_ = json.Unmarshal(data, &ack)
		acked <- ack.EnvelopeID
		<-hold // keep the socket open so the client doesn't reconnect
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	events := make(chan []byte, 1)
	sm := NewSocketMode("xapp-test", time.Second, discardLogger(),
		func(p []byte) { events <- p }, nil)
	sm.SetAPIBaseURL(srv.URL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sm.Run(ctx)

	select {
	case p := <-events:
		if !strings.Contains(string(p), "app_mention") {
			t.Fatalf("event payload = %s", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event delivered over the socket")
	}
	select {
	case id := <-acked:
		if id != "env1" {
			t.Fatalf("ack echoed %q, want env1", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("envelope was not acknowledged")
	}
}

// openConnection surfaces a Slack ok:false as an error rather than dialing.
func TestSocketModeOpenConnectionError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":false,"error":"invalid_auth"}`))
	}))
	defer srv.Close()
	sm := NewSocketMode("xapp-bad", time.Second, discardLogger(), nil, nil)
	sm.SetAPIBaseURL(srv.URL)
	if _, err := sm.openConnection(context.Background()); err == nil {
		t.Fatal("expected an error from an ok:false apps.connections.open")
	}
}
