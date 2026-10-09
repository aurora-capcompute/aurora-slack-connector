package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

// Slack sends events over the Socket Mode connection. The connector
// acknowledges each envelope, runs its event, and opens a new connection when
// Slack asks for one.
func TestSocketModeAcksEventsAndReconnects(t *testing.T) {
	a := newAuroraStub()
	defer a.close()
	sl := newSlackStub()
	defer sl.close()

	acks := make(chan string, 4)
	var opens atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/apps.connections.open", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer xapp-test" {
			t.Errorf("apps.connections.open auth = %q, want the app token", got)
		}
		fmt.Fprintf(w, `{"ok":true,"url":"ws://%s/link/%d"}`, r.Host, opens.Add(1))
	})
	// Each connection delivers one message of the thread. The first then asks
	// for a new connection.
	mux.HandleFunc("/link/{n}", func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer conn.CloseNow()
		ctx := r.Context()
		first := r.PathValue("n") == "1"
		ts, threadTS := "300.1", ""
		if !first {
			ts, threadTS = "300.2", "300.1"
		}
		event, _ := json.Marshal(mentionEvent("U1", "<@UBOT> look at "+ts, ts, threadTS))
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "hello"})
		_ = wsjson.Write(ctx, conn, map[string]any{"type": "events_api", "envelope_id": "env-" + ts, "payload": json.RawMessage(event)})
		var ack map[string]string
		if err := wsjson.Read(ctx, conn, &ack); err == nil {
			acks <- ack["envelope_id"]
		}
		if first {
			_ = wsjson.Write(ctx, conn, map[string]any{"type": "disconnect", "reason": "refresh_requested"})
		}
		// Hold the connection until the connector closes it.
		_, _, _ = conn.Read(ctx)
	})
	mux.Handle("/", sl.server.Config.Handler)
	slackAPI := httptest.NewServer(mux)
	defer slackAPI.Close()

	conn := newTestConnector(t, a, sl)
	conn.cfg.SlackAppToken = "xapp-test"
	conn.slack.baseURL = slackAPI.URL
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn.Start(ctx)
	go conn.RunSocket(ctx)

	for _, want := range []string{"env-300.1", "env-300.2"} {
		select {
		case got := <-acks:
			if got != want {
				t.Fatalf("ack = %q, want %q", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("no ack for %s", want)
		}
	}
	waitForPost(t, sl.postCh, "Re: look at 300.1", 3*time.Second)
	waitForPost(t, sl.postCh, "Re: look at 300.2", 3*time.Second)
	if n := opens.Load(); n != 2 {
		t.Fatalf("connections opened = %d, want 2", n)
	}
}
