package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSlackClientCalls(t *testing.T) {
	var gotAuth, gotPost, gotUpdate bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer xoxb-test" {
			t.Errorf("missing bearer auth: %q", got)
		}
		body, _ := io.ReadAll(r.Body)
		var in map[string]any
		_ = json.Unmarshal(body, &in)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/auth.test":
			gotAuth = true
			_, _ = w.Write([]byte(`{"ok":true,"user_id":"UBOT"}`))
		case "/chat.postMessage":
			gotPost = true
			if in["channel"] != "C1" || in["thread_ts"] != "T1" {
				t.Errorf("post payload = %v", in)
			}
			_, _ = w.Write([]byte(`{"ok":true,"ts":"111.222"}`))
		case "/chat.update":
			gotUpdate = true
			if in["ts"] != "111.222" {
				t.Errorf("update payload = %v", in)
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := NewSlackClient("xoxb-test", srv.URL, time.Second)
	ctx := context.Background()

	id, err := c.AuthTest(ctx)
	if err != nil || id != "UBOT" {
		t.Fatalf("auth.test: id=%q err=%v", id, err)
	}
	ts, err := c.PostMessage(ctx, "C1", "T1", "hi")
	if err != nil || ts != "111.222" {
		t.Fatalf("postMessage: ts=%q err=%v", ts, err)
	}
	if err := c.UpdateMessage(ctx, "C1", "111.222", "updated"); err != nil {
		t.Fatalf("update: %v", err)
	}
	if !gotAuth || !gotPost || !gotUpdate {
		t.Fatalf("not all endpoints hit: auth=%v post=%v update=%v", gotAuth, gotPost, gotUpdate)
	}
}

func TestSlackClientErrorResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":false,"error":"channel_not_found"}`))
	}))
	defer srv.Close()
	c := NewSlackClient("xoxb-test", srv.URL, time.Second)
	if _, err := c.PostMessage(context.Background(), "C1", "T1", "hi"); err == nil {
		t.Fatal("expected error from ok:false response")
	}
}
