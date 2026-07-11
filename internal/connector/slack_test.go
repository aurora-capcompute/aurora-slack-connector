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

	c := NewSlackClient("xoxb-test", time.Second)
	c.baseURL = srv.URL
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
	c := NewSlackClient("xoxb-test", time.Second)
	c.baseURL = srv.URL
	if _, err := c.PostMessage(context.Background(), "C1", "T1", "hi"); err == nil {
		t.Fatal("expected error from ok:false response")
	}
}

func TestSlackClientReactions(t *testing.T) {
	var addBody, removeBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var in map[string]any
		_ = json.Unmarshal(body, &in)
		switch r.URL.Path {
		case "/reactions.add":
			addBody = in
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/reactions.remove":
			removeBody = in
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c := NewSlackClient("xoxb-test", time.Second)
	c.baseURL = srv.URL
	ctx := context.Background()

	if err := c.AddReaction(ctx, "C1", "100.1", "eyes"); err != nil {
		t.Fatalf("add reaction: %v", err)
	}
	if addBody["channel"] != "C1" || addBody["timestamp"] != "100.1" || addBody["name"] != "eyes" {
		t.Fatalf("reactions.add body = %v", addBody)
	}
	if err := c.RemoveReaction(ctx, "C1", "100.1", "eyes"); err != nil {
		t.Fatalf("remove reaction: %v", err)
	}
	if removeBody["name"] != "eyes" {
		t.Fatalf("reactions.remove body = %v", removeBody)
	}
}

// An already-present reaction (add) or an absent one (remove) is not an error —
// reaction acknowledgements are idempotent.
func TestSlackClientReactionsIdempotent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/reactions.add" {
			_, _ = w.Write([]byte(`{"ok":false,"error":"already_reacted"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":false,"error":"no_reaction"}`))
	}))
	defer srv.Close()
	c := NewSlackClient("xoxb-test", time.Second)
	c.baseURL = srv.URL
	ctx := context.Background()
	if err := c.AddReaction(ctx, "C1", "1.1", "eyes"); err != nil {
		t.Fatalf("already_reacted should be tolerated: %v", err)
	}
	if err := c.RemoveReaction(ctx, "C1", "1.1", "eyes"); err != nil {
		t.Fatalf("no_reaction should be tolerated: %v", err)
	}
}

func TestSlackClientGetMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/conversations.history" {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var in map[string]any
		_ = json.Unmarshal(body, &in)
		if in["inclusive"] != true {
			t.Errorf("history should request an inclusive single-message window: %v", in)
		}
		// Only the exact-ts window returns the message; any other ts returns none.
		if in["latest"] == "42.1" {
			_, _ = w.Write([]byte(`{"ok":true,"messages":[{"type":"message","user":"U1","text":"hello","ts":"42.1"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"messages":[]}`))
	}))
	defer srv.Close()
	c := NewSlackClient("xoxb-test", time.Second)
	c.baseURL = srv.URL

	msg, found, err := c.GetMessage(context.Background(), "C1", "42.1")
	if err != nil || !found {
		t.Fatalf("get message: found=%v err=%v", found, err)
	}
	if msg.Text != "hello" || msg.User != "U1" {
		t.Fatalf("message = %+v", msg)
	}

	// A ts the history doesn't return (e.g. a thread reply) is not found.
	if _, found, _ := c.GetMessage(context.Background(), "C1", "99.9"); found {
		t.Fatal("a missing message should not be found")
	}
}
