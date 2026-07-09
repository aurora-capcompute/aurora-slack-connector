package connector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// signSlack produces the headers Slack would send for a body, for tests and for
// the handler test below.
func signSlack(secret, body string, ts time.Time) http.Header {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("v0:" + strconv.FormatInt(ts.Unix(), 10) + ":" + body))
	h := http.Header{}
	h.Set("X-Slack-Request-Timestamp", strconv.FormatInt(ts.Unix(), 10))
	h.Set("X-Slack-Signature", "v0="+hex.EncodeToString(mac.Sum(nil)))
	return h
}

func TestVerifySlackSignature(t *testing.T) {
	secret := "s3cr3t"
	body := `{"type":"event_callback"}`
	now := time.Unix(1_700_000_000, 0)

	if err := VerifySlackSignature(secret, signSlack(secret, body, now), []byte(body), now); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	// Wrong secret.
	if err := VerifySlackSignature(secret, signSlack("other", body, now), []byte(body), now); err == nil {
		t.Fatal("signature under wrong secret accepted")
	}
	// Tampered body.
	if err := VerifySlackSignature(secret, signSlack(secret, body, now), []byte(body+"x"), now); err == nil {
		t.Fatal("tampered body accepted")
	}
	// Stale timestamp (10 minutes old).
	stale := signSlack(secret, body, now.Add(-10*time.Minute))
	if err := VerifySlackSignature(secret, stale, []byte(body), now); err == nil {
		t.Fatal("stale timestamp accepted")
	}
	// Missing headers.
	if err := VerifySlackSignature(secret, http.Header{}, []byte(body), now); err == nil {
		t.Fatal("missing headers accepted")
	}
}

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
