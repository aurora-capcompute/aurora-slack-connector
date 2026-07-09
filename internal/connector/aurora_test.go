package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestAuroraClientCreateSessionAndProcess(t *testing.T) {
	var createdManifest json.RawMessage
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		var in map[string]any
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &in)
		if in["name"] != "slack:T1" {
			t.Errorf("session name = %v", in["name"])
		}
		_, _ = w.Write([]byte(`{"session":{"id":"ses_1","name":"slack:T1"}}`))
	})
	mux.HandleFunc("POST /v1/sessions/{id}/processes", func(w http.ResponseWriter, r *http.Request) {
		var in map[string]json.RawMessage
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &in)
		createdManifest = in["manifest"]
		_, _ = w.Write([]byte(`{"id":"proc_1","session_id":"ses_1","status":"running","journal_length":1}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAuroraClient(srv.URL, time.Second)
	ctx := context.Background()

	id, err := c.CreateSession(ctx, "slack:T1", map[string]string{"source": "slack"})
	if err != nil || id != "ses_1" {
		t.Fatalf("create session: id=%q err=%v", id, err)
	}
	proc, err := c.CreateProcess(ctx, "ses_1", "help", json.RawMessage(`{"version":4}`))
	if err != nil || proc.ID != "proc_1" || proc.Status != StatusRunning {
		t.Fatalf("create process: %+v err=%v", proc, err)
	}
	if string(createdManifest) != `{"version":4}` {
		t.Fatalf("manifest not forwarded verbatim: %s", createdManifest)
	}
}

func TestAuroraEnsureSessionReattachesOnConflict(t *testing.T) {
	var mu sync.Mutex
	created := false
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if created {
			// Name already taken — aurora returns 409 conflict.
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"session name in use","code":"conflict"}`))
			return
		}
		created = true
		_, _ = w.Write([]byte(`{"session":{"id":"ses_A","name":"slack:T1"}}`))
	})
	mux.HandleFunc("GET /v1/sessions", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"ses_A","name":"slack:T1"},{"id":"ses_B","name":"other"}]`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewAuroraClient(srv.URL, time.Second)
	ctx := context.Background()

	first, err := c.EnsureSession(ctx, "slack:T1", nil)
	if err != nil || first != "ses_A" {
		t.Fatalf("first ensure: id=%q err=%v", first, err)
	}
	// Second call conflicts, then reattaches by name to the same id.
	second, err := c.EnsureSession(ctx, "slack:T1", nil)
	if err != nil || second != "ses_A" {
		t.Fatalf("reattach: id=%q err=%v", second, err)
	}
}

func TestAuroraGetSessionLogProcessLookup(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
			"session":{"id":"ses_1","active_process_id":"proc_1"},
			"processes":[{"id":"proc_1","status":"completed","answer":"done","journal_length":3,
				"entries":[
					{"position":0,"syscall":{"name":"sys.input"},"outcome":{"status":"result"}},
					{"position":1,"syscall":{"name":"core.internet"},"outcome":{"status":"result"}},
					{"position":2,"syscall":{"name":"sys.output"},"outcome":{"status":"result"}}
				]}]}`))
	}))
	defer srv.Close()

	c := NewAuroraClient(srv.URL, time.Second)
	log, err := c.GetSessionLog(context.Background(), "ses_1")
	if err != nil {
		t.Fatalf("get session log: %v", err)
	}
	if log.Session.ActiveProcessID != "proc_1" {
		t.Fatalf("active process = %q", log.Session.ActiveProcessID)
	}
	pl := log.Process("proc_1")
	if pl == nil || len(pl.Entries) != 3 || pl.Entries[1].Syscall.Name != "core.internet" {
		t.Fatalf("process lookup wrong: %+v", pl)
	}
	if log.Process("nope") != nil {
		t.Fatal("unexpected process match")
	}
}

func TestIsTerminal(t *testing.T) {
	for _, s := range []string{StatusCompleted, StatusFailed, StatusStopped, StatusCompensated} {
		if !IsTerminal(s) {
			t.Errorf("%s should be terminal", s)
		}
	}
	for _, s := range []string{StatusQueued, StatusRunning, StatusYielded, StatusWaitingTask, StatusInterrupted} {
		if IsTerminal(s) {
			t.Errorf("%s should not be terminal", s)
		}
	}
}
