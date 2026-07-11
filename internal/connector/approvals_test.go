package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNeedsHumanDecision(t *testing.T) {
	mk := func(name, state string) Task {
		tk := Task{State: state}
		tk.Syscall.Name = name
		return tk
	}
	if !mk("core.internet", TaskPending).NeedsHumanDecision() {
		t.Error("a pending leaf syscall should need a human")
	}
	if mk("sys.timer", TaskPending).NeedsHumanDecision() {
		t.Error("a pending timer resolves itself; no human needed")
	}
	if mk("core.internet", TaskApproved).NeedsHumanDecision() {
		t.Error("a resolved task needs no human")
	}
}

func TestResolveTaskClientBody(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tasks/task_1/resolve" {
			t.Errorf("path = %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		_, _ = w.Write([]byte(`{"id":"task_1","state":"approved"}`))
	}))
	defer srv.Close()

	c := NewAuroraClient(srv.URL, time.Second)
	if _, err := c.ResolveTask(context.Background(), "task_1", "tok_1", DecisionApproved, "slack:alice", "looks safe"); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got["resolution_token"] != "tok_1" {
		t.Fatalf("resolution_token = %v", got["resolution_token"])
	}
	res, _ := got["resolution"].(map[string]any)
	if res["decision"] != "approved" || res["actor"] != "slack:alice" || res["reason"] != "looks safe" {
		t.Fatalf("resolution = %v", res)
	}
}

// approvalAurora is a stub that parks a process on a pending approval task and
// completes it once the task is resolved.
type approvalAurora struct {
	server    *httptest.Server
	mu        sync.Mutex
	resolved  bool
	decision  string
	actor     string
	resolveCh chan struct{}
}

func newApprovalAurora() *approvalAurora {
	s := &approvalAurora{resolveCh: make(chan struct{}, 4)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("POST /v1/sessions", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"session":{"id":"ses_1","name":"slack:100.1"}}`))
	})
	mux.HandleFunc("GET /v1/sessions", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"id":"ses_1","name":"slack:100.1"}]`))
	})
	mux.HandleFunc("POST /v1/sessions/{id}/processes", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"proc_1","session_id":"ses_1","status":"running","journal_length":1}`))
	})
	mux.HandleFunc("GET /v1/processes/{id}", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.resolved {
			_, _ = w.Write([]byte(`{"id":"proc_1","status":"completed","answer":"Mitigation applied.","journal_length":4}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"proc_1","status":"waiting_for_task","journal_length":2}`))
	})
	mux.HandleFunc("GET /v1/sessions/{id}", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.resolved {
			_, _ = w.Write([]byte(`{"session":{"id":"ses_1","active_process_id":""},"processes":[{"id":"proc_1","status":"completed","answer":"Mitigation applied.","entries":[{"position":0,"syscall":{"name":"sys.input"},"outcome":{"status":"result"}}],"tasks":[{"id":"task_1","state":"approved","syscall":{"name":"core.internet"},"resolution_token":"tok_1"}]}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"session":{"id":"ses_1","active_process_id":"proc_1"},"processes":[{"id":"proc_1","status":"waiting_for_task","entries":[{"position":0,"syscall":{"name":"sys.input"},"outcome":{"status":"result"}},{"position":1,"syscall":{"name":"core.internet"},"outcome":{"status":"yield"}}],"tasks":[{"id":"task_1","state":"pending","summary":"POST https://api.example.com/mitigate","syscall":{"name":"core.internet","args":{"method":"POST"}},"resolution_token":"tok_1"}]}]}`))
	})
	mux.HandleFunc("POST /v1/tasks/{id}/resolve", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			ResolutionToken string `json:"resolution_token"`
			Resolution      struct {
				Decision string `json:"decision"`
				Actor    string `json:"actor"`
			} `json:"resolution"`
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &in)
		if r.PathValue("id") != "task_1" || in.ResolutionToken != "tok_1" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"bad token","code":"unauthorized"}`))
			return
		}
		s.mu.Lock()
		s.resolved = true
		s.decision = in.Resolution.Decision
		s.actor = in.Resolution.Actor
		s.mu.Unlock()
		s.resolveCh <- struct{}{}
		_, _ = w.Write([]byte(`{"id":"task_1","state":"` + in.Resolution.Decision + `"}`))
	})
	s.server = httptest.NewServer(mux)
	return s
}

func (s *approvalAurora) close() { s.server.Close() }

// hitlSlack captures posts (noting Block Kit messages) and updates.
type hitlSlack struct {
	server   *httptest.Server
	mu       sync.Mutex
	postCh   chan hitlPost
	updateCh chan string
	nextTS   int
}

type hitlPost struct {
	text      string
	hasBlocks bool
	ts        string
}

func newHITLSlack() *hitlSlack {
	s := &hitlSlack{postCh: make(chan hitlPost, 32), updateCh: make(chan string, 32)}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var in map[string]any
		_ = json.Unmarshal(body, &in)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"user_id":"UBOT"}`))
		case "/chat.postMessage":
			s.mu.Lock()
			s.nextTS++
			ts := "ts_" + strconv.Itoa(s.nextTS)
			s.mu.Unlock()
			blocks, _ := in["blocks"].([]any)
			text, _ := in["text"].(string)
			s.postCh <- hitlPost{text: text, hasBlocks: len(blocks) > 0, ts: ts}
			_, _ = w.Write([]byte(`{"ok":true,"ts":"` + ts + `"}`))
		case "/chat.update":
			text, _ := in["text"].(string)
			s.updateCh <- text
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/reactions.add", "/reactions.remove":
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			http.NotFound(w, r)
		}
	}))
	return s
}

func (s *hitlSlack) close() { s.server.Close() }

func TestApprovalEndToEnd(t *testing.T) {
	a := newApprovalAurora()
	defer a.close()
	sl := newHITLSlack()
	defer sl.close()

	cfg := Config{
		SlackAppToken:  "xapp-test",
		ChannelID:      "C1",
		TriggerKeyword: "@duty",
		AuroraBaseURL:  a.server.URL,
		Manifest:       json.RawMessage(`{"version":4}`),
		PollInterval:   5 * time.Millisecond,
		ProcessTimeout: 5 * time.Second,
		HTTPTimeout:    2 * time.Second,
	}
	aurora := NewAuroraClient(a.server.URL, cfg.HTTPTimeout)
	slack := NewSlackClient("xoxb-test", cfg.HTTPTimeout)
	slack.baseURL = sl.server.URL
	conn := New(cfg, aurora, slack, discardLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn.Start(ctx)

	// A mention opens the thread; the process parks on an approval.
	conn.dispatchEvent(mentionEvent("U1", "<@UBOT> @duty apply the mitigation", "100.1", ""))

	// The connector should post an interactive approval prompt (a Block Kit
	// message) into the thread.
	var promptTS string
	deadline := time.After(3 * time.Second)
	for promptTS == "" {
		select {
		case p := <-sl.postCh:
			if p.hasBlocks {
				promptTS = p.ts
			}
		case <-deadline:
			t.Fatal("no approval prompt was posted")
		}
	}

	// Simulate the user clicking Approve: the interactive payload arrives over the
	// socket (delivered here straight to the payload handler).
	val, _ := json.Marshal(buttonValue{Task: "task_1", Session: "ses_1"})
	payload := map[string]any{
		"type":    "block_actions",
		"user":    map[string]any{"id": "U1", "username": "alice"},
		"channel": map[string]any{"id": "C1"},
		"message": map[string]any{"ts": promptTS},
		"actions": []any{map[string]any{"action_id": actionApprove, "value": string(val)}},
	}
	pj, _ := json.Marshal(payload)
	conn.handleInteractionPayload(pj)

	// The task is resolved (approved).
	select {
	case <-a.resolveCh:
	case <-time.After(3 * time.Second):
		t.Fatal("task was never resolved")
	}
	a.mu.Lock()
	dec, actor := a.decision, a.actor
	a.mu.Unlock()
	if dec != "approved" {
		t.Fatalf("decision = %q", dec)
	}
	if !strings.Contains(actor, "alice") {
		t.Fatalf("actor not recorded: %q", actor)
	}

	// The prompt is rewritten to show the outcome, and the final answer posts.
	sawApprovedUpdate := false
	sawAnswer := false
	deadline = time.After(3 * time.Second)
	for !sawAnswer {
		select {
		case u := <-sl.updateCh:
			if strings.Contains(u, "Approved") {
				sawApprovedUpdate = true
			}
		case p := <-sl.postCh:
			if strings.Contains(p.text, "Mitigation applied.") {
				sawAnswer = true
			}
		case <-deadline:
			t.Fatal("did not observe resolution + answer")
		}
	}
	if !sawApprovedUpdate {
		// Drain any remaining updates briefly to catch the approved rewrite.
		select {
		case u := <-sl.updateCh:
			sawApprovedUpdate = strings.Contains(u, "Approved")
		case <-time.After(200 * time.Millisecond):
		}
	}
	if !sawApprovedUpdate {
		t.Error("approval prompt was not rewritten to an Approved outcome")
	}
}
