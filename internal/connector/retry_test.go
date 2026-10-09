package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// retryAurora is a stub whose process ends with status (interrupted or failed)
// until it is retried, and then completes.
type retryAurora struct {
	server  *httptest.Server
	mu      sync.Mutex
	retried bool
	retryCh chan struct{}
}

func newRetryAurora(status string) *retryAurora {
	s := &retryAurora{retryCh: make(chan struct{}, 4)}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"session":{"id":"ses_1","name":"slack:100.1"}}`))
	})
	mux.HandleFunc("POST /v1/sessions/{id}/processes", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"proc_1","session_id":"ses_1","status":"running","journal_length":1}`))
	})
	mux.HandleFunc("GET /v1/processes/{id}", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.retried {
			_, _ = w.Write([]byte(`{"id":"proc_1","status":"completed","answer":"Recovered answer.","journal_length":3}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"proc_1","status":"` + status + `","error":"context deadline exceeded","journal_length":2}`))
	})
	mux.HandleFunc("GET /v1/sessions/{id}", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"session":{"id":"ses_1"},"processes":[{"id":"proc_1","entries":[{"position":0,"syscall":{"name":"sys.input"},"outcome":{"status":"result"}}]}]}`))
	})
	mux.HandleFunc("POST /v1/processes/{id}/retry", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Mode string `json:"mode"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		if r.PathValue("id") != "proc_1" || in.Mode != "resume" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.retried = true
		s.mu.Unlock()
		s.retryCh <- struct{}{}
		_, _ = w.Write([]byte(`{"id":"proc_1","status":"queued"}`))
	})
	s.server = httptest.NewServer(mux)
	return s
}

// An interrupted or failed process is an end for the thread: the connector says
// so and offers a Retry button. A click resumes the process, and its answer
// arrives.
func TestEndedProcessOffersRetry(t *testing.T) {
	for _, c := range []struct{ status, notice string }{
		{StatusInterrupted, "stopped before it finished"},
		{StatusFailed, "investigation failed"},
	} {
		t.Run(c.status, func(t *testing.T) {
			a := newRetryAurora(c.status)
			defer a.server.Close()
			sl := newHITLSlack()
			defer sl.close()

			cfg := Config{
				ChannelID:      "C1",
				Manifest:       json.RawMessage(`{"version":4}`),
				PollInterval:   5 * time.Millisecond,
				ProcessTimeout: 5 * time.Second,
				HTTPTimeout:    2 * time.Second,
			}
			slack := NewSlackClient("xoxb-test", sl.server.URL, cfg.HTTPTimeout)
			conn := New(cfg, NewAuroraClient(a.server.URL, cfg.HTTPTimeout), slack, discardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			conn.Start(ctx)

			conn.dispatchEvent(mentionEvent("U1", "<@UBOT> @duty dig into the outage", "100.1", ""))

			var noticeTS string
			deadline := time.After(3 * time.Second)
			for noticeTS == "" {
				select {
				case p := <-sl.postCh:
					if p.hasBlocks && strings.Contains(p.text, c.notice) {
						noticeTS = p.ts
					}
				case <-deadline:
					t.Fatal("no interrupted notice with a Retry button was posted")
				}
			}

			value, _ := json.Marshal(buttonValue{Process: "proc_1", Session: "ses_1", Thread: "100.1"})
			payload, _ := json.Marshal(map[string]any{
				"type":    "block_actions",
				"user":    map[string]any{"id": "U1"},
				"channel": map[string]any{"id": "C1"},
				"message": map[string]any{"ts": noticeTS},
				"actions": []any{map[string]any{"action_id": actionRetry, "value": string(value)}},
			})
			conn.route(socketMessage{Type: "interactive", EnvelopeID: "env1", Payload: payload})

			select {
			case <-a.retryCh:
			case <-time.After(3 * time.Second):
				t.Fatal("the process was never retried")
			}
			deadline = time.After(3 * time.Second)
			for {
				select {
				case p := <-sl.postCh:
					if strings.Contains(p.text, "Recovered answer.") {
						return
					}
				case <-deadline:
					t.Fatal("the retried process's answer was not posted")
				}
			}
		})
	}
}

// The prompt leads with what the bot wants to do, shows the exact call below,
// and drops the runtime's "Approve <capability>:" prefix.
func TestApprovalPartsReadWell(t *testing.T) {
	task := func(summary, name, args string) Task {
		tk := Task{Summary: summary}
		tk.Syscall.Name = name
		tk.Syscall.Args = json.RawMessage(args)
		return tk
	}
	cases := []struct {
		task              Task
		headline, details string
	}{
		{task("Approve core.internet/POST: post to the API\nargs: {\"method\":\"POST\"}", "core.internet", `{}`),
			"post to the API", `args: {"method":"POST"}`},
		{task("Approve core.command/kubectl-get: List pods\nrun: \"/bin/sh\" \"get.sh\"\ndir: /srv", "core.command", `{}`),
			"List pods", "run: \"/bin/sh\" \"get.sh\"\ndir: /srv"},
		{task("Approve core.openaiApi/chat: chat: create a Chat Completions response\nmodel: gpt-4o", "core.openaiApi", `{}`),
			"chat: create a Chat Completions response", "model: gpt-4o"},
		{task("Approve declassifying [secret]: the date is public now", "sys.declassify", `{}`),
			"declassifying [secret]: the date is public now", ""},
		{task("Approve core.internet/GET\nargs: {}", "core.internet", `{"method":"GET","url":"https://status.example.com/x"}`),
			"🌐 Reading status.example.com", "args: {}"},
	}
	for _, c := range cases {
		headline, details := approvalParts(c.task)
		if headline != c.headline || details != c.details {
			t.Errorf("approvalParts(%q) = %q, %q; want %q, %q", c.task.Summary, headline, details, c.headline, c.details)
		}
	}
	if got := escapeMrkdwn("<!channel> & ```"); got != "&lt;!channel&gt; &amp; '''" {
		t.Errorf("escapeMrkdwn = %q", got)
	}
}

func TestSyscallLabelsNameTheWork(t *testing.T) {
	cases := map[string]struct{ name, args string }{
		"🌐 Reading status.example.com": {"core.internet", `{"method":"GET","url":"https://status.example.com/x"}`},
		"🌐 POST to api.example.com":    {"core.internet", `{"method":"POST","url":"https://api.example.com/y"}`},
		"🔌 Calling search_onyx":        {"core.httpTemplate", `{"operation":"search_onyx"}`},
		"🖥️ Running kubectl-get":       {"core.command", `{"operation":"kubectl-get","params":{}}`},
		"🗒️ Saving notes":              {"core.scratch", `{"operation":"put"}`},
		"":                             {"sys.begin", `{}`},
	}
	for want, c := range cases {
		if got := syscallLabel(c.name, json.RawMessage(c.args)); got != want {
			t.Errorf("syscallLabel(%s, %s) = %q, want %q", c.name, c.args, got, want)
		}
	}
	// The agent saves every tool result; the save is not a step of the work.
	if got := syscallLabel("core.scratch", json.RawMessage(`{"operation":"store","value":{}}`)); got != "" {
		t.Errorf("an automatic save is labelled %q, want it hidden", got)
	}
}
