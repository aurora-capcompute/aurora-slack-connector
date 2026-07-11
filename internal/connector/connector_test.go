package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- stubs ---

// auroraStub is a minimal stateful aurora-dist: sessions are unique by name, a
// process completes after two polls, and its answer echoes its input so a test
// can trace the round trip.
type auroraStub struct {
	mu                sync.Mutex
	server            *httptest.Server
	sessionByName     map[string]string
	nextSession       int
	nextProcess       int
	inputByProc       map[string]string
	pollsByProc       map[string]int
	sessionCreates    int
	processCreates    int
	rejectProcessEcho bool // reject createProcess with a 400 that echoes the input
}

func newAuroraStub() *auroraStub {
	s := &auroraStub{
		sessionByName: map[string]string{},
		inputByProc:   map[string]string{},
		pollsByProc:   map[string]int{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sessions", s.createSession)
	mux.HandleFunc("GET /v1/sessions", s.listSessions)
	mux.HandleFunc("POST /v1/sessions/{id}/processes", s.createProcess)
	mux.HandleFunc("GET /v1/sessions/{id}", s.getSession)
	mux.HandleFunc("GET /v1/processes/{id}", s.getProcess)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	s.server = httptest.NewServer(mux)
	return s
}

func (s *auroraStub) close() { s.server.Close() }

func (s *auroraStub) createSession(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name string `json:"name"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &in)
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.sessionByName[in.Name]; ok {
		w.WriteHeader(http.StatusConflict)
		fmt.Fprintf(w, `{"error":"name %q in use","code":"conflict"}`, in.Name)
		_ = id
		return
	}
	s.nextSession++
	id := fmt.Sprintf("ses_%d", s.nextSession)
	s.sessionByName[in.Name] = id
	s.sessionCreates++
	fmt.Fprintf(w, `{"session":{"id":%q,"name":%q}}`, id, in.Name)
}

func (s *auroraStub) listSessions(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var parts []string
	for name, id := range s.sessionByName {
		parts = append(parts, fmt.Sprintf(`{"id":%q,"name":%q}`, id, name))
	}
	fmt.Fprintf(w, "[%s]", strings.Join(parts, ","))
}

func (s *auroraStub) createProcess(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Input string `json:"input"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &in)
	sessionID := r.PathValue("id")
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rejectProcessEcho {
		// Aurora rejecting a process and echoing the guest's input back in the
		// error message — the path that would carry an injected token into Slack.
		w.WriteHeader(http.StatusBadRequest)
		raw, _ := json.Marshal(map[string]string{"error": "input rejected: " + in.Input, "code": "invalid"})
		_, _ = w.Write(raw)
		return
	}
	s.nextProcess++
	id := fmt.Sprintf("proc_%d", s.nextProcess)
	s.inputByProc[id] = in.Input
	s.processCreates++
	fmt.Fprintf(w, `{"id":%q,"session_id":%q,"status":"running","journal_length":1}`, id, sessionID)
}

// procStatus computes a process's status/answer/journal from its poll count, so
// GetProcess and GetSessionLog agree. Done after two polls.
func (s *auroraStub) procStatus(id string) (status, answer string, jl int) {
	polls := s.pollsByProc[id]
	if polls >= 2 {
		return StatusCompleted, "Re: " + s.inputByProc[id], 3
	}
	return StatusRunning, "", 2
}

func (s *auroraStub) getProcess(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pollsByProc[id]++
	status, answer, jl := s.procStatus(id)
	fmt.Fprintf(w, `{"id":%q,"status":%q,"answer":%q,"journal_length":%d}`, id, status, answer, jl)
}

func (s *auroraStub) getSession(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	s.mu.Lock()
	defer s.mu.Unlock()
	// Render the most recently created process for this session.
	id := fmt.Sprintf("proc_%d", s.nextProcess)
	status, answer, _ := s.procStatus(id)
	active := id
	if IsTerminal(status) {
		active = ""
	}
	entries := `{"position":0,"syscall":{"name":"sys.input"},"outcome":{"status":"result"}},` +
		`{"position":1,"syscall":{"name":"core.internet"},"outcome":{"status":"yield"}}`
	if IsTerminal(status) {
		entries = `{"position":0,"syscall":{"name":"sys.input"},"outcome":{"status":"result"}},` +
			`{"position":1,"syscall":{"name":"core.internet"},"outcome":{"status":"result"}},` +
			`{"position":2,"syscall":{"name":"sys.output"},"outcome":{"status":"result"}}`
	}
	fmt.Fprintf(w, `{"session":{"id":%q,"active_process_id":%q},"processes":[{"id":%q,"status":%q,"answer":%q,"entries":[%s]}]}`,
		sessionID, active, id, status, answer, entries)
}

// slackStub records posts, updates, and reactions and streams post texts on a
// channel. messages seeds conversations.history so a reaction trigger can read
// the message it fired on.
type slackStub struct {
	server          *httptest.Server
	mu              sync.Mutex
	posts           int
	updates         int
	postCh          chan string
	updateCh        chan string             // text of each chat.update
	reactCh         chan string             // "add:name:ts" / "remove:name:ts"
	messages        map[string]slackMessage // ts -> message for conversations.history
	historyFailures int                     // fail this many conversations.history calls first
}

func newSlackStub() *slackStub {
	s := &slackStub{
		postCh:   make(chan string, 64),
		updateCh: make(chan string, 64),
		reactCh:  make(chan string, 64),
		messages: map[string]slackMessage{},
	}
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
			s.posts++
			ts := fmt.Sprintf("ts_%d", s.posts)
			s.mu.Unlock()
			if text, _ := in["text"].(string); true {
				select {
				case s.postCh <- text:
				default:
				}
			}
			fmt.Fprintf(w, `{"ok":true,"ts":%q}`, ts)
		case "/chat.update":
			s.mu.Lock()
			s.updates++
			s.mu.Unlock()
			if text, _ := in["text"].(string); true {
				select {
				case s.updateCh <- text:
				default:
				}
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/reactions.add", "/reactions.remove":
			verb := "add"
			if r.URL.Path == "/reactions.remove" {
				verb = "remove"
			}
			name, _ := in["name"].(string)
			ts, _ := in["timestamp"].(string)
			select {
			case s.reactCh <- verb + ":" + name + ":" + ts:
			default:
			}
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "/conversations.history":
			latest, _ := in["latest"].(string)
			s.mu.Lock()
			if s.historyFailures > 0 {
				s.historyFailures--
				s.mu.Unlock()
				_, _ = w.Write([]byte(`{"ok":false,"error":"ratelimited"}`))
				return
			}
			msg, ok := s.messages[latest]
			s.mu.Unlock()
			if !ok {
				_, _ = w.Write([]byte(`{"ok":true,"messages":[]}`))
				return
			}
			raw, _ := json.Marshal(struct {
				OK       bool           `json:"ok"`
				Messages []slackMessage `json:"messages"`
			}{OK: true, Messages: []slackMessage{msg}})
			_, _ = w.Write(raw)
		default:
			http.NotFound(w, r)
		}
	}))
	return s
}

func (s *slackStub) close() { s.server.Close() }

// setMessage seeds a message conversations.history will return by its ts.
func (s *slackStub) setMessage(m slackMessage) {
	s.mu.Lock()
	s.messages[m.TS] = m
	s.mu.Unlock()
}

// newTestConnector wires a connector to the two stubs with fast polling.
func newTestConnector(t *testing.T, a *auroraStub, sl *slackStub) *Connector {
	t.Helper()
	cfg := Config{
		SlackAppToken:   "xapp-test",
		ChannelID:       "C1",
		TriggerKeyword:  "@duty",
		TriggerReaction: "eyes",
		AuroraBaseURL:   a.server.URL,
		Manifest:        json.RawMessage(`{"version":4}`),
		PollInterval:    5 * time.Millisecond,
		ProcessTimeout:  5 * time.Second,
		HTTPTimeout:     2 * time.Second,
	}
	aurora := NewAuroraClient(a.server.URL, cfg.HTTPTimeout)
	slack := NewSlackClient("xoxb-test", cfg.HTTPTimeout)
	slack.baseURL = sl.server.URL
	return New(cfg, aurora, slack, discardLogger())
}

func waitForPost(t *testing.T, ch chan string, substr string, timeout time.Duration) string {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case text := <-ch:
			if strings.Contains(text, substr) {
				return text
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a post containing %q", substr)
			return ""
		}
	}
}

func mentionEvent(user, text, ts, threadTS string) slackEnvelope {
	return slackEnvelope{
		Type:    "event_callback",
		EventID: "Ev" + ts,
		Event: slackEvent{
			Type: "app_mention", User: user, Text: text, TS: ts, ThreadTS: threadTS, Channel: "C1",
		},
	}
}

// --- tests ---

func TestConnectorEndToEnd(t *testing.T) {
	a := newAuroraStub()
	defer a.close()
	sl := newSlackStub()
	defer sl.close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newTestConnector(t, a, sl)
	conn.Start(ctx)
	if conn.botUserID != "UBOT" {
		t.Fatalf("bot identity not resolved: %q", conn.botUserID)
	}

	// A mention opens a new thread → new session → process → answer.
	conn.dispatchEvent(mentionEvent("U1", "<@UBOT> @duty why is checkout 500ing?", "100.1", ""))
	answer := waitForPost(t, sl.postCh, "Re: why is checkout 500ing?", 3*time.Second)
	if !strings.Contains(answer, "Re: why is checkout 500ing?") {
		t.Fatalf("answer text = %q", answer)
	}

	a.mu.Lock()
	sc, pc := a.sessionCreates, a.processCreates
	name := ""
	for n := range a.sessionByName {
		name = n
	}
	a.mu.Unlock()
	if sc != 1 || pc != 1 {
		t.Fatalf("want 1 session / 1 process, got %d / %d", sc, pc)
	}
	if name != "slack:100.1" {
		t.Fatalf("session name = %q, want slack:100.1", name)
	}

	// A follow-up in the same thread reuses the session (shared history) and
	// runs a second process — no new session.
	conn.dispatchEvent(mentionEvent("U1", "any recent deploys?", "100.2", "100.1"))
	waitForPost(t, sl.postCh, "Re: any recent deploys?", 3*time.Second)

	a.mu.Lock()
	sc, pc = a.sessionCreates, a.processCreates
	a.mu.Unlock()
	if sc != 1 {
		t.Fatalf("follow-up created a new session: %d", sc)
	}
	if pc != 2 {
		t.Fatalf("want 2 processes total, got %d", pc)
	}

	// Progress was reported via chat.update at least once.
	sl.mu.Lock()
	updates := sl.updates
	sl.mu.Unlock()
	if updates == 0 {
		t.Fatal("expected at least one status update")
	}
}

// A native @-mention is delivered as both an app_mention and a message with the
// same ts; dedup keeps only one. The trigger decision must not depend on which
// sibling wins, so the message-typed sibling (no literal keyword) must still
// open a thread — otherwise ~half of first-contact mentions are dropped.
func TestMessageSiblingOfNativeMentionTriggers(t *testing.T) {
	a := newAuroraStub()
	defer a.close()
	sl := newSlackStub()
	defer sl.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newTestConnector(t, a, sl)
	conn.Start(ctx) // resolves botUserID = UBOT

	ev := mentionEvent("U1", "<@UBOT> check the db", "200.1", "")
	ev.Event.Type = "message" // the message sibling, not app_mention
	conn.dispatchEvent(ev)

	waitForPost(t, sl.postCh, "Re: check the db", 3*time.Second)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.processCreates != 1 {
		t.Fatalf("native mention via message event did not trigger: %d processes", a.processCreates)
	}
}

func TestConnectorIgnoresOtherChannelsAndBots(t *testing.T) {
	a := newAuroraStub()
	defer a.close()
	sl := newSlackStub()
	defer sl.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newTestConnector(t, a, sl)
	conn.Start(ctx)

	// Wrong channel.
	ev := mentionEvent("U1", "<@UBOT> hi", "1.1", "")
	ev.Event.Channel = "C-other"
	conn.dispatchEvent(ev)

	// The bot's own message.
	self := mentionEvent("UBOT", "<@UBOT> hi", "1.2", "")
	conn.dispatchEvent(self)

	// A bot_id message (another integration).
	botMsg := mentionEvent("U9", "hi", "1.3", "")
	botMsg.Event.Type = "message"
	botMsg.Event.BotID = "B123"
	conn.dispatchEvent(botMsg)

	// A plain channel message in an unknown thread (no trigger) — ignored.
	plain := mentionEvent("U1", "just chatting", "1.4", "")
	plain.Event.Type = "message"
	conn.dispatchEvent(plain)

	time.Sleep(150 * time.Millisecond)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sessionCreates != 0 || a.processCreates != 0 {
		t.Fatalf("expected no work, got %d sessions / %d processes", a.sessionCreates, a.processCreates)
	}
}

func TestConnectorDedupSameMessage(t *testing.T) {
	a := newAuroraStub()
	defer a.close()
	sl := newSlackStub()
	defer sl.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newTestConnector(t, a, sl)
	conn.Start(ctx)

	// The same user message delivered as both app_mention and message (same ts).
	appM := mentionEvent("U1", "<@UBOT> investigate", "7.7", "")
	msg := appM
	msg.Event.Type = "message"
	conn.dispatchEvent(appM)
	conn.dispatchEvent(msg)

	waitForPost(t, sl.postCh, "Re: investigate", 3*time.Second)
	time.Sleep(100 * time.Millisecond)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.processCreates != 1 {
		t.Fatalf("dedup failed: %d processes created", a.processCreates)
	}
}

// A Socket Mode events_api payload (the standard Events API body) routes through
// handleEventPayload to the same dispatch path an HTTP delivery used to.
func TestHandleEventPayloadRoutesEvent(t *testing.T) {
	a := newAuroraStub()
	defer a.close()
	sl := newSlackStub()
	defer sl.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newTestConnector(t, a, sl)
	conn.Start(ctx)

	payload := `{"type":"event_callback","event":{"type":"app_mention","user":"U1","text":"<@UBOT> ping","ts":"300.1","channel":"C1"}}`
	conn.handleEventPayload([]byte(payload))
	waitForPost(t, sl.postCh, "Re: ping", 3*time.Second)
}

// A trigger reaction (:eyes:) added to a channel message makes the bot read that
// message and investigate its text.
func TestReactionTriggersInvestigation(t *testing.T) {
	a := newAuroraStub()
	defer a.close()
	sl := newSlackStub()
	defer sl.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newTestConnector(t, a, sl)
	conn.Start(ctx)

	// The message a human reacts on.
	sl.setMessage(slackMessage{Type: "message", User: "U1", Text: "checkout is 500ing", TS: "400.1"})
	conn.dispatchEvent(slackEnvelope{Type: "event_callback", Event: slackEvent{
		Type: "reaction_added", User: "U2", Reaction: "eyes",
		Item: reactionItem{Type: "message", Channel: "C1", TS: "400.1"},
	}})

	waitForPost(t, sl.postCh, "Re: checkout is 500ing", 3*time.Second)
	a.mu.Lock()
	pc := a.processCreates
	name := ""
	for n := range a.sessionByName {
		name = n
	}
	a.mu.Unlock()
	if pc != 1 {
		t.Fatalf("reaction trigger did not run a process: %d", pc)
	}
	if name != "slack:400.1" {
		t.Fatalf("session name = %q, want slack:400.1 (thread rooted at the reacted message)", name)
	}
}

// A transient conversations.history failure must not permanently consume the
// reaction trigger: the dedup key is released so a later reaction re-triggers.
func TestReactionRetriesAfterTransientFetchFailure(t *testing.T) {
	a := newAuroraStub()
	defer a.close()
	sl := newSlackStub()
	defer sl.close()
	sl.historyFailures = 1 // the first fetch fails
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newTestConnector(t, a, sl)
	conn.Start(ctx)

	sl.setMessage(slackMessage{Type: "message", User: "U1", Text: "check the db", TS: "410.1"})
	react := slackEnvelope{Type: "event_callback", Event: slackEvent{
		Type: "reaction_added", User: "U2", Reaction: "eyes",
		Item: reactionItem{Type: "message", Channel: "C1", TS: "410.1"}}}

	conn.dispatchEvent(react) // first: history fails, key released, no investigation
	time.Sleep(100 * time.Millisecond)
	a.mu.Lock()
	pc := a.processCreates
	a.mu.Unlock()
	if pc != 0 {
		t.Fatalf("investigation ran despite the fetch failure: %d", pc)
	}

	conn.dispatchEvent(react) // second: history succeeds, the trigger is not deduped away
	waitForPost(t, sl.postCh, "Re: check the db", 3*time.Second)
}

// A wrong reaction, a reaction in another channel, and a reaction on a bot
// message are all ignored.
func TestReactionIgnoresNonTriggers(t *testing.T) {
	a := newAuroraStub()
	defer a.close()
	sl := newSlackStub()
	defer sl.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newTestConnector(t, a, sl)
	conn.Start(ctx)

	sl.setMessage(slackMessage{Type: "message", User: "U1", Text: "hi", TS: "401.1"})
	sl.setMessage(slackMessage{Type: "message", BotID: "B1", Text: "bot post", TS: "401.2"})

	// Wrong emoji.
	conn.dispatchEvent(slackEnvelope{Type: "event_callback", Event: slackEvent{
		Type: "reaction_added", User: "U2", Reaction: "thumbsup",
		Item: reactionItem{Type: "message", Channel: "C1", TS: "401.1"}}})
	// Right emoji, wrong channel.
	conn.dispatchEvent(slackEnvelope{Type: "event_callback", Event: slackEvent{
		Type: "reaction_added", User: "U2", Reaction: "eyes",
		Item: reactionItem{Type: "message", Channel: "C-other", TS: "401.1"}}})
	// Right emoji, but the reacted message is a bot post.
	conn.dispatchEvent(slackEnvelope{Type: "event_callback", Event: slackEvent{
		Type: "reaction_added", User: "U2", Reaction: "eyes",
		Item: reactionItem{Type: "message", Channel: "C1", TS: "401.2"}}})

	time.Sleep(150 * time.Millisecond)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.processCreates != 0 {
		t.Fatalf("a non-trigger reaction started work: %d processes", a.processCreates)
	}
}

// The bot acknowledges a message it works on: 👀 when it starts, then the
// working reaction is removed and a ✅ added when the process completes.
func TestAckReactionsAddedAndSwapped(t *testing.T) {
	a := newAuroraStub()
	defer a.close()
	sl := newSlackStub()
	defer sl.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newTestConnector(t, a, sl)
	conn.Start(ctx)

	conn.dispatchEvent(mentionEvent("U1", "<@UBOT> look into it", "500.1", ""))
	waitForPost(t, sl.postCh, "Re: look into it", 3*time.Second)

	got := map[string]bool{}
	deadline := time.After(2 * time.Second)
	for len(got) < 3 {
		select {
		case r := <-sl.reactCh:
			got[r] = true
		case <-deadline:
			t.Fatalf("did not observe the expected reaction acks; saw %v", got)
		}
	}
	for _, want := range []string{"add:eyes:500.1", "remove:eyes:500.1", "add:white_check_mark:500.1"} {
		if !got[want] {
			t.Fatalf("missing reaction ack %q; saw %v", want, got)
		}
	}
}

func TestEscapeSlack(t *testing.T) {
	cases := map[string]string{
		"<!channel> now":   "&lt;!channel&gt; now",
		"ping <@U123>":     "ping &lt;@U123&gt;",
		"a & b < c > d":    "a &amp; b &lt; c &gt; d",
		"<https://x|safe>": "&lt;https://x|safe&gt;",
		"plain text":       "plain text",
	}
	for in, want := range cases {
		if got := escapeSlack(in); got != want {
			t.Errorf("escapeSlack(%q) = %q, want %q", in, got, want)
		}
	}
}

// codeSpan must strip interior backticks: that is the only way to break out of a
// mrkdwn inline-code span, so a value carrying one could otherwise smuggle a live
// <!channel>/<@U…> past the surrounding backticks.
func TestCodeSpan(t *testing.T) {
	cases := map[string]string{
		"internet.fetch":         "`internet.fetch`",
		"a`<!channel>`b":         "`a'<!channel>'b`",
		"trailing`":              "`trailing'`",
		"{\"url\":\"http://x\"}": "`{\"url\":\"http://x\"}`",
	}
	for in, want := range cases {
		got := codeSpan(in)
		if got != want {
			t.Errorf("codeSpan(%q) = %q, want %q", in, got, want)
		}
		if strings.Contains(got[1:len(got)-1], "`") {
			t.Errorf("codeSpan(%q) left an interior backtick: %q", in, got)
		}
	}
}

// A model answer that carries a Slack broadcast token is posted escaped, so it
// cannot make the bot @-broadcast the channel.
func TestOutcomeEscapesBroadcastInjection(t *testing.T) {
	a := newAuroraStub()
	defer a.close()
	sl := newSlackStub()
	defer sl.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newTestConnector(t, a, sl)
	conn.Start(ctx)

	// The stub echoes the input as the answer, so this rides through to postOutcome.
	conn.dispatchEvent(mentionEvent("U1", "<@UBOT> repeat <!channel> please", "900.1", ""))
	answer := waitForPost(t, sl.postCh, "&lt;!channel&gt;", 3*time.Second)
	if strings.Contains(answer, "<!channel>") {
		t.Fatalf("answer posted an un-escaped broadcast token: %q", answer)
	}
}

// An aurora error that echoes the guest's input (here, a rejected process
// creation) is posted escaped too — the control-plane error path is not a hole in
// the broadcast-injection defense.
func TestSpawnErrorEscapesEchoedInput(t *testing.T) {
	a := newAuroraStub()
	a.rejectProcessEcho = true
	defer a.close()
	sl := newSlackStub()
	defer sl.close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newTestConnector(t, a, sl)
	conn.Start(ctx)

	conn.dispatchEvent(mentionEvent("U1", "<@UBOT> please <!channel>", "901.1", ""))
	// report() rewrites the "queued" status message in place, so the error text
	// arrives as a chat.update rather than a fresh post.
	msg := waitForPost(t, sl.updateCh, "couldn't start the investigation", 3*time.Second)
	if strings.Contains(msg, "<!channel>") {
		t.Fatalf("spawn error posted an un-escaped broadcast token: %q", msg)
	}
}

func TestCleanTextAndTrigger(t *testing.T) {
	conn := New(Config{TriggerKeyword: "@duty"}, nil, nil, discardLogger())
	cases := []struct{ in, want string }{
		{"<@UBOT> why is it down?", "why is it down?"},
		{"@duty check the db", "check the db"},
		{"<@UBOT>   @duty   restart   the   worker  ", "restart the worker"},
		{"plain text", "plain text"},
	}
	for _, c := range cases {
		if got := conn.cleanText(c.in); got != c.want {
			t.Errorf("cleanText(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	if !conn.hasTrigger("hey @DUTY help") {
		t.Error("hasTrigger should be case-insensitive")
	}
	if conn.hasTrigger("no keyword here") {
		t.Error("hasTrigger false positive")
	}
}
