package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// AuroraClient is a thin client for a local aurora-dist instance — the single
// HTTP way into the runtime, versioned /v1. The connector only ever needs four
// calls: create a session (one per Slack thread), start a process (one per user
// message — the "aurora spawn"), poll a process for its status, and read the
// session log for the journal (the syscalls a process is running). Everything
// else the API offers is out of scope here.
//
// We mirror only the response fields we use rather than importing the
// aurora-dist module: the connector talks to the runtime purely over HTTP+JSON,
// so coupling it to the runtime's Go types (and their heavy transitive deps —
// wazero, sqlite) would buy nothing.
type AuroraClient struct {
	baseURL string
	http    *http.Client
}

// maxAuroraResponseBytes backstops the response read in do(). It is generous —
// a session log grows with the investigation — but bounded so a runaway or buggy
// response cannot exhaust the connector's memory.
const maxAuroraResponseBytes = 16 << 20 // 16 MiB

// NewAuroraClient returns a client for the aurora-dist base URL (e.g.
// http://localhost:8080). timeout bounds each individual request.
func NewAuroraClient(baseURL string, timeout time.Duration) *AuroraClient {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &AuroraClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: timeout},
	}
}

// Process statuses, mirrored from aurora's agent.ProcessStatus. The terminal
// set ends a poll; everything else means "still working".
const (
	StatusQueued      = "queued"
	StatusRunning     = "running"
	StatusStopping    = "stopping"
	StatusYielded     = "yielded"
	StatusWaitingTask = "waiting_for_task"
	StatusInterrupted = "interrupted"
	StatusCompleted   = "completed"
	StatusStopped     = "stopped"
	StatusFailed      = "failed"
	StatusCompensated = "compensated"
)

// IsTerminal reports whether a process status is final — the process will not
// advance further, so polling can stop.
func IsTerminal(status string) bool {
	switch status {
	case StatusCompleted, StatusStopped, StatusFailed, StatusCompensated:
		return true
	default:
		return false
	}
}

// ProcessSnapshot is aurora's cheap single-process status poll
// (GET /v1/processes/{id}). JournalLength lets a caller detect new syscalls
// without refetching the whole session log.
type ProcessSnapshot struct {
	ID            string `json:"id"`
	SessionID     string `json:"session_id"`
	Status        string `json:"status"`
	Answer        string `json:"answer"`
	Error         string `json:"error"`
	JournalLength int    `json:"journal_length"`
}

// SessionLog is aurora's one comprehensive read (GET /v1/sessions/{id}). We keep
// only the session id/active-process fields and each process's journal — the
// journal is where the syscalls live, and there is no narrower endpoint for it.
type SessionLog struct {
	Session struct {
		ID              string `json:"id"`
		Name            string `json:"name"`
		ActiveProcessID string `json:"active_process_id"`
	} `json:"session"`
	Processes []ProcessLog `json:"processes"`
}

// ProcessLog is one process inside the session log: its snapshot fields, the
// flat journal of every syscall it has issued, and its tasks (durable
// approvals/timers the process is parked on or has resolved).
type ProcessLog struct {
	ID            string         `json:"id"`
	Status        string         `json:"status"`
	Answer        string         `json:"answer"`
	Error         string         `json:"error"`
	JournalLength int            `json:"journal_length"`
	Entries       []JournalEntry `json:"entries"`
	Tasks         []Task         `json:"tasks"`
}

// Task states, mirrored from aurora's task.State (= sys.Decision). A pending
// task is one awaiting resolution; a human-approvable one parks the process in
// waiting_for_task until POST /v1/tasks/{id}/resolve settles it.
const (
	TaskPending   = "pending"
	TaskApproved  = "approved"
	TaskCompleted = "completed"
	TaskDenied    = "denied"
	TaskFailed    = "failed"
	TaskCancelled = "cancelled"
	TaskExpired   = "expired"
	TaskExecuted  = "executed"
)

// Resolution decisions accepted by POST /v1/tasks/{id}/resolve. For a duty bot's
// human-in-the-loop approvals only two matter: approve lets the syscall proceed,
// deny refuses it (the guest sees a denied result and reacts).
const (
	DecisionApproved = "approved"
	DecisionDenied   = "denied"
)

// Task is one durable task on a process (GET /v1/sessions/{id} →
// processes[].tasks[]). The connector reads pending human approvals from here —
// including the ResolutionToken, the bearer credential it presents back to
// resolve the task — and renders Summary/Syscall into the Slack prompt.
type Task struct {
	ID        string `json:"id"`
	ProcessID string `json:"process_id"`
	Summary   string `json:"summary"`
	State     string `json:"state"`
	Syscall   struct {
		Name string          `json:"name"`
		Args json.RawMessage `json:"args,omitempty"`
	} `json:"syscall"`
	ResolutionToken string     `json:"resolution_token"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
}

// NeedsHumanDecision reports whether a task is a pending approval awaiting a
// human — as opposed to a timer park (sys.timer), which the runtime's timer
// actor resolves on its own and which no one should be prompted about.
func (t Task) NeedsHumanDecision() bool {
	return t.State == TaskPending && t.Syscall.Name != "sys.timer"
}

// JournalEntry is one syscall of a process's journal. Outcome.Status is one of
// aurora's syscall statuses: "result" (done), "failed", or "yield" (in flight or
// parked — the entry whose syscall is running right now).
type JournalEntry struct {
	Position int    `json:"position"`
	Revision uint64 `json:"revision"`
	Syscall  struct {
		Name string `json:"name"`
	} `json:"syscall"`
	Outcome struct {
		Status  string `json:"status"`
		Message string `json:"message,omitempty"`
	} `json:"outcome"`
}

// Process finds a process by id within the log, or nil if absent.
func (l *SessionLog) Process(processID string) *ProcessLog {
	for i := range l.Processes {
		if l.Processes[i].ID == processID {
			return &l.Processes[i]
		}
	}
	return nil
}

// Task finds a task by id across all of the session's processes, or nil if
// absent — used to look a task's current state and resolution token back up
// when a human clicks Approve/Deny.
func (l *SessionLog) Task(taskID string) *Task {
	for i := range l.Processes {
		for j := range l.Processes[i].Tasks {
			if l.Processes[i].Tasks[j].ID == taskID {
				return &l.Processes[i].Tasks[j]
			}
		}
	}
	return nil
}

// apiError is aurora's one error shape: a human message and a stable code.
type apiError struct {
	Status  int
	Message string `json:"error"`
	Code    string `json:"code"`
}

func (e *apiError) Error() string {
	return fmt.Sprintf("aurora %d %s: %s", e.Status, e.Code, e.Message)
}

// IsConflict reports whether an error is aurora's 409 — most importantly the
// "session already has an active process" conflict, which the connector avoids
// by serializing a thread's processes but still tolerates defensively.
func IsConflict(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.Code == "conflict"
}

// CreateSession creates a session (one per Slack thread) and returns its id.
// name is an optional human handle; tags are opaque key/values stored on the
// session.
func (c *AuroraClient) CreateSession(ctx context.Context, name string, tags map[string]string) (string, error) {
	body := map[string]any{}
	if name != "" {
		body["name"] = name
	}
	if len(tags) > 0 {
		body["tags"] = tags
	}
	var out SessionLog
	if err := c.do(ctx, http.MethodPost, "/v1/sessions", body, &out); err != nil {
		return "", err
	}
	if out.Session.ID == "" {
		return "", fmt.Errorf("aurora: created session has no id")
	}
	return out.Session.ID, nil
}

// SessionSummary is one entry of the session list (GET /v1/sessions).
type SessionSummary struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// ListSessions returns every session summary the runtime holds.
func (c *AuroraClient) ListSessions(ctx context.Context) ([]SessionSummary, error) {
	var out []SessionSummary
	err := c.do(ctx, http.MethodGet, "/v1/sessions", nil, &out)
	return out, err
}

// FindSessionByName returns the id of the session with the given explicit name,
// or "" if none — the durable lookup that lets a Slack thread reattach to its
// aurora session after the connector restarts or retires the in-memory worker.
func (c *AuroraClient) FindSessionByName(ctx context.Context, name string) (string, error) {
	if name == "" {
		return "", nil
	}
	sessions, err := c.ListSessions(ctx)
	if err != nil {
		return "", err
	}
	for _, s := range sessions {
		if s.Name == name {
			return s.ID, nil
		}
	}
	return "", nil
}

// EnsureSession returns the id of the session with the given name, creating it
// if absent. Session names are unique per tenant, so a create that races (or a
// restart) yields a conflict; we recover by looking the existing session up by
// name. This is what makes "one Slack thread → one aurora session" hold across
// connector restarts.
func (c *AuroraClient) EnsureSession(ctx context.Context, name string, tags map[string]string) (string, error) {
	id, err := c.CreateSession(ctx, name, tags)
	if err == nil {
		return id, nil
	}
	if !IsConflict(err) {
		return "", err
	}
	existing, findErr := c.FindSessionByName(ctx, name)
	if findErr != nil {
		return "", fmt.Errorf("reattach session %q after conflict: %w", name, findErr)
	}
	if existing == "" {
		return "", err
	}
	return existing, nil
}

// CreateProcess starts a process on a session — the aurora spawn. input is the
// user's message; manifest is the opaque grant set (LLM config, capabilities)
// passed through verbatim. Omitted (nil) means an empty composition.
func (c *AuroraClient) CreateProcess(ctx context.Context, sessionID, input string, manifest json.RawMessage) (ProcessSnapshot, error) {
	body := map[string]any{"input": input}
	if len(manifest) > 0 {
		body["manifest"] = manifest
	}
	var out ProcessSnapshot
	err := c.do(ctx, http.MethodPost, "/v1/sessions/"+sessionID+"/processes", body, &out)
	return out, err
}

// GetProcess is the cheap status poll.
func (c *AuroraClient) GetProcess(ctx context.Context, processID string) (ProcessSnapshot, error) {
	var out ProcessSnapshot
	err := c.do(ctx, http.MethodGet, "/v1/processes/"+processID, nil, &out)
	return out, err
}

// GetSessionLog reads the whole session log — the only place the per-syscall
// journal and the per-process tasks are exposed.
func (c *AuroraClient) GetSessionLog(ctx context.Context, sessionID string) (SessionLog, error) {
	var out SessionLog
	err := c.do(ctx, http.MethodGet, "/v1/sessions/"+sessionID, nil, &out)
	return out, err
}

// ResolveTask settles a pending task — the human-in-the-loop decision. token is
// the task's ResolutionToken (the bearer credential from the session log);
// decision is DecisionApproved or DecisionDenied; actor and reason are recorded
// on the resolution for the audit trail. On success the runtime resumes the
// process that was parked on the task.
func (c *AuroraClient) ResolveTask(ctx context.Context, taskID, token, decision, actor, reason string) (Task, error) {
	body := map[string]any{
		"resolution_token": token,
		"resolution": map[string]any{
			"decision": decision,
			"actor":    actor,
			"reason":   reason,
		},
	}
	var out Task
	err := c.do(ctx, http.MethodPost, "/v1/tasks/"+taskID+"/resolve", body, &out)
	return out, err
}

// Healthz reports whether the aurora-dist instance answers its health check.
func (c *AuroraClient) Healthz(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("aurora healthz: status %d", resp.StatusCode)
	}
	return nil
}

func (c *AuroraClient) do(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("aurora %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	// Bound the read (like the Slack and socket clients do). aurora is a trusted
	// local process, so the ceiling is generous — well above any realistic session
	// log — and exists only as a backstop against a runaway response.
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxAuroraResponseBytes))
	if err != nil {
		return fmt.Errorf("aurora %s %s: read body: %w", method, path, err)
	}
	if resp.StatusCode >= 300 {
		ae := &apiError{Status: resp.StatusCode}
		_ = json.Unmarshal(raw, ae) // best effort; body may not be the error shape
		if ae.Message == "" {
			ae.Message = strings.TrimSpace(string(raw))
		}
		return ae
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("aurora %s %s: decode response: %w", method, path, err)
		}
	}
	return nil
}
