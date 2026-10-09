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

// AuroraClient is a thin client for an aurora-dist instance — the single HTTP
// way into the runtime, versioned /v1. The connector needs a few calls: create
// or find a session (one per Slack thread), start a process (one per user
// message), poll a process, read the session log (the journal and tasks),
// resolve a task, and retry an interrupted process.
//
// We mirror only the response fields we use rather than importing the
// aurora-dist module: the connector talks to the runtime purely over HTTP+JSON,
// so coupling it to the runtime's Go types (and their heavy transitive deps —
// wazero, sqlite) would buy nothing.
type AuroraClient struct {
	baseURL string
	http    *http.Client
}

// NewAuroraClient returns a client for the aurora-dist base URL (e.g.
// http://localhost:8080). timeout bounds each individual request.
func NewAuroraClient(baseURL string, timeout time.Duration) *AuroraClient {
	return &AuroraClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: timeout},
	}
}

// Process statuses, mirrored from aurora's durable.ProcessStatus.
const (
	StatusQueued      = "queued"
	StatusRunning     = "running"
	StatusWaitingTask = "waiting_for_task"
	StatusInterrupted = "interrupted"
	StatusCompleted   = "completed"
	StatusStopped     = "stopped"
	StatusFailed      = "failed"
)

// IsTerminal reports whether a process will not advance on its own, so polling
// can stop. An interrupted process (past the server's time limit, or cut off by
// a restart) waits for a retry.
func IsTerminal(status string) bool {
	switch status {
	case StatusCompleted, StatusStopped, StatusFailed, StatusInterrupted:
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

// TaskPending is the state of a task that awaits resolution. A pending
// approval parks its process in waiting_for_task until
// POST /v1/tasks/{id}/resolve settles it.
const TaskPending = "pending"

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
		Name string          `json:"name"`
		Args json.RawMessage `json:"args,omitempty"`
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
// name is its unique handle, and tags are opaque key/values stored on it.
func (c *AuroraClient) CreateSession(ctx context.Context, name string, tags map[string]string) (string, error) {
	out, err := call[SessionLog](ctx, c, http.MethodPost, "/v1/sessions", map[string]any{"name": name, "tags": tags})
	return out.Session.ID, err
}

// SessionSummary is one entry of the session list (GET /v1/sessions).
type SessionSummary struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// EnsureSession returns the id of the session with the given name, creating it
// if absent. Session names are unique, so a create that races (or a restart)
// yields a conflict, and the existing session is then found by its name. This
// is what makes "one Slack thread → one aurora session" hold across connector
// restarts.
func (c *AuroraClient) EnsureSession(ctx context.Context, name string, tags map[string]string) (string, error) {
	id, err := c.CreateSession(ctx, name, tags)
	if !IsConflict(err) {
		return id, err
	}
	sessions, findErr := call[[]SessionSummary](ctx, c, http.MethodGet, "/v1/sessions", nil)
	if findErr != nil {
		return "", fmt.Errorf("reattach session %q after conflict: %w", name, findErr)
	}
	for _, s := range sessions {
		if s.Name == name {
			return s.ID, nil
		}
	}
	return "", err
}

// CreateProcess starts a process on a session — the aurora spawn. input is the
// user's message; manifest is the opaque grant set (LLM config, capabilities)
// passed through verbatim.
func (c *AuroraClient) CreateProcess(ctx context.Context, sessionID, input string, manifest json.RawMessage) (ProcessSnapshot, error) {
	return call[ProcessSnapshot](ctx, c, http.MethodPost, "/v1/sessions/"+sessionID+"/processes",
		map[string]any{"input": input, "manifest": manifest})
}

// GetProcess is the cheap status poll.
func (c *AuroraClient) GetProcess(ctx context.Context, processID string) (ProcessSnapshot, error) {
	return call[ProcessSnapshot](ctx, c, http.MethodGet, "/v1/processes/"+processID, nil)
}

// GetSessionLog reads the whole session log — the only place the per-syscall
// journal and the per-process tasks are exposed.
func (c *AuroraClient) GetSessionLog(ctx context.Context, sessionID string) (SessionLog, error) {
	return call[SessionLog](ctx, c, http.MethodGet, "/v1/sessions/"+sessionID, nil)
}

// ResolveTask settles a pending task — the human-in-the-loop decision. token is
// the task's ResolutionToken (the bearer credential from the session log);
// decision is DecisionApproved or DecisionDenied; actor and reason are recorded
// on the resolution for the audit trail. On success the runtime resumes the
// process that was parked on the task.
func (c *AuroraClient) ResolveTask(ctx context.Context, taskID, token, decision, actor, reason string) (Task, error) {
	return call[Task](ctx, c, http.MethodPost, "/v1/tasks/"+taskID+"/resolve", map[string]any{
		"resolution_token": token,
		"resolution": map[string]any{
			"decision": decision,
			"actor":    actor,
			"reason":   reason,
		},
	})
}

// Retry resumes an interrupted process from where its journal stopped.
func (c *AuroraClient) Retry(ctx context.Context, processID string) (ProcessSnapshot, error) {
	return call[ProcessSnapshot](ctx, c, http.MethodPost, "/v1/processes/"+processID+"/retry", map[string]any{"mode": "resume"})
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

// call sends one request and decodes the response into a T. A failure status
// comes back as an *apiError.
func call[T any](ctx context.Context, c *AuroraClient, method, path string, body any) (T, error) {
	var out T
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return out, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return out, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return out, fmt.Errorf("aurora %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return out, fmt.Errorf("aurora %s %s: read body: %w", method, path, err)
	}
	if resp.StatusCode >= 300 {
		ae := &apiError{Status: resp.StatusCode}
		_ = json.Unmarshal(raw, ae) // best effort; body may not be the error shape
		if ae.Message == "" {
			ae.Message = strings.TrimSpace(string(raw))
		}
		return out, ae
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("aurora %s %s: decode response: %w", method, path, err)
	}
	return out, nil
}
