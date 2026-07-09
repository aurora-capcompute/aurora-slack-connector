package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Human-in-the-loop: when a process is parked on a pending approval task, the
// connector posts an interactive Approve/Deny prompt into the thread. A click
// arrives at the interactions endpoint, is verified, and is turned into a
// POST /v1/tasks/{id}/resolve — after which aurora resumes the parked process
// and the poll loop carries on to the answer.

// Button action ids. The decision is carried by which button was clicked; the
// button value carries only the (non-secret) task and session ids.
const (
	actionApprove = "duty_approve"
	actionDeny    = "duty_deny"
)

// buttonValue is encoded into each button's value. It deliberately holds no
// secret: the resolution token is re-read from the session log at click time,
// so it never lives in Slack and the flow survives a connector restart.
type buttonValue struct {
	Task    string `json:"t"`
	Session string `json:"s"`
}

// postApprovalPrompt posts one interactive approval request for a pending task.
func (c *Connector) postApprovalPrompt(ctx context.Context, t *thread, tk Task) {
	value, err := json.Marshal(buttonValue{Task: tk.ID, Session: t.sessionID})
	if err != nil {
		c.logger.Warn("encode approval button", "task", tk.ID, "error", err)
		return
	}
	fallback := "Approval needed: " + approvalSummary(tk)
	ts, err := c.slack.PostBlockMessage(ctx, c.cfg.ChannelID, t.threadTS, fallback, approvalBlocks(tk, string(value)))
	if err != nil {
		c.logger.Warn("post approval prompt", "task", tk.ID, "error", err)
		return
	}
	c.logger.Info("posted approval prompt", "task", tk.ID, "syscall", tk.Syscall.Name, "message_ts", ts)
}

// approvalSummary is the one-line human description of what is being approved.
func approvalSummary(tk Task) string {
	if s := strings.TrimSpace(tk.Summary); s != "" {
		return s
	}
	if tk.Syscall.Name != "" {
		return "run `" + tk.Syscall.Name + "`"
	}
	return "perform an action"
}

// approvalBlocks builds the Block Kit layout for an approval prompt: a summary
// section and the Approve/Deny buttons carrying value.
func approvalBlocks(tk Task, value string) []map[string]any {
	text := "🔐 *Approval needed* — the duty bot wants to " + approvalSummary(tk)
	if tk.Syscall.Name != "" {
		text += "\n• syscall: `" + tk.Syscall.Name + "`"
	}
	if snippet := argsSnippet(tk.Syscall.Args); snippet != "" {
		text += "\n• args: `" + snippet + "`"
	}
	if tk.ExpiresAt != nil {
		text += "\n• expires: " + tk.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return []map[string]any{
		{
			"type": "section",
			"text": map[string]any{"type": "mrkdwn", "text": text},
		},
		{
			"type":     "actions",
			"block_id": "duty_approval",
			"elements": []map[string]any{
				button(actionApprove, "Approve", "primary", value),
				button(actionDeny, "Deny", "danger", value),
			},
		},
	}
}

func button(actionID, label, style, value string) map[string]any {
	return map[string]any{
		"type":      "button",
		"action_id": actionID,
		"style":     style,
		"text":      map[string]any{"type": "plain_text", "text": label},
		"value":     value,
	}
}

// argsSnippet renders a bounded, single-line view of a syscall's args for the
// prompt (the decision-relevant content of an approval — the URL being fetched,
// the value being written).
func argsSnippet(raw json.RawMessage) string {
	s := collapseSpaces(string(raw))
	if s == "" || s == "null" {
		return ""
	}
	const max = 300
	if len(s) > max {
		s = s[:max] + "…"
	}
	return s
}

// --- interactions endpoint ---

// interactionPayload is the subset of Slack's block_actions payload we use.
type interactionPayload struct {
	Type string `json:"type"`
	User struct {
		ID       string `json:"id"`
		Username string `json:"username"`
		Name     string `json:"name"`
	} `json:"user"`
	Channel struct {
		ID string `json:"id"`
	} `json:"channel"`
	Message struct {
		TS string `json:"ts"`
	} `json:"message"`
	Actions []struct {
		ActionID string `json:"action_id"`
		Value    string `json:"value"`
	} `json:"actions"`
}

// handleInteractions receives interactive component events (button clicks).
// Slack sends them form-encoded with a single `payload` field; the signature
// still covers the raw body, so it is verified before anything is parsed.
func (c *Connector) handleInteractions(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	if err := VerifySlackSignature(c.cfg.SlackSigningSecret, r.Header, body, time.Now()); err != nil {
		c.logger.Warn("rejected slack interaction", "error", err)
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	raw := form.Get("payload")
	if raw == "" {
		w.WriteHeader(http.StatusOK)
		return
	}
	var p interactionPayload
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		c.logger.Warn("decode interaction payload", "error", err)
		http.Error(w, "bad payload", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK) // ack fast; resolve asynchronously
	go c.handleInteraction(p)
}

// handleInteraction maps a verified button click to a task resolution.
func (c *Connector) handleInteraction(p interactionPayload) {
	if p.Type != "block_actions" || len(p.Actions) == 0 {
		return
	}
	if p.Channel.ID != c.cfg.ChannelID {
		return
	}
	var decision string
	switch p.Actions[0].ActionID {
	case actionApprove:
		decision = DecisionApproved
	case actionDeny:
		decision = DecisionDenied
	default:
		return
	}
	var v buttonValue
	if err := json.Unmarshal([]byte(p.Actions[0].Value), &v); err != nil {
		c.logger.Warn("decode button value", "error", err)
		return
	}
	c.resolveApproval(p, v, decision)
}

// resolveApproval re-reads the task from the session log (for its current state
// and resolution token), resolves it if still pending, and rewrites the prompt
// message to reflect the outcome — removing the buttons either way.
func (c *Connector) resolveApproval(p interactionPayload, v buttonValue, decision string) {
	ctx := c.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	log, err := c.aurora.GetSessionLog(ctx, v.Session)
	if err != nil {
		c.logger.Warn("interaction: fetch session log", "session", v.Session, "error", err)
		c.updatePrompt(ctx, p, "⚠️ Couldn't reach aurora to record this decision — please try again.")
		return
	}
	tk := log.Task(v.Task)
	if tk == nil {
		c.updatePrompt(ctx, p, "⚠️ This approval is no longer available.")
		return
	}
	if tk.State != TaskPending {
		c.updatePrompt(ctx, p, fmt.Sprintf("ℹ️ Already %s — %s", tk.State, approvalSummary(*tk)))
		return
	}
	actor := slackActor(p)
	if _, err := c.aurora.ResolveTask(ctx, tk.ID, tk.ResolutionToken, decision, actor, ""); err != nil {
		if IsConflict(err) {
			c.updatePrompt(ctx, p, "⚠️ Someone is already resolving this — try again in a moment.")
			return
		}
		c.logger.Error("resolve task", "task", tk.ID, "decision", decision, "error", err)
		c.updatePrompt(ctx, p, "⚠️ Failed to record the decision: "+oneLine(err.Error()))
		return
	}
	c.logger.Info("resolved approval", "task", tk.ID, "decision", decision, "actor", actor)

	verb := "✅ Approved"
	if decision == DecisionDenied {
		verb = "🚫 Denied"
	}
	c.updatePrompt(ctx, p, fmt.Sprintf("%s by <@%s> — %s", verb, p.User.ID, approvalSummary(*tk)))
}

// updatePrompt rewrites the prompt message to plain text, which also strips its
// buttons so it cannot be clicked again.
func (c *Connector) updatePrompt(ctx context.Context, p interactionPayload, text string) {
	if p.Message.TS == "" {
		return
	}
	if err := c.slack.UpdateMessage(ctx, p.Channel.ID, p.Message.TS, text); err != nil {
		c.logger.Warn("update approval prompt", "error", err)
	}
}

// slackActor is the audit identity recorded on the resolution.
func slackActor(p interactionPayload) string {
	who := p.User.Username
	if who == "" {
		who = p.User.Name
	}
	if who == "" {
		return "slack:" + p.User.ID
	}
	return fmt.Sprintf("slack:%s (%s)", who, p.User.ID)
}
