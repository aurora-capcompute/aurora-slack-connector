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
// button value carries only (non-secret) ids.
const (
	actionApprove = "duty_approve"
	actionDeny    = "duty_deny"
	actionRetry   = "duty_retry"
)

// buttonValue is encoded into each button's value. It deliberately holds no
// secret: the resolution token is re-read from the session log at click time,
// so it never lives in Slack and the flow survives a connector restart. An
// approval button carries the task; a retry button carries the process and its
// thread.
type buttonValue struct {
	Task    string `json:"t,omitempty"`
	Session string `json:"s"`
	Process string `json:"p,omitempty"`
	Thread  string `json:"th,omitempty"`
}

// postApprovalPrompt posts one interactive approval request for a pending task.
func (c *Connector) postApprovalPrompt(ctx context.Context, t *thread, tk Task) {
	value, err := json.Marshal(buttonValue{Task: tk.ID, Session: t.sessionID})
	if err != nil {
		c.logger.Warn("encode approval button", "task", tk.ID, "error", err)
		return
	}
	headline, _ := approvalParts(tk)
	fallback := "Approval needed: " + escapeMrkdwn(headline)
	ts, err := c.slack.PostBlockMessage(ctx, c.cfg.ChannelID, t.threadTS, fallback, approvalBlocks(tk, string(value)))
	if err != nil {
		c.logger.Warn("post approval prompt", "task", tk.ID, "error", err)
		return
	}
	c.logger.Info("posted approval prompt", "task", tk.ID, "syscall", tk.Syscall.Name, "message_ts", ts)
}

// approvalParts splits a task's summary into a one-line headline and the
// details below it. The runtime writes "Approve <capability>: <description>" on
// the first line, and the arguments or the command line on the lines after.
// The headline drops the "Approve" verb and the capability key, which the
// prompt shows on its own.
func approvalParts(tk Task) (headline, details string) {
	first, rest, _ := strings.Cut(strings.TrimSpace(tk.Summary), "\n")
	first = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(first), "Approve"))
	if key, description, ok := strings.Cut(first, ": "); ok && !strings.Contains(key, " ") {
		first = description
	}
	if first == "" || !strings.Contains(first, " ") && strings.Contains(first, ".") {
		// Only a capability key: say what kind of call it is instead.
		first = strings.TrimSpace(syscallLabel(tk.Syscall.Name, tk.Syscall.Args))
	}
	return first, strings.TrimSpace(rest)
}

// approvalBlocks builds the Block Kit layout for an approval prompt: what the
// bot wants to do, the exact call, when the request expires, and the buttons.
func approvalBlocks(tk Task, value string) []map[string]any {
	headline, details := approvalParts(tk)
	blocks := []map[string]any{{
		"type": "section",
		"text": map[string]any{"type": "mrkdwn", "text": "🔐 *Approval needed:* " + escapeMrkdwn(headline)},
	}}
	if details != "" {
		blocks = append(blocks, map[string]any{
			"type": "section",
			"text": map[string]any{"type": "mrkdwn", "text": "```" + escapeMrkdwn(truncate(details, 2500)) + "```"},
		})
	}
	footer := "`" + escapeMrkdwn(tk.Syscall.Name) + "`"
	if tk.ExpiresAt != nil {
		footer += fmt.Sprintf(" · expires <!date^%d^{date_short_pretty} at {time}|%s>",
			tk.ExpiresAt.Unix(), tk.ExpiresAt.UTC().Format(time.RFC3339))
	}
	blocks = append(blocks,
		map[string]any{"type": "context", "elements": []map[string]any{{"type": "mrkdwn", "text": footer}}},
		map[string]any{
			"type":     "actions",
			"block_id": "duty_approval",
			"elements": []map[string]any{
				button(actionApprove, "Approve", "primary", value),
				button(actionDeny, "Deny", "danger", value),
			},
		},
	)
	return blocks
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

// escapeMrkdwn escapes the three characters Slack's mrkdwn treats as control,
// so text from a call's arguments cannot form a link or ping the channel, and
// keeps a code block closed.
func escapeMrkdwn(s string) string {
	s = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
	return strings.ReplaceAll(s, "```", "'''")
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
	var v buttonValue
	if err := json.Unmarshal([]byte(p.Actions[0].Value), &v); err != nil {
		c.logger.Warn("decode button value", "error", err)
		return
	}
	switch p.Actions[0].ActionID {
	case actionApprove:
		c.resolveApproval(p, v, DecisionApproved)
	case actionDeny:
		c.resolveApproval(p, v, DecisionDenied)
	case actionRetry:
		c.requestRetry(p, v)
	}
}

// requestRetry hands an interrupted process to its thread's worker, which
// resumes it and follows it to its outcome. The notice loses its button, so a
// second click cannot queue a second retry.
func (c *Connector) requestRetry(p interactionPayload, v buttonValue) {
	if v.Process == "" || v.Thread == "" {
		return
	}
	c.updatePrompt(c.ctx, p, fmt.Sprintf("🔁 Retry requested by <@%s>.", p.User.ID))
	c.startWorker(v.Thread, v.Session, work{retryID: v.Process})
}

// resolveApproval re-reads the task from the session log (for its current state
// and resolution token), resolves it if still pending, and rewrites the prompt
// message to reflect the outcome — removing the buttons either way.
func (c *Connector) resolveApproval(p interactionPayload, v buttonValue, decision string) {
	ctx := c.ctx
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
	headline, _ := approvalParts(*tk)
	if tk.State != TaskPending {
		c.updatePrompt(ctx, p, fmt.Sprintf("ℹ️ Already %s: %s", tk.State, escapeMrkdwn(headline)))
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
	c.updatePrompt(ctx, p, fmt.Sprintf("%s by <@%s>: %s", verb, p.User.ID, escapeMrkdwn(headline)))
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
