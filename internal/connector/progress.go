package connector

import (
	"cmp"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

// syscallLabel turns one journal entry into a short line for the Slack thread.
// The duty bot's point is legibility to an on-call human, so the journal is
// narrated in plain language. Protocol and housekeeping syscalls (savepoints,
// progress notes, time) return "" and are not shown.
func syscallLabel(name string, args json.RawMessage) string {
	var call struct {
		Operation string `json:"operation"`
		Method    string `json:"method"`
		URL       string `json:"url"`
	}
	_ = json.Unmarshal(args, &call)
	switch name {
	case "sys.input":
		return "📥 Read the request"
	case "sys.output":
		return "📤 Wrote the answer"
	case "core.openaiApi":
		return "🧠 Thinking"
	case "core.internet":
		host := "the web"
		if u, err := url.Parse(call.URL); err == nil && u.Host != "" {
			host = u.Host
		}
		if call.Method == "" || strings.EqualFold(call.Method, "GET") {
			return "🌐 Reading " + host
		}
		return "🌐 " + strings.ToUpper(call.Method) + " to " + host
	case "core.httpTemplate":
		return "🔌 Calling " + cmp.Or(call.Operation, "a service")
	case "core.command":
		return "🖥️ Running " + cmp.Or(call.Operation, "a command")
	case "core.scratch":
		switch call.Operation {
		case "store":
			return "" // the agent saves every tool result: housekeeping
		case "put":
			return "🗒️ Saving notes"
		}
		return "🗒️ Checking notes"
	case "sys.timer":
		return "⏳ Waiting on a timer"
	case "sys.declassify":
		return "🔓 Asking to release labelled data"
	case "sys.abort":
		return "↩️ Starting a step again"
	case "sys.begin", "sys.commit", "sys.log", "sys.now", "sys.random":
		return ""
	default:
		return "⚙️ " + name
	}
}

// step is one collapsed run of the timeline: a label and how many consecutive
// syscalls of that kind produced it.
type step struct {
	label string
	count int
	// active is true while the underlying syscall is still in flight (its
	// journal entry carries a yield outcome) — the "what is running right now".
	active bool
}

// maxTrailIcons caps the icon trail of a status message, so a long
// investigation does not grow an unbounded Slack message.
const maxTrailIcons = 60

// buildTimeline folds a process's journal into a compact, human-readable list
// of steps: consecutive syscalls of the same kind collapse into one line with a
// count, and housekeeping syscalls are dropped. running reports whether the
// process itself is still going, which decides if the final step is shown as
// in-progress.
func buildTimeline(entries []JournalEntry, running bool) []step {
	var steps []step
	for _, e := range entries {
		label := syscallLabel(e.Syscall.Name, e.Syscall.Args)
		if label == "" {
			continue
		}
		inFlight := e.Outcome.Status == "yield" || e.Outcome.Status == ""
		if n := len(steps); n > 0 && steps[n-1].label == label {
			steps[n-1].count++
			steps[n-1].active = steps[n-1].active || inFlight
			continue
		}
		steps = append(steps, step{label: label, count: 1, active: inFlight})
	}
	// Only the final step can be genuinely "running now", and only if the
	// process has not finished. Clear stale active flags on earlier steps.
	for i := range steps {
		if i < len(steps)-1 || !running {
			steps[i].active = false
		}
	}
	return steps
}

// renderStatus produces the text of the single status message the connector
// keeps updated in the thread as a process runs: the header, a trail of one
// icon per step, and, while the process runs, the latest step in words. When
// the process has ended, the header counts the steps instead.
func renderStatus(header string, entries []JournalEntry, running bool) string {
	steps := buildTimeline(entries, running)
	if len(steps) == 0 {
		return header
	}
	var b strings.Builder
	b.WriteString(header)
	if !running {
		calls := 0
		for _, s := range steps {
			calls += s.count
		}
		fmt.Fprintf(&b, " · %d steps", calls)
	}

	// Every label starts with its icon.
	icons := make([]string, 0, len(steps))
	for _, s := range steps {
		icon, _, _ := strings.Cut(s.label, " ")
		icons = append(icons, icon)
	}
	b.WriteString("\n")
	if len(icons) > maxTrailIcons {
		b.WriteString("… ")
		icons = icons[len(icons)-maxTrailIcons:]
	}
	b.WriteString(strings.Join(icons, " "))

	if running {
		latest := steps[len(steps)-1]
		b.WriteString("\n")
		b.WriteString(latest.label)
		if latest.active {
			b.WriteString("…")
		}
	}
	return b.String()
}
