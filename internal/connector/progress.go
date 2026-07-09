package connector

import (
	"fmt"
	"strings"
)

// syscallLabel maps an aurora syscall name to a short, human-friendly line for
// the Slack thread. The duty bot's whole point is legibility to an on-call
// human, so the journal — normally an engineer's artifact — is narrated in
// plain language.
func syscallLabel(name string) string {
	switch name {
	case "sys.input":
		return "📥 read the request"
	case "sys.output":
		return "📤 composing the answer"
	case "core.openaiApi":
		return "🧠 thinking"
	case "core.internet":
		return "🌐 querying the internet"
	case "core.memory":
		return "🗄️ consulting memory"
	case "sys.timer":
		return "⏳ waiting on a timer"
	case "sys.spawn":
		return "🧵 delegating to a sub-agent"
	case "sys.now", "sys.random":
		return "" // housekeeping syscalls, not worth narrating
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

// maxTimelineSteps caps how many recent steps a status message shows, so a long
// investigation does not grow an unbounded Slack message.
const maxTimelineSteps = 10

// buildTimeline folds a process's journal into a compact, human-readable list
// of steps: consecutive syscalls of the same kind collapse into one line with a
// count, and housekeeping syscalls are dropped. running reports whether the
// process itself is still going, which decides if the final step is shown as
// in-progress.
func buildTimeline(entries []JournalEntry, running bool) []step {
	var steps []step
	for _, e := range entries {
		label := syscallLabel(e.Syscall.Name)
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
// keeps updated in the thread as a process runs. header names the phase; the
// timeline narrates the syscalls.
func renderStatus(header string, entries []JournalEntry, running bool) string {
	var b strings.Builder
	b.WriteString(header)

	steps := buildTimeline(entries, running)
	if len(steps) == 0 {
		return b.String()
	}

	start := 0
	if len(steps) > maxTimelineSteps {
		start = len(steps) - maxTimelineSteps
		fmt.Fprintf(&b, "\n> …(%d earlier steps)", start)
	}
	for _, s := range steps[start:] {
		line := s.label
		if s.count > 1 {
			line = fmt.Sprintf("%s ×%d", line, s.count)
		}
		if s.active {
			line += " …"
		}
		b.WriteString("\n> ")
		b.WriteString(line)
	}
	return b.String()
}
