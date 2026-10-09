package connector

import (
	"strings"
	"testing"
)

func entry(name, status string) JournalEntry {
	e := JournalEntry{}
	e.Syscall.Name = name
	e.Outcome.Status = status
	return e
}

func TestBuildTimelineCollapsesAndDropsHousekeeping(t *testing.T) {
	entries := []JournalEntry{
		entry("sys.input", "result"),
		entry("core.openaiApi", "result"),
		entry("core.openaiApi", "result"),
		entry("sys.now", "result"), // housekeeping, dropped
		entry("core.internet", "result"),
		entry("core.openaiApi", "yield"), // in flight
	}
	steps := buildTimeline(entries, true)
	if len(steps) != 4 {
		t.Fatalf("got %d steps, want 4: %+v", len(steps), steps)
	}
	if steps[1].label != syscallLabel("core.openaiApi", nil) || steps[1].count != 2 {
		t.Fatalf("thinking step not collapsed: %+v", steps[1])
	}
	// Only the final step, and only while running, may be active.
	if !steps[3].active {
		t.Fatalf("final in-flight step should be active: %+v", steps[3])
	}
	for i := 0; i < 3; i++ {
		if steps[i].active {
			t.Fatalf("non-final step %d should not be active", i)
		}
	}
}

func TestBuildTimelineNotRunningHasNoActive(t *testing.T) {
	entries := []JournalEntry{entry("core.openaiApi", "yield")}
	steps := buildTimeline(entries, false)
	if len(steps) != 1 || steps[0].active {
		t.Fatalf("finished process should show no active step: %+v", steps)
	}
}

// While a process runs, the status shows one icon per step and the latest step
// in words. When it has ended, the header counts the steps and the trail stays.
func TestRenderStatusShowsTheTrailAndTheLatestStep(t *testing.T) {
	entries := []JournalEntry{
		entry("sys.input", "result"),
		entry("core.openaiApi", "result"),
		entry("core.openaiApi", "result"),
		entry("core.internet", "yield"),
	}
	running := renderStatus("HEAD", entries, true)
	if want := "HEAD\n📥 🧠 🌐\n" + syscallLabel("core.internet", nil) + "…"; running != want {
		t.Fatalf("running status = %q, want %q", running, want)
	}
	entries[3].Outcome.Status = "result"
	done := renderStatus("DONE", entries, false)
	if want := "DONE · 4 steps\n📥 🧠 🌐"; done != want {
		t.Fatalf("done status = %q, want %q", done, want)
	}
}

func TestRenderStatusCapsTheTrail(t *testing.T) {
	var entries []JournalEntry
	names := []string{"core.internet", "core.scratch", "core.openaiApi"}
	for i := 0; i < 100; i++ {
		entries = append(entries, entry(names[i%len(names)], "result"))
	}
	got := renderStatus("H", entries, false)
	_, trail, _ := strings.Cut(got, "\n")
	if !strings.HasPrefix(trail, "… ") || len(strings.Fields(trail)) != maxTrailIcons+1 {
		t.Fatalf("trail = %q, want … and the last %d icons", trail, maxTrailIcons)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
