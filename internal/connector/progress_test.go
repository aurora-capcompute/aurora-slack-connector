package connector

import "testing"

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
	if steps[1].label != syscallLabel("core.openaiApi") || steps[1].count != 2 {
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

func TestRenderStatusIncludesHeaderAndTimeline(t *testing.T) {
	entries := []JournalEntry{
		entry("sys.input", "result"),
		entry("core.internet", "yield"),
	}
	got := renderStatus("HEAD", entries, true)
	if want := "HEAD"; got[:len(want)] != want {
		t.Fatalf("missing header: %q", got)
	}
	if !contains(got, "querying the internet") {
		t.Fatalf("missing internet step: %q", got)
	}
}

func TestRenderStatusCapsSteps(t *testing.T) {
	var entries []JournalEntry
	names := []string{"core.internet", "core.memory", "core.openaiApi"}
	for i := 0; i < 40; i++ {
		entries = append(entries, entry(names[i%len(names)], "result"))
	}
	got := renderStatus("H", entries, false)
	if !contains(got, "earlier steps") {
		t.Fatalf("expected a truncation notice for a long timeline: %q", got)
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
