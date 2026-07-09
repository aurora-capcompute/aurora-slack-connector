package connector

import (
	"strings"
	"testing"
)

func TestNormalizeInputPreservesLineBreaks(t *testing.T) {
	in := "  first  line \n\n   second   line   \n"
	got := normalizeInput(in)
	want := "first line\n\nsecond line"
	if got != want {
		t.Fatalf("normalizeInput = %q, want %q", got, want)
	}
}

func TestCleanTextKeepsMultilineBody(t *testing.T) {
	conn := New(Config{TriggerKeyword: "@duty"}, nil, nil, discardLogger())
	in := "<@UBOT> @duty here is the trace:\n  at foo()\n  at bar()"
	got := conn.cleanText(in)
	want := "here is the trace:\nat foo()\nat bar()"
	if got != want {
		t.Fatalf("cleanText = %q, want %q", got, want)
	}
}

func TestCleanTextStripsLabelledMention(t *testing.T) {
	conn := New(Config{}, nil, nil, discardLogger())
	if got := conn.cleanText("<@U123|dutybot> status?"); got != "status?" {
		t.Fatalf("labelled mention not stripped: %q", got)
	}
}

func TestTruncateIsRuneSafe(t *testing.T) {
	// Ten multi-byte runes; truncating to 4 must not split a rune.
	s := strings.Repeat("é", 10)
	got := truncate(s, 4)
	if got != "éééé…" {
		t.Fatalf("truncate = %q", got)
	}
	if truncate("short", 100) != "short" {
		t.Fatal("truncate shortened a string under the limit")
	}
}
