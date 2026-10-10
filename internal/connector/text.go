package connector

// Text helpers: mentions of the bot, and lines for a chat message.

import (
	"cmp"
	"regexp"
	"strings"
)

// mentionPattern matches a Slack user mention: "<@U123>" or the labelled form
// "<@U123|name>".
var mentionPattern = regexp.MustCompile(`<@[A-Z0-9]+(?:\|[^>]*)?>`)

// cleanText strips the bot's own mention — the bytes that reach aurora are the
// human's actual request. Other mentions stay: they name people. Horizontal
// whitespace within a line is collapsed, but line breaks are preserved so a
// pasted log or stack trace keeps its structure.
func (c *Connector) cleanText(text string) string {
	return normalizeInput(mentionPattern.ReplaceAllStringFunc(text, func(mention string) string {
		if c.isBotMention(mention) {
			return " "
		}
		return mention
	}))
}

// mentionsBot reports whether text contains a mention of this bot.
func (c *Connector) mentionsBot(text string) bool {
	for _, mention := range mentionPattern.FindAllString(text, -1) {
		if c.isBotMention(mention) {
			return true
		}
	}
	return false
}

// isBotMention reports whether one mention ("<@U123>" or the labelled
// "<@U123|name>") names this bot.
func (c *Connector) isBotMention(mention string) bool {
	return c.botUserID != "" &&
		(mention == "<@"+c.botUserID+">" || strings.HasPrefix(mention, "<@"+c.botUserID+"|"))
}

// normalizeInput collapses horizontal whitespace within each line but keeps the
// line breaks, then trims — so multi-line input reaches aurora intact.
func normalizeInput(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.Join(strings.Fields(line), " ")
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// truncate bounds a string to limit runes (not bytes, so it never splits a
// multi-byte rune), appending an ellipsis when it cuts.
func truncate(s string, limit int) string {
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit]) + "…"
}

// oneLine flattens every run of whitespace to one space and bounds the result,
// for an error or detail on a chat line.
func oneLine(s string) string {
	return cmp.Or(truncate(strings.Join(strings.Fields(s), " "), 500), "(no detail)")
}
