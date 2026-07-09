package connector

import (
	"io"
	"log/slog"
)

// discardLogger is a no-op logger for tests.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
