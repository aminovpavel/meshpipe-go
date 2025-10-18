package observability

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// LoggerOption configures logger creation.
type LoggerOption func(*loggerConfig)

type loggerConfig struct {
	level slog.Level
	json  bool
	out   io.Writer
}

// WithJSON toggles JSON output for the logger.
func WithJSON(json bool) LoggerOption {
	return func(cfg *loggerConfig) {
		cfg.json = json
	}
}

// WithWriter overrides the output writer; mainly useful for tests.
func WithWriter(w io.Writer) LoggerOption {
	return func(cfg *loggerConfig) {
		if w != nil {
			cfg.out = w
		}
	}
}

// WithLevel overrides the log level.
func WithLevel(level slog.Level) LoggerOption {
	return func(cfg *loggerConfig) {
		cfg.level = level
	}
}

// NewLogger constructs a slog.Logger with sane defaults and optional overrides.
func NewLogger(level string, opts ...LoggerOption) *slog.Logger {
	cfg := loggerConfig{
		level: parseLevel(level),
		out:   os.Stdout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	handlerOpts := &slog.HandlerOptions{Level: cfg.level}
	var handler slog.Handler
	if cfg.json {
		handler = slog.NewJSONHandler(cfg.out, handlerOpts)
	} else {
		handler = slog.NewTextHandler(cfg.out, handlerOpts)
	}
	return slog.New(handler)
}

// NoOpLogger provides a logger that discards all output.
func NoOpLogger() *slog.Logger {
	return NewLogger("ERROR", WithWriter(io.Discard))
}

func parseLevel(level string) slog.Level {
	switch strings.ToUpper(strings.TrimSpace(level)) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	case "INFO", "":
		fallthrough
	default:
		return slog.LevelInfo
	}
}
