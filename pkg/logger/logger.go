package logger

import (
	"log/slog"
	"os"
)

type Config struct {
	AppName       string `envconfig:"APP_NAME"    required:"true"`
	AppVersion    string `envconfig:"APP_VERSION" required:"true"`
	Level         string `envconfig:"LOGGER_LEVEL" default:"error"`
	PrettyConsole bool   `envconfig:"LOGGER_PRETTY_CONSOLE" default:"false"`
}

func Init(c Config) {
	// Настраиваем level и форматирование
	var level slog.Level
	switch c.Level {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelError
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	}

	if c.PrettyConsole {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	logger := slog.New(handler).With(
		slog.String("app", c.AppName),
		slog.String("version", c.AppVersion),
	)

	slog.SetDefault(logger)
}
