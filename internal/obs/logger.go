package obs

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"time"
)

type Field map[string]any

type Logger struct {
	mu sync.Mutex
}

var std = &Logger{}

func L() *Logger {
	return std
}

func sanitizeValue(value string) string {
	if len(value) <= 8 {
		return "***REDACTED***"
	}
	return value[:4] + "***" + value[len(value)-4:]
}

func sanitizeField(key, value string) string {
	lowerKey := strings.ToLower(key)
	sensitivePatterns := []string{"token", "password", "secret", "key", "auth", "credential"}
	for _, pattern := range sensitivePatterns {
		if strings.Contains(lowerKey, pattern) {
			return sanitizeValue(value)
		}
	}
	return value
}

func (l *Logger) log(level, msg string, fields Field) {
	record := Field{
		"ts":    time.Now().UTC().Format(time.RFC3339Nano),
		"level": level,
		"msg":   msg,
	}

	for k, v := range fields {
		if str, ok := v.(string); ok {
			record[k] = sanitizeField(k, str)
		} else {
			record[k] = v
		}
	}

	b, err := json.Marshal(record)
	if err != nil {
		os.Stdout.Write([]byte(`{"level":"error","msg":"log marshal failed"}` + "\n"))
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	os.Stdout.Write(append(b, '\n'))
}

func Info(msg string, fields Field)  { std.log("info", msg, fields) }
func Warn(msg string, fields Field)  { std.log("warn", msg, fields) }
func Error(msg string, fields Field) { std.log("error", msg, fields) }

func Debug(msg string, fields Field) {
	if os.Getenv("LOG_LEVEL") == "debug" {
		std.log("debug", msg, fields)
	}
}
