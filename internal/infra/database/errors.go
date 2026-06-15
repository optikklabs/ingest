package database

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// chErrorMessages maps ClickHouse codes to human-readable messages.
var chErrorMessages = map[int32]string{
	60:  "Database table not found",
	16:  "Column not found in table",
	62:  "Query syntax error",
	241: "Query exceeded memory limit",
	159: "Query timed out",
	210: "Database network error",
	47:  "Unknown identifier in query",
	8:   "Column not found in table",
	53:  "Data type mismatch",
	70:  "Cannot convert data type",
	43:  "Illegal argument type",
	158: "Too many rows returned",
	161: "Too many columns in result",
}

// networkErrorPatterns maps Go network error substrings to friendly messages.
var networkErrorPatterns = []struct {
	substr  string
	message string
}{
	{"connection refused", "Database connection refused"},
	{"connection reset", "Database connection reset"},
	{"broken pipe", "Database connection lost"},
	{"i/o timeout", "Database connection timed out"},
	{"no such host", "Database host not found"},
	{"timeout exceeded", "Database query timed out"},
	{"connection closed", "Database connection closed"},
	{"unexpected eof", "Database connection interrupted"},
	{"context deadline exceeded", "Request timed out"},
	{"context canceled", "Request was canceled"},
}

// sqlStripPattern matches SQL keywords and everything after them.
var sqlStripPattern = regexp.MustCompile(`(?i)\s+(SELECT|INSERT|UPDATE|DELETE|FROM|WHERE|IN SCOPE)\s+.*`)

// SanitizeError converts a raw error into a clean, human-readable message.
// It strips SQL queries, stack traces, and internal identifiers.
func SanitizeError(err error) string {
	if err == nil {
		return ""
	}

	// ClickHouse structured exception — use code mapping or Name field.
	var chErr *clickhouse.Exception
	if errors.As(err, &chErr) {
		if friendly, ok := chErrorMessages[chErr.Code]; ok {
			return friendly
		}
		if chErr.Name != "" {
			return "Database error: " + chErr.Name
		}
		return fmt.Sprintf("Database error (code %d)", chErr.Code)
	}

	msg := err.Error()

	// Check for known network error patterns.
	lower := strings.ToLower(msg)
	for _, p := range networkErrorPatterns {
		if strings.Contains(lower, p.substr) {
			return p.message
		}
	}

	// Fallback: strip SQL from the message and truncate.
	return sanitizeRawMessage(msg)
}

// sanitizeRawMessage strips SQL queries and truncates to a safe length.
func sanitizeRawMessage(msg string) string {
	msg = sqlStripPattern.ReplaceAllString(msg, "")
	msg = strings.TrimSpace(msg)
	if len(msg) > 200 {
		msg = msg[:200] + "…"
	}
	if msg == "" {
		return "An unexpected error occurred"
	}
	return msg
}
