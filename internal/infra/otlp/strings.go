package otlp

import "unicode/utf8"

// FirstNonEmpty returns the first non-empty value among keys, in order.
// Semantic-convention keys have been renamed across OTel versions, so readers
// list the current spelling alongside the legacy ones.
func FirstNonEmpty(attrs map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := attrs[k]; v != "" {
			return v
		}
	}
	return ""
}

// TruncateUTF8 caps s at maxBytes without splitting a rune. Rows are proto3
// messages, and proto.Marshal rejects string fields holding invalid UTF-8.
func TruncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	cut := maxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}
