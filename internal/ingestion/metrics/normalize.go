package metrics

import "strings"

// attrAliases maps each canonical attribute key to the source keys that may
// carry the same logical dimension, in priority order (first non-empty wins).
//
// Different telemetry sources name the same dimension differently: the OTel
// kafkametrics receiver emits topic / group; otelsql emits db.system.name (and
// a malformed nested db.system object); stable semantic conventions use the
// canonical names below. Coalescing here — once, at ingest — lets the rollup
// MV read a single canonical key per dimension instead of branching per source.
//
// To support a new source, add its key to the relevant sources list. No SQL or
// schema change is required.
var attrAliases = []struct {
	canonical string
	sources   []string
}{
	{"db.system", []string{"db.system.name", "db.system"}},
	{"messaging.destination.name", []string{"messaging.destination.name", "topic"}},
	{"messaging.consumer.group.name", []string{"messaging.consumer.group.name", "group"}},
}

func normalizeAttrs(metricName string, attrs map[string]string) {
	for _, a := range attrAliases {
		for _, src := range a.sources {
			if v := attrs[src]; v != "" {
				attrs[a.canonical] = v
				break
			}
		}
	}

	if attrs["messaging.consumer.group.name"] == "" {
		if g := groupFromClientID(attrs["client-id"]); g != "" {
			attrs["messaging.consumer.group.name"] = g
		}
	}

	if attrs["messaging.system"] == "" && strings.HasPrefix(metricName, "kafka.") {
		attrs["messaging.system"] = "kafka"
	}
}

func groupFromClientID(clientID string) string {
	const prefix = "consumer-"
	rest, ok := strings.CutPrefix(clientID, prefix)
	if !ok {
		return ""
	}
	i := strings.LastIndexByte(rest, '-')
	if i <= 0 || !isAllDigits(rest[i+1:]) {
		return ""
	}
	return rest[:i]
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
