package core

import "google.golang.org/protobuf/proto"

// Row represents a generic telemetry row (e.g. spans, logs, metrics).
// It embeds proto.Message to allow marshaling/unmarshaling, and requires
// a GetTeamId() method to enable sticky Kafka partitioning.
type Row interface {
	proto.Message
	GetTeamId() uint32
}
