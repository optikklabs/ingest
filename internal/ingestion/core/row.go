package core

import "google.golang.org/protobuf/proto"

// Row represents a generic telemetry row (e.g. spans, logs, metrics).
// It embeds proto.Message to allow marshaling/unmarshaling, and requires
// a GetTenantId() method to enable sticky Kafka partitioning.
type Row interface {
	proto.Message
	GetTenantId() uint32
}
