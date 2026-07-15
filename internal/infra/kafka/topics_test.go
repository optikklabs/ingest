package kafka

import (
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestDecidePartitionAction(t *testing.T) {
	tests := []struct {
		name    string
		current int32
		target  int32
		want    partitionAction
	}{
		{"below target grows", 8, 32, partitionGrow},
		{"at target no-ops", 32, 32, partitionNoop},
		{"above target skips shrink", 32, 8, partitionShrinkSkip},
		{"zero current grows", 0, 1, partitionGrow},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decidePartitionAction(tt.current, tt.target); got != tt.want {
				t.Errorf("decidePartitionAction(%d, %d) = %d, want %d", tt.current, tt.target, got, tt.want)
			}
		})
	}
}

func TestPartitionsRespError(t *testing.T) {
	const topic = "optikk.ingest.spans_resource"

	tests := []struct {
		name string
		resp kadm.CreatePartitionsResponses
		want string // "" means no error
	}{
		{
			name: "success reports no error",
			resp: kadm.CreatePartitionsResponses{topic: {Topic: topic}},
			want: "",
		},
		{
			name: "missing topic reports no error",
			resp: kadm.CreatePartitionsResponses{},
			want: "",
		},
		{
			name: "broker message is surfaced alongside the code",
			resp: kadm.CreatePartitionsResponses{topic: {
				Topic:      topic,
				Err:        kerr.InvalidPartitions,
				ErrMessage: "topic already has 8 partitions",
			}},
			want: "topic already has 8 partitions",
		},
		{
			name: "bare code when broker sends no message",
			resp: kadm.CreatePartitionsResponses{topic: {Topic: topic, Err: kerr.InvalidPartitions}},
			want: "INVALID_PARTITIONS",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := partitionsRespError(tt.resp, topic)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("want nil, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want error containing %q, got nil", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not contain %q", err, tt.want)
			}
		})
	}
}
