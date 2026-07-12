package kafka

import "testing"

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
