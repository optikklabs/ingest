package metrics

import "testing"

func TestNormalizeAttrs(t *testing.T) {
	tests := []struct {
		name       string
		metricName string
		in         map[string]string
		want       map[string]string
	}{
		{
			name:       "kafkametrics receiver: topic/group coalesced, system synthesized",
			metricName: "kafka.consumer_group.lag",
			in:         map[string]string{"topic": "orders", "group": "checkout"},
			want: map[string]string{
				"topic":                         "orders",
				"group":                         "checkout",
				"messaging.destination.name":    "orders",
				"messaging.consumer.group.name": "checkout",
				"messaging.system":              "kafka",
			},
		},
		{
			name:       "otelsql: db.system.name wins over malformed db.system object",
			metricName: "db.sql.connection.open",
			in:         map[string]string{"db.system.name": "postgresql", "db.system": `{"name":"postgresql"}`},
			want: map[string]string{
				"db.system.name": "postgresql",
				"db.system":      "postgresql",
			},
		},
		{
			name:       "stable semconv: canonical keys pass through unchanged",
			metricName: "messaging.client.consumed.messages",
			in: map[string]string{
				"messaging.destination.name":    "orders",
				"messaging.consumer.group.name": "checkout",
				"messaging.system":              "kafka",
			},
			want: map[string]string{
				"messaging.destination.name":    "orders",
				"messaging.consumer.group.name": "checkout",
				"messaging.system":              "kafka",
			},
		},
		{
			name:       "non-kafka metric: system not synthesized",
			metricName: "system.cpu.utilization",
			in:         map[string]string{},
			want:       map[string]string{},
		},
		{
			name:       "JMX consumer: group recovered from client-id (group has a hyphen)",
			metricName: "kafka.consumer.commit_rate",
			in:         map[string]string{"client-id": "consumer-fraud-detection-1"},
			want: map[string]string{
				"client-id":                     "consumer-fraud-detection-1",
				"messaging.consumer.group.name": "fraud-detection",
				"messaging.system":              "kafka",
			},
		},
		{
			name:       "explicit group wins over client-id",
			metricName: "kafka.consumer.commit_rate",
			in:         map[string]string{"client-id": "consumer-fraud-detection-1", "group": "accounting"},
			want: map[string]string{
				"client-id":                     "consumer-fraud-detection-1",
				"group":                         "accounting",
				"messaging.consumer.group.name": "accounting",
				"messaging.system":              "kafka",
			},
		},
		{
			name:       "non-consumer client-id: group not derived",
			metricName: "kafka.producer.record_send_rate",
			in:         map[string]string{"client-id": "producer-1"},
			want: map[string]string{
				"client-id":        "producer-1",
				"messaging.system": "kafka",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalizeAttrs(tt.metricName, tt.in)
			if len(tt.in) != len(tt.want) {
				t.Fatalf("len = %d, want %d\ngot:  %v\nwant: %v", len(tt.in), len(tt.want), tt.in, tt.want)
			}
			for k, want := range tt.want {
				if got := tt.in[k]; got != want {
					t.Errorf("attrs[%q] = %q, want %q", k, got, want)
				}
			}
		})
	}
}
