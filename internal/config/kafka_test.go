package config

import (
	"slices"
	"testing"
)

func TestKafkaBrokers(t *testing.T) {
	list := Config{Kafka: KafkaConfig{BrokerList: " a:9092, b:9092 ,,", Brokers: []string{"ignored"}}}
	if got := list.KafkaBrokers(); !slices.Equal(got, []string{"a:9092", "b:9092"}) {
		t.Fatalf("broker list = %q", got)
	}
	array := Config{Kafka: KafkaConfig{Brokers: []string{"c:9092"}}}
	if got := array.KafkaBrokers(); !slices.Equal(got, []string{"c:9092"}) {
		t.Fatalf("brokers = %q", got)
	}
}
