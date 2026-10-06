package config

import (
	"slices"
	"testing"
)

func TestKafkaBrokers(t *testing.T) {
	list := Config{Kafka: KafkaConfig{BrokerList: " a:9092, b:9092 ,,"}}
	if got := list.KafkaBrokers(); !slices.Equal(got, []string{"a:9092", "b:9092"}) {
		t.Fatalf("broker list = %q", got)
	}
}
