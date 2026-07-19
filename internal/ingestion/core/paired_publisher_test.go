package core

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	spanschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"
)

type blockingPairPublisher struct {
	started chan struct{}
	release chan struct{}
	err     error
}

func (p *blockingPairPublisher) Publish(context.Context, []*spanschema.Row) error {
	close(p.started)
	<-p.release
	return p.err
}

func TestPublishMetricPairStartsBothPublishesBeforeWaiting(t *testing.T) {
	series := &blockingPairPublisher{started: make(chan struct{}), release: make(chan struct{})}
	metrics := &blockingPairPublisher{started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- PublishMetricPair(context.Background(), series, nil, metrics, nil)
	}()

	for name, started := range map[string]<-chan struct{}{
		"series":  series.started,
		"metrics": metrics.started,
	} {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("%s publish did not start concurrently", name)
		}
	}
	close(series.release)
	close(metrics.release)
	if err := <-done; err != nil {
		t.Fatalf("PublishMetricPair() error = %v", err)
	}
}

func TestPublishMetricPairIdentifiesFailedSide(t *testing.T) {
	series := &blockingPairPublisher{started: make(chan struct{}), release: make(chan struct{}), err: errors.New("unavailable")}
	metrics := &blockingPairPublisher{started: make(chan struct{}), release: make(chan struct{})}
	close(series.release)
	close(metrics.release)

	err := PublishMetricPair(context.Background(), series, nil, metrics, nil)
	if err == nil || !strings.Contains(err.Error(), "metric series publish") {
		t.Fatalf("PublishMetricPair() error = %v, want identified series error", err)
	}
}
