package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func batch(n int) []*kgo.Record {
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{Offset: int64(i)}
	}
	return recs
}

// TestWorkerAndCommitter asserts the commit fires only on success.
func TestWorkerAndCommitter(t *testing.T) {
	c := &Consumer{signal: "test"}

	// 1. Success case
	job := batchJob{recs: batch(3), done: make(chan error, 1)}
	workerIn := make(chan batchJob, 1)
	committerIn := make(chan batchJob, 1)
	workerIn <- job
	committerIn <- job
	close(workerIn)
	close(committerIn)

	var commits int
	commit := func(context.Context, []*kgo.Record) error { commits++; return nil }
	handleOk := func(context.Context, []*kgo.Record) error { return nil }

	c.workerLoop(context.Background(), workerIn, handleOk)
	c.committerLoop(context.Background(), committerIn, commit)

	if commits != 1 {
		t.Errorf("commits = %d, want 1", commits)
	}

	// 2. Error case
	job2 := batchJob{recs: batch(3), done: make(chan error, 1)}
	workerIn2 := make(chan batchJob, 1)
	committerIn2 := make(chan batchJob, 1)
	workerIn2 <- job2
	committerIn2 <- job2
	close(workerIn2)
	close(committerIn2)

	commits = 0
	handleErr := func(context.Context, []*kgo.Record) error { return errors.New("err") }

	c.workerLoop(context.Background(), workerIn2, handleErr)
	c.committerLoop(context.Background(), committerIn2, commit)

	if commits != 0 {
		t.Errorf("commits = %d, want 0", commits)
	}
}

// TestParallelWorkers proves multiple workers can drain the channel concurrently.
func TestParallelWorkers(t *testing.T) {
	c := &Consumer{signal: "test", workers: 2}

	const numJobs = 10
	workerIn := make(chan batchJob, numJobs)
	committerIn := make(chan batchJob, numJobs)

	for i := 0; i < numJobs; i++ {
		job := batchJob{recs: batch(1), done: make(chan error, 1)}
		workerIn <- job
		committerIn <- job
	}
	close(workerIn)
	close(committerIn)

	var mu sync.Mutex
	var handled, committed int

	handle := func(context.Context, []*kgo.Record) error {
		mu.Lock()
		handled++
		mu.Unlock()
		return nil
	}
	commit := func(context.Context, []*kgo.Record) error {
		committed++
		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.workerLoop(context.Background(), workerIn, handle)
		}()
	}
	wg.Wait()

	c.committerLoop(context.Background(), committerIn, commit)

	if handled != numJobs || committed != numJobs {
		t.Errorf("handled=%d committed=%d, want %d each", handled, committed, numJobs)
	}
}
