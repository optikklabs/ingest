package core

import (
	"context"
	"errors"
	"testing"

	spansresourceschema "github.com/optikklabs/ingest/internal/ingestion/spansresource/schema"
)

type fakeResourcePublisher struct {
	calls int
	err   error
}

func (f *fakeResourcePublisher) Publish(ctx context.Context, rows []*spansresourceschema.ResourceRow) error {
	f.calls++
	return f.err
}

func TestPublishResourcesRollsBackCacheOnFailure(t *testing.T) {
	cache := NewResourceCache(10)
	keys := []ResourceKey{
		{TenantID: 1, Fingerprint: 100},
		{TenantID: 1, Fingerprint: 200},
	}
	rows := make([]*spansresourceschema.ResourceRow, len(keys))
	for i, k := range keys {
		if !cache.Add(k) {
			t.Fatalf("Add(%v) = false, want true on empty cache", k)
		}
		rows[i] = &spansresourceschema.ResourceRow{TenantId: k.TenantID, Fingerprint: k.Fingerprint}
	}

	pub := &fakeResourcePublisher{err: errors.New("kafka down")}
	PublishResources(pub, cache, keys, rows, "spans")

	for _, k := range keys {
		if !cache.Add(k) {
			t.Errorf("key %v still cached after failed publish, want evicted", k)
		}
	}
}

func TestPublishResourcesKeepsCacheOnSuccess(t *testing.T) {
	cache := NewResourceCache(10)
	key := ResourceKey{TenantID: 1, Fingerprint: 100}
	cache.Add(key)
	rows := []*spansresourceschema.ResourceRow{{TenantId: 1, Fingerprint: 100}}

	pub := &fakeResourcePublisher{}
	PublishResources(pub, cache, []ResourceKey{key}, rows, "spans")

	if pub.calls != 1 {
		t.Fatalf("publish calls = %d, want 1", pub.calls)
	}
	if cache.Add(key) {
		t.Error("key evicted after successful publish, want cached")
	}
}

func TestPublishResourcesSkipsEmptyBatch(t *testing.T) {
	pub := &fakeResourcePublisher{}
	PublishResources(pub, NewResourceCache(10), nil, nil, "spans")
	if pub.calls != 0 {
		t.Errorf("publish calls = %d, want 0 for empty batch", pub.calls)
	}
}
