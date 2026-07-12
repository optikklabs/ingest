package core

import (
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/optikklabs/ingest/internal/infra/metrics"
)

// resourceCacheShards stripes the cache lock N ways so the span/log hot path
// does not serialize on a single mutex. Power of two: shard = fp & (N-1).
const resourceCacheShards = 32

type ResourceKey struct {
	TenantID    uint32
	Fingerprint uint64
}

// ResourceCache is a sharded LRU keyed by resource fingerprint. golang-lru
// locks each shard independently, so the shards give N-way lock striping.
type ResourceCache struct {
	signal string
	shards [resourceCacheShards]*lru.Cache[ResourceKey, uint32]
}

// NewResourceCache builds the sharded LRU. maxSize is split evenly across
// shards; signal labels the hit/miss/eviction metrics for this instance.
func NewResourceCache(signal string, maxSize int) *ResourceCache {
	perShard := maxSize / resourceCacheShards
	if perShard < 1 {
		perShard = 1
	}
	onEvict := func(ResourceKey, uint32) {
		metrics.ResourceCacheEvictions.WithLabelValues(signal).Inc()
	}
	c := &ResourceCache{signal: signal}
	for i := range c.shards {
		s, err := lru.NewWithEvict[ResourceKey, uint32](perShard, onEvict)
		if err != nil {
			panic(err)
		}
		c.shards[i] = s
	}
	return c
}

// shard selects the shard for a key. Fingerprint is a well-distributed xxhash,
// so its low bits are uniform.
func (c *ResourceCache) shard(key ResourceKey) *lru.Cache[ResourceKey, uint32] {
	return c.shards[key.Fingerprint&(resourceCacheShards-1)]
}

// Remove evicts a key, e.g. to roll back a resource whose publish failed.
func (c *ResourceCache) Remove(key ResourceKey) {
	c.shard(key).Remove(key)
}

// CheckAndUpdateBucket returns true if the key is new OR the provided bucket is different
// from the currently cached bucket.
func (c *ResourceCache) CheckAndUpdateBucket(key ResourceKey, bucket uint32) bool {
	s := c.shard(key)
	v, ok := s.Peek(key)
	if ok && v == bucket {
		// Update recent-ness without changing value
		s.Get(key)
		metrics.ResourceCacheHits.WithLabelValues(c.signal).Inc()
		return false
	}
	s.Add(key, bucket)
	metrics.ResourceCacheMisses.WithLabelValues(c.signal).Inc()
	return true
}
