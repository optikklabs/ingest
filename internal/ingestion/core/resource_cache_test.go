package core

import (
	"math/rand"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
)

// TestResourceCacheBucketSemantics: new key or changed bucket returns true;
// an unchanged key+bucket returns false (a hit).
func TestResourceCacheBucketSemantics(t *testing.T) {
	c := NewResourceCache("spans", 1024)
	key := ResourceKey{TenantID: 1, Fingerprint: 42}

	if !c.CheckAndUpdateBucket(key, 100) {
		t.Fatal("first insert = false, want true (new key)")
	}
	if c.CheckAndUpdateBucket(key, 100) {
		t.Error("same key+bucket = true, want false (hit)")
	}
	if !c.CheckAndUpdateBucket(key, 101) {
		t.Error("changed bucket = false, want true (re-emit)")
	}
}

// TestResourceCacheRemoveReEmits: removing a key makes the next check re-emit.
func TestResourceCacheRemoveReEmits(t *testing.T) {
	c := NewResourceCache("spans", 1024)
	key := ResourceKey{TenantID: 1, Fingerprint: 7}

	c.CheckAndUpdateBucket(key, 100)
	c.Remove(key)
	if !c.CheckAndUpdateBucket(key, 100) {
		t.Error("check after Remove = false, want true (re-emit)")
	}
}

// TestResourceCacheDistributesAcrossShards: random fingerprints should touch
// most shards, confirming the shard-select spreads load.
func TestResourceCacheDistributesAcrossShards(t *testing.T) {
	c := NewResourceCache("spans", resourceCacheShards*64)
	rng := rand.New(rand.NewSource(1))

	used := make(map[*lru.Cache[ResourceKey, uint32]]struct{})
	for i := 0; i < 10_000; i++ {
		key := ResourceKey{TenantID: 1, Fingerprint: rng.Uint64()}
		used[c.shard(key)] = struct{}{}
	}
	if len(used) != resourceCacheShards {
		t.Errorf("touched %d/%d shards, want all", len(used), resourceCacheShards)
	}
}

// BenchmarkResourceCacheParallel measures contention on the sharded hot path.
// Compare -mutexprofile before/after sharding to confirm the lock no longer
// dominates.
func BenchmarkResourceCacheParallel(b *testing.B) {
	c := NewResourceCache("spans", resourceCacheShards*1024)
	b.RunParallel(func(pb *testing.PB) {
		var fp uint64
		for pb.Next() {
			fp++
			c.CheckAndUpdateBucket(ResourceKey{TenantID: 1, Fingerprint: fp}, 1)
		}
	})
}

// TestResourceCacheConcurrent exercises the shards under concurrent access;
// run with -race to catch lock-striping bugs.
func TestResourceCacheConcurrent(t *testing.T) {
	c := NewResourceCache("spans", resourceCacheShards*128)
	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func(seed int64) {
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 5_000; i++ {
				key := ResourceKey{TenantID: 1, Fingerprint: rng.Uint64()}
				c.CheckAndUpdateBucket(key, uint32(i))
				if i%7 == 0 {
					c.Remove(key)
				}
			}
			done <- struct{}{}
		}(int64(g))
	}
	for g := 0; g < 8; g++ {
		<-done
	}
}
