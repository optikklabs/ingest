package core

import (
	lru "github.com/hashicorp/golang-lru/v2"
)

type ResourceKey struct {
	TenantID    uint32
	Fingerprint uint64
}

type ResourceCache struct {
	cache *lru.Cache[ResourceKey, uint32]
}

func NewResourceCache(maxSize int) *ResourceCache {
	c, err := lru.New[ResourceKey, uint32](maxSize)
	if err != nil {
		panic(err)
	}
	return &ResourceCache{
		cache: c,
	}
}

// Add returns true if the key is newly added.
func (c *ResourceCache) Add(key ResourceKey) bool {
	exists, _ := c.cache.ContainsOrAdd(key, 0)
	return !exists
}

// Remove evicts a key, e.g. to roll back an Add whose publish failed.
func (c *ResourceCache) Remove(key ResourceKey) {
	c.cache.Remove(key)
}

// CheckAndUpdateBucket returns true if the key is new OR the provided bucket is different
// from the currently cached bucket.
func (c *ResourceCache) CheckAndUpdateBucket(key ResourceKey, bucket uint32) bool {
	v, ok := c.cache.Peek(key)
	if ok && v == bucket {
		// Update recent-ness without changing value
		c.cache.Get(key)
		return false
	}
	c.cache.Add(key, bucket)
	return true
}
