package core

import "sync"

type ResourceCache struct {
	mu      sync.Mutex
	seen    map[string]uint32
	queue   []string
	maxSize int
}

func NewResourceCache(maxSize int) *ResourceCache {
	return &ResourceCache{
		seen:    make(map[string]uint32),
		queue:   make([]string, 0, maxSize),
		maxSize: maxSize,
	}
}

// Add returns true if the key is newly added.
func (c *ResourceCache) Add(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.seen[key]; exists {
		return false
	}
	c.evictIfNeeded()
	c.seen[key] = 0
	c.queue = append(c.queue, key)
	return true
}

// CheckAndUpdateBucket returns true if the key is new OR the provided bucket is different 
// from the currently cached bucket.
func (c *ResourceCache) CheckAndUpdateBucket(key string, bucket uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if currentBucket, exists := c.seen[key]; exists {
		if currentBucket == bucket {
			return false
		}
		// Bucket changed, update it. No need to evict/re-queue since it's already there.
		c.seen[key] = bucket
		return true
	}
	
	c.evictIfNeeded()
	c.seen[key] = bucket
	c.queue = append(c.queue, key)
	return true
}

func (c *ResourceCache) evictIfNeeded() {
	if len(c.queue) >= c.maxSize {
		oldest := c.queue[0]
		c.queue = c.queue[1:]
		delete(c.seen, oldest)
	}
}
