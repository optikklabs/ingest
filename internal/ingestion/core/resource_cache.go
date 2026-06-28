package core

import "sync"

type ResourceCache struct {
	mu      sync.Mutex
	seen    map[string]struct{}
	queue   []string
	maxSize int
}

func NewResourceCache(maxSize int) *ResourceCache {
	return &ResourceCache{
		seen:    make(map[string]struct{}),
		queue:   make([]string, 0, maxSize),
		maxSize: maxSize,
	}
}

func (c *ResourceCache) Add(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.seen[key]; exists {
		return false
	}
	if len(c.queue) >= c.maxSize {
		oldest := c.queue[0]
		c.queue = c.queue[1:]
		delete(c.seen, oldest)
	}
	c.seen[key] = struct{}{}
	c.queue = append(c.queue, key)
	return true
}
