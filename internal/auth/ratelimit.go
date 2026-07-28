package auth

import (
	"sync"

	"golang.org/x/time/rate"

	lru "github.com/hashicorp/golang-lru/v2"
)

// TenantRateLimiter enforces per-tenant request limits within one replica:
// state is in-process, so a tenant's effective cluster-wide rate is
// limit x replica count. Limits come from the ratelimit.* config keys.
type TenantRateLimiter struct {
	cache *lru.Cache[int64, *rate.Limiter]
	limit rate.Limit
	burst int
	mu    sync.Mutex
}

func NewTenantRateLimiter(limit float64, burst int, maxTenants int) (*TenantRateLimiter, error) {
	cache, err := lru.New[int64, *rate.Limiter](maxTenants)
	if err != nil {
		return nil, err
	}
	return &TenantRateLimiter{
		cache: cache,
		limit: rate.Limit(limit),
		burst: burst,
	}, nil
}

func (trl *TenantRateLimiter) Allow(tenantID int64) bool {
	if tenantID == 0 {
		return true
	}

	limiter, ok := trl.cache.Get(tenantID)
	if !ok {
		trl.mu.Lock()
		limiter, ok = trl.cache.Get(tenantID)
		if !ok {
			limiter = rate.NewLimiter(trl.limit, trl.burst)
			trl.cache.Add(tenantID, limiter)
		}
		trl.mu.Unlock()
	}

	return limiter.Allow()
}
