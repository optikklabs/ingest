package auth

import (
	"golang.org/x/time/rate"

	lru "github.com/hashicorp/golang-lru/v2"
)

var RateLimiter *TenantRateLimiter

func init() {
	var err error
	RateLimiter, err = NewTenantRateLimiter(1000, 2000, 10000)
	if err != nil {
		panic(err)
	}
}

type TenantRateLimiter struct {
	cache *lru.Cache[int64, *rate.Limiter]
	limit rate.Limit
	burst int
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
		return true // skip limiting if no tenant
	}
	
	limiter, ok := trl.cache.Get(tenantID)
	if !ok {
		limiter = rate.NewLimiter(trl.limit, trl.burst)
		trl.cache.Add(tenantID, limiter)
	}

	return limiter.Allow()
}
