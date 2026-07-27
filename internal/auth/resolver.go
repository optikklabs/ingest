package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
)

var (
	ErrMissingAPIKey   = errors.New("missing API key")
	ErrInvalidAPIKey   = errors.New("invalid API key")
	ErrAuthRateLimited = errors.New("authentication rate limited")
	ErrResolveFailed   = errors.New("failed to resolve team")
)

const (
	defaultCacheTTL  = 30 * time.Second
	defaultCacheSize = 50_000
	// Short so a key tried before its team exists recovers quickly.
	negativeCacheTTL = 15 * time.Second
	coldLookupRate   = 200
	coldLookupBurst  = 400
)

type cacheEntry struct {
	tenantID  int64
	err       error
	expiresAt time.Time
}

// TeamResolver turns an OTLP API key into the owning team id.
type TeamResolver interface {
	ResolveTenantID(ctx context.Context, apiKey string) (int64, error)
}

type TeamFinder interface {
	FindTenantIDByAPIKey(ctx context.Context, apiKey string) (int64, error)
}

type Authenticator struct {
	finder        TeamFinder
	cache         *lru.Cache[string, cacheEntry]
	group         singleflight.Group
	lookupLimiter *rate.Limiter
	ttl           time.Duration
}

func NewAuthenticator(finder TeamFinder, ttl time.Duration, cacheSize int) *Authenticator {
	return newAuthenticator(finder, ttl, cacheSize, rate.NewLimiter(coldLookupRate, coldLookupBurst))
}

func newAuthenticator(finder TeamFinder, ttl time.Duration, cacheSize int, lookupLimiter *rate.Limiter) *Authenticator {
	if ttl <= 0 {
		ttl = defaultCacheTTL
	}
	if cacheSize <= 0 {
		cacheSize = defaultCacheSize
	}
	cache, err := lru.New[string, cacheEntry](cacheSize)
	if err != nil {
		panic("auth cache: " + err.Error())
	}
	return &Authenticator{
		finder:        finder,
		cache:         cache,
		lookupLimiter: lookupLimiter,
		ttl:           ttl,
	}
}

func (a *Authenticator) ResolveTenantID(ctx context.Context, apiKey string) (int64, error) {
	if apiKey == "" {
		return 0, ErrMissingAPIKey
	}
	// Hash once per request; the digest keys the cache, singleflight, and store.
	cacheKey := apiKeyCacheKey(apiKey)
	if entry, ok := a.lookupCache(cacheKey); ok {
		return entry.tenantID, entry.err
	}
	// Collapse concurrent lookups for the same cold key into one DB call so a
	// traffic spike on an uncached key can't stampede the auth database.
	v, err, _ := a.group.Do(cacheKey, func() (any, error) {
		if entry, ok := a.lookupCache(cacheKey); ok {
			return entry.tenantID, entry.err
		}
		if a.lookupLimiter != nil && !a.lookupLimiter.Allow() {
			return int64(0), ErrAuthRateLimited
		}
		id, err := a.finder.FindTenantIDByAPIKey(ctx, apiKey)
		if err != nil {
			// Only negative-cache genuine not-found; never cache transient or
			// context errors, which would lock out a tenant for the full TTL.
			if errors.Is(err, ErrInvalidAPIKey) {
				a.cacheSet(cacheKey, 0, err)
			}
			return int64(0), err
		}
		a.cacheSet(cacheKey, id, nil)
		return id, nil
	})
	if err != nil {
		return 0, err
	}
	return v.(int64), nil
}

func (a *Authenticator) lookupCache(cacheKey string) (cacheEntry, bool) {
	entry, ok := a.cache.Get(cacheKey)
	if !ok {
		return cacheEntry{}, false
	}
	if time.Now().After(entry.expiresAt) {
		a.cache.Remove(cacheKey)
		return cacheEntry{}, false
	}
	return entry, true
}

func (a *Authenticator) cacheSet(cacheKey string, tenantID int64, err error) {
	ttl := a.ttl
	if ttl <= 0 {
		ttl = defaultCacheTTL
	}
	if err != nil {
		ttl = negativeCacheTTL
	}
	a.cache.Add(cacheKey, cacheEntry{
		tenantID:  tenantID,
		err:       err,
		expiresAt: time.Now().Add(ttl),
	})
}

func apiKeyCacheKey(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(h[:])
}
