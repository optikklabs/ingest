package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

var (
	ErrMissingAPIKey = errors.New("missing API key")
	ErrInvalidAPIKey = errors.New("invalid API key")
	ErrResolveFailed = errors.New("failed to resolve team")
)

const (
	cacheTTL = 5 * time.Minute
	// Short so a key tried before its team exists recovers quickly.
	negativeCacheTTL = 15 * time.Second
)

type cacheEntry struct {
	tenantID    int64
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
	finder TeamFinder
	cache  sync.Map
	group  singleflight.Group
}

func NewAuthenticator(ctx context.Context, finder TeamFinder) *Authenticator {
	a := &Authenticator{finder: finder}
	go a.startCleanup(ctx, 5*time.Minute)
	return a
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
	val, ok := a.cache.Load(cacheKey)
	if !ok {
		return cacheEntry{}, false
	}
	entry := val.(cacheEntry)
	if time.Now().After(entry.expiresAt) {
		a.cache.Delete(cacheKey)
		return cacheEntry{}, false
	}
	return entry, true
}

func (a *Authenticator) cacheSet(cacheKey string, tenantID int64, err error) {
	ttl := cacheTTL
	if err != nil {
		ttl = negativeCacheTTL
	}
	a.cache.Store(cacheKey, cacheEntry{
		tenantID:  tenantID,
		err:       err,
		expiresAt: time.Now().Add(ttl),
	})
}

func apiKeyCacheKey(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(h[:])
}

func (a *Authenticator) startCleanup(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			a.cache.Range(func(key, val any) bool {
				if entry, ok := val.(cacheEntry); ok && now.After(entry.expiresAt) {
					a.cache.Delete(key)
				}
				return true
			})
		}
	}
}
