package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"
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
	redisKeyPrefix   = "optikk:otlp:team_by_api_key:"
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
}

func NewAuthenticator(finder TeamFinder) *Authenticator {
	a := &Authenticator{finder: finder}
	go a.startCleanup(5 * time.Minute)
	return a
}

func (a *Authenticator) ResolveTenantID(ctx context.Context, apiKey string) (int64, error) {
	if apiKey == "" {
		return 0, ErrMissingAPIKey
	}
	if entry, ok := a.lookupCache(apiKey); ok {
		return entry.tenantID, entry.err
	}
	id, err := a.finder.FindTenantIDByAPIKey(ctx, apiKey)
	if err != nil {
		// Only negative-cache genuine not-found; never cache transient or
		// context errors, which would lock out a tenant for the full TTL.
		if errors.Is(err, ErrInvalidAPIKey) {
			a.cacheSet(apiKey, 0, err)
		}
		return 0, err
	}
	a.cacheSet(apiKey, id, nil)
	return id, nil
}

func (a *Authenticator) lookupCache(apiKey string) (cacheEntry, bool) {
	val, ok := a.cache.Load(apiKeyCacheKey(apiKey))
	if !ok {
		return cacheEntry{}, false
	}
	entry := val.(cacheEntry)
	if time.Now().After(entry.expiresAt) {
		a.cache.Delete(apiKeyCacheKey(apiKey))
		return cacheEntry{}, false
	}
	return entry, true
}

func (a *Authenticator) cacheSet(apiKey string, tenantID int64, err error) {
	ttl := cacheTTL
	if err != nil {
		ttl = negativeCacheTTL
	}
	a.cache.Store(apiKeyCacheKey(apiKey), cacheEntry{
		tenantID:    tenantID,
		err:       err,
		expiresAt: time.Now().Add(ttl),
	})
}

func apiKeyCacheKey(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return redisKeyPrefix + hex.EncodeToString(h[:])
}

func (a *Authenticator) startCleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		now := time.Now()
		a.cache.Range(func(key, val any) bool {
			if entry, ok := val.(cacheEntry); ok && now.After(entry.expiresAt) {
				a.cache.Delete(key)
			}
			return true
		})
	}
}
