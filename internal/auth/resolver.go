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
	cacheTTL       = 5 * time.Minute
	redisKeyPrefix = "optikk:otlp:team_by_api_key:"
)

type cacheEntry struct {
	teamID    int64
	err       error
	expiresAt time.Time
}

// TeamResolver turns an OTLP API key into the owning team id.
type TeamResolver interface {
	ResolveTeamID(ctx context.Context, apiKey string) (int64, error)
}

type TeamFinder interface {
	FindTeamIDByAPIKey(ctx context.Context, apiKey string) (int64, error)
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

func (a *Authenticator) ResolveTeamID(ctx context.Context, apiKey string) (int64, error) {
	if apiKey == "" {
		return 0, ErrMissingAPIKey
	}
	if entry, ok := a.lookupCache(apiKey); ok {
		return entry.teamID, entry.err
	}
	id, err := a.finder.FindTeamIDByAPIKey(ctx, apiKey)
	if err != nil {
		a.cacheSet(apiKey, 0, err)
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

func (a *Authenticator) cacheSet(apiKey string, teamID int64, err error) {
	a.cache.Store(apiKeyCacheKey(apiKey), cacheEntry{
		teamID:    teamID,
		err:       err,
		expiresAt: time.Now().Add(cacheTTL),
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
