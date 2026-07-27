package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// baseConfig writes the minimum config Load accepts.
func baseConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yml")
	body := "mysql:\n  password: mysql\nclickhouse:\n  password: clickhouse\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestResourceCacheSizeEnvironmentOverride(t *testing.T) {
	t.Setenv("OPTIKK_INGESTION_RESOURCE_CACHE_SIZE", "1234")

	cfg, err := Load(baseConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.ResourceCacheSize(); got != 1234 {
		t.Errorf("ResourceCacheSize = %d, want environment override 1234", got)
	}
}

func TestResourceCacheSizeDefault(t *testing.T) {
	cfg, err := Load(baseConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.ResourceCacheSize(); got != 500_000 {
		t.Errorf("ResourceCacheSize = %d, want default 500000", got)
	}
}

func TestAPIKeyCacheTTLEnvironmentOverride(t *testing.T) {
	t.Setenv("OPTIKK_INGESTION_API_KEY_CACHE_TTL_SECONDS", "12")

	cfg, err := Load(baseConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.APIKeyCacheTTL(); got != 12*time.Second {
		t.Errorf("APIKeyCacheTTL = %v, want 12s", got)
	}
}

func TestAPIKeyCacheSizeEnvironmentOverride(t *testing.T) {
	t.Setenv("OPTIKK_INGESTION_API_KEY_CACHE_SIZE", "1234")

	cfg, err := Load(baseConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.APIKeyCacheSize(); got != 1234 {
		t.Errorf("APIKeyCacheSize = %d, want 1234", got)
	}
}
