package config

// RateLimitConfig bounds per-tenant OTLP request rates for one replica.
type RateLimitConfig struct {
	TenantRPS   float64 `yaml:"tenant_rps"`
	TenantBurst int     `yaml:"tenant_burst"`
	MaxTenants  int     `yaml:"max_tenants"`
}

func (c Config) RateLimitTenantRPS() float64 { return c.RateLimit.TenantRPS }

func (c Config) RateLimitTenantBurst() int { return c.RateLimit.TenantBurst }

func (c Config) RateLimitMaxTenants() int { return c.RateLimit.MaxTenants }
