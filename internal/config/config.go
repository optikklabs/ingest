package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

type Config struct {
	Environment string           `yaml:"environment"`
	Server      ServerConfig     `yaml:"server"`
	MySQL       MySQLConfig      `yaml:"mysql"`
	ClickHouse  ClickHouseConfig `yaml:"clickhouse"`
	Kafka       KafkaConfig      `yaml:"kafka"`
	OTLP        OTLPConfig       `yaml:"otlp"`
	Ingestion   IngestionConfig  `yaml:"ingestion"`
}

// Load reads YAML configuration with environment variable overrides.
// If no path is provided, it defaults to "config.yml".
func Load(path ...string) (Config, error) {
	p := "config.yml"
	if len(path) > 0 && path[0] != "" {
		p = path[0]
	}

	resolved, err := resolveConfigFilePath(p)
	if err != nil {
		return Config{}, err
	}

	v := viper.New()
	v.SetConfigFile(resolved)

	setDefaults(v)

	v.SetEnvPrefix("OPTIKK")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return Config{}, fmt.Errorf("cannot read config file %s: %w", resolved, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg, viper.DecoderConfigOption(func(dc *mapstructure.DecoderConfig) {
		dc.TagName = "yaml"
	})); err != nil {
		return Config{}, fmt.Errorf("invalid config in %s: %w", resolved, err)
	}

	return cfg, nil
}

func resolveConfigFilePath(p string) (string, error) {
	if filepath.IsAbs(p) {
		if _, err := os.Stat(p); err != nil {
			return "", fmt.Errorf("config file %q: %w", p, err)
		}
		return p, nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	for dir := wd; ; {
		candidate := filepath.Join(dir, p)
		if st, statErr := os.Stat(candidate); statErr == nil && !st.IsDir() {
			return candidate, nil
		} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return "", fmt.Errorf("config file %q: %w", candidate, statErr)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("config file %q not found (searched from %s upward)", p, wd)
		}
		dir = parent
	}
}

func setDefaults(v *viper.Viper) {

	v.SetDefault("environment", "")

	v.SetDefault("server.port", "")
	v.SetDefault("server.allowed_origins", "")
	v.SetDefault("server.debug_api_logs", false)

	v.SetDefault("mysql.host", "")
	v.SetDefault("mysql.port", "")
	v.SetDefault("mysql.database", "")
	v.SetDefault("mysql.user", "")
	v.SetDefault("mysql.password", "")
	v.SetDefault("mysql.max_open_conns", 0)
	v.SetDefault("mysql.max_idle_conns", 0)

	v.SetDefault("clickhouse.host", "")
	v.SetDefault("clickhouse.port", "")
	v.SetDefault("clickhouse.database", "")
	v.SetDefault("clickhouse.user", "")
	v.SetDefault("clickhouse.password", "")
	v.SetDefault("clickhouse.production", false)
	v.SetDefault("clickhouse.cloud_host", "")

	v.SetDefault("kafka.broker_list", "")
	v.SetDefault("kafka.consumer_group", "")
	v.SetDefault("kafka.topic_prefix", "")

	v.SetDefault("otlp.grpc_port", "")
	v.SetDefault("otlp.grpc_max_concurrent_streams", 10000)

	v.SetDefault("ingestion.spans_bucket_seconds", 0)
	v.SetDefault("ingestion.logs_bucket_seconds", 0)

}
