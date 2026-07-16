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

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Validate rejects a config missing the credentials required to run safely.
// The service is always deployed in production, so these checks are
// unconditional. Each failing field is named so the startup log is actionable.
func (c Config) Validate() error {
	if c.MySQL.Password == "" {
		return errors.New("mysql.password must not be empty")
	}
	if c.ClickHouse.Password == "" {
		return errors.New("clickhouse.password must not be empty")
	}
	return nil
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
	v.SetDefault("clickhouse.max_open_conns", 0)
	v.SetDefault("clickhouse.max_idle_conns", 0)

	v.SetDefault("kafka.broker_list", "")
	v.SetDefault("kafka.topic_prefix", "")
	v.SetDefault("kafka.dlq_prefix", "")
	v.SetDefault("kafka.compression", "zstd")
	v.SetDefault("kafka.linger_ms", 20)
	v.SetDefault("kafka.batch_max_bytes", 1<<20)
	v.SetDefault("kafka.consumer_max_retries", 0)
	v.SetDefault("kafka.consumer_max_poll_records", 5000)

	v.SetDefault("otlp.grpc_port", "")
	v.SetDefault("otlp.http_port", "")
	v.SetDefault("otlp.grpc_max_concurrent_streams", 10000)
	v.SetDefault("otlp.grpc_max_recv_msg_size", 16*1024*1024)

	for _, signal := range []string{"spans", "spans_tracegraph", "spans_resource", "logs", "logs_resource", "metrics", "metric_series", "ingestion_stats"} {
		def := SignalDefaults(signal)
		prefix := "ingestion." + signal + "."
		v.SetDefault(prefix+"partitions", def.Partitions)
		v.SetDefault(prefix+"replicas", def.Replicas)
		v.SetDefault(prefix+"retention_hours", def.RetentionHours)
		v.SetDefault(prefix+"consumer_group", def.ConsumerGroup)
	}
	v.SetDefault("ingestion.side_publish.queue_size", 4096)
	v.SetDefault("ingestion.side_publish.workers", 2)
	v.SetDefault("ingestion.resource_cache_size", 500000)
	v.SetDefault("ingestion.spanmetrics.consumer_group", "optikk-ingest.spanaggregator.spanmetrics.consumer")

}
