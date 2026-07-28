package config

import "fmt"

type ClickHouseConfig struct {
	Host         string `yaml:"host"`
	Port         string `yaml:"port"`
	Database     string `yaml:"database"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Secure       bool   `yaml:"secure"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
}

func (c Config) ClickHouseMaxOpenConns() int {
	if n := c.ClickHouse.MaxOpenConns; n > 0 {
		return n
	}
	return 5
}

func (c Config) ClickHouseMaxIdleConns() int {
	if n := c.ClickHouse.MaxIdleConns; n > 0 {
		return n
	}
	return 3
}

func (c Config) ClickHouseDSN() string {
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s",
		c.ClickHouse.User,
		c.ClickHouse.Password,
		c.ClickHouse.Host,
		c.ClickHouse.Port,
		c.ClickHouse.Database,
	)
	if c.ClickHouse.Secure {
		dsn += "?secure=true"
	}
	return dsn
}
