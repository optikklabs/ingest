package config

import "fmt"

type ClickHouseConfig struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Secure   bool   `yaml:"secure"`
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
