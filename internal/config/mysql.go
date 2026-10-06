package config

import (
	"net"
	"time"

	"github.com/go-sql-driver/mysql"
)

type MySQLConfig struct {
	Host         string `yaml:"host"`
	Port         string `yaml:"port"`
	Database     string `yaml:"database"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
}

// MySQLDriverConfig builds the driver connection settings. Building them as
// a struct keeps credentials with special characters intact, which a
// formatted DSN would not.
func (c Config) MySQLDriverConfig() (*mysql.Config, error) {
	cfg := mysql.NewConfig()
	cfg.User = c.MySQL.User
	cfg.Passwd = c.MySQL.Password
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(c.MySQL.Host, c.MySQL.Port)
	cfg.DBName = c.MySQL.Database
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 30 * time.Second
	cfg.WriteTimeout = 30 * time.Second
	if err := cfg.Apply(mysql.Charset("utf8mb4", "")); err != nil {
		return nil, err
	}
	return cfg, nil
}
