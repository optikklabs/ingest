package database

import (
	"context"
	"database/sql"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Open connects to MySQL with cfg and verifies the connection.
func Open(cfg *mysql.Config, maxOpen, maxIdle int) (*sql.DB, error) {
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	if maxOpen <= 0 {
		maxOpen = 50
	}
	if maxIdle <= 0 {
		maxIdle = maxOpen / 2
	}
	db.SetConnMaxLifetime(15 * time.Minute)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	pingCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
