package database

import (
	"context"
	"database/sql"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// injectMySQLTimeouts adds DSN timeouts and parseTime=true (required to scan
// MySQL datetime columns into time.Time fields) if not already present.
func injectMySQLTimeouts(dsn string) string {
	params := map[string]string{
		"timeout":      "5s",
		"readTimeout":  "30s",
		"writeTimeout": "30s",
		"parseTime":    "true",
	}
	for param, val := range params {
		if !strings.Contains(dsn, param+"=") {
			sep := "&"
			if !strings.Contains(dsn, "?") {
				sep = "?"
			}
			dsn = dsn + sep + param + "=" + val
		}
	}
	return dsn
}

func Open(dsn string, maxOpen, maxIdle int) (*sql.DB, error) {
	dsn = injectMySQLTimeouts(dsn)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	if maxOpen <= 0 {
		maxOpen = 50
	}
	if maxIdle <= 0 {
		maxIdle = maxOpen / 2
	}

	db.SetConnMaxLifetime(15 * time.Minute)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pingCancel()
	if err := db.PingContext(pingCtx); err != nil {
		return nil, err
	}

	return db, nil
}
