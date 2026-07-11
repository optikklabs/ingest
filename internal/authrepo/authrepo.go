// Package authrepo is the minimal MySQL read access ingest needs to resolve
// OTLP API keys to teams. It satisfies auth.TeamFinder.
package authrepo

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"

	"github.com/jmoiron/sqlx"

	"github.com/optikklabs/ingest/internal/auth"
	dbutil "github.com/optikklabs/ingest/internal/infra/database"
)

type Repository struct {
	db *sqlx.DB
}

func New(db *sql.DB) *Repository {
	return &Repository{db: sqlx.NewDb(db, "mysql")}
}

// FindTenantIDByAPIKey resolves the raw key from the OTLP request to its
// tenant by hex SHA-256 — mirrors query's shared.HashAPIKey, which owns the
// stored form (tenant.api_key_hash).
func (r *Repository) FindTenantIDByAPIKey(ctx context.Context, apiKey string) (int64, error) {
	sum := sha256.Sum256([]byte(apiKey))
	var tenantID int64
	err := dbutil.GetSQL(ctx, r.db, "authrepo.FindTenantIDByAPIKey", &tenantID, `
		SELECT id FROM tenant WHERE api_key_hash = ? AND active = 1 LIMIT 1
	`, hex.EncodeToString(sum[:]))
	if errors.Is(err, sql.ErrNoRows) {
		return 0, auth.ErrInvalidAPIKey
	}
	return tenantID, err
}
