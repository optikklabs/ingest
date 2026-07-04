// Package authrepo is the minimal MySQL read access ingest needs to resolve
// OTLP API keys to teams. It satisfies auth.TeamFinder.
package authrepo

import (
	"context"
	"database/sql"
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

func (r *Repository) FindTenantIDByAPIKey(ctx context.Context, apiKey string) (int64, error) {
	var tenantID int64
	err := dbutil.GetSQL(ctx, r.db, "authrepo.FindTenantIDByAPIKey", &tenantID, `
		SELECT id FROM tenant WHERE api_key = ? AND active = 1 LIMIT 1
	`, apiKey)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, auth.ErrInvalidAPIKey
	}
	return tenantID, err
}
