// Package authrepo is the minimal MySQL read access ingest needs to resolve
// OTLP API keys to teams. It satisfies auth.TeamFinder.
package authrepo

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"

	dbutil "github.com/optikklabs/ingest/internal/infra/database"
)

type Repository struct {
	db *sqlx.DB
}

func New(db *sql.DB) *Repository {
	return &Repository{db: sqlx.NewDb(db, "mysql")}
}

func (r *Repository) FindTeamIDByAPIKey(ctx context.Context, apiKey string) (int64, error) {
	var teamID int64
	err := dbutil.GetSQL(ctx, r.db, "authrepo.FindTeamIDByAPIKey", &teamID, `
		SELECT id FROM teams WHERE api_key = ? AND active = 1 LIMIT 1
	`, apiKey)
	return teamID, err
}
