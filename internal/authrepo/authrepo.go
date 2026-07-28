// Package authrepo reads the `tenant` table owned by the QUERY repository.
//
// CROSS-REPO CONTRACT — query repo, db/01_tenant.sql:
//   - table `tenant` must expose columns id, api_key_hash, active
//   - api_key_hash stores hex(sha256(api_key)), lowercase (query writes it,
//     ingest re-derives it here in hashAPIKey; see TestAPIKeyHashContract)
//   - active = 1 gates ingestion; trial suspension flips it to 0
//
// Any query-side change to this schema or hashing convention silently kills
// ALL ingestion auth. ProbeSchema fails readiness if the shape drifts.
package authrepo

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

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

// hashAPIKey mirrors query's provisioning convention: hex(sha256(key)).
func hashAPIKey(apiKey string) string {
	sum := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(sum[:])
}

func (r *Repository) FindTenantIDByAPIKey(ctx context.Context, apiKey string) (int64, error) {
	var tenantID int64
	err := dbutil.GetSQL(ctx, r.db, "authrepo.FindTenantIDByAPIKey", &tenantID, `
		SELECT id FROM tenant WHERE api_key_hash = ? AND active = 1 LIMIT 1
	`, hashAPIKey(apiKey))
	if errors.Is(err, sql.ErrNoRows) {
		return 0, auth.ErrInvalidAPIKey
	}
	return tenantID, err
}

// ProbeSchema cheaply verifies the tenant-table contract (table plus the
// id / api_key_hash / active columns) so a query-side schema change fails
// readiness with a clear error instead of rejecting every ingest request.
func (r *Repository) ProbeSchema(ctx context.Context) error {
	var n int64
	err := dbutil.GetSQL(ctx, r.db, "authrepo.ProbeSchema", &n, `
		SELECT COUNT(*) FROM tenant WHERE api_key_hash = '' AND active = 1
	`)
	if err != nil {
		return fmt.Errorf("tenant table contract check (see authrepo package doc; owned by query db/01_tenant.sql): %w", err)
	}
	return nil
}
