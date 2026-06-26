// Package clickhouse exposes the embedded DDL filesystem so the Migrator
// in internal/infra/database can apply schema during server boot.
package clickhouse

import "embed"

//go:embed *.sql
var FS embed.FS
