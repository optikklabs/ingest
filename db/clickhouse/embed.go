// Package clickhouse exposes the embedded DDL filesystem so the Migrator
// in internal/infra/database can apply schema during server boot.
package clickhouse

import "embed"

// FS holds the schema files applied in lexical order by the migrator.
//
//go:embed *.sql
var FS embed.FS
