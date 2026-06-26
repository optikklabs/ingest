package database

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Migrator applies ClickHouse DDL migrations from FS to the database.
// Already-applied migrations are skipped using a schema tracking table.
type Migrator struct {
	DB       clickhouse.Conn
	FS       fs.FS
	Database string
	Logger   func(format string, args ...any)
}

const migrationTrackingTable = "schema_migrations"

func (m *Migrator) Up(ctx context.Context) (applied int, skipped int, err error) {
	if err := m.ensureTrackingTable(ctx); err != nil {
		return 0, 0, fmt.Errorf("chmigrate: ensure tracking table: %w", err)
	}
	files, err := m.listFiles()
	if err != nil {
		return 0, 0, err
	}
	appliedSet, err := m.appliedVersions(ctx)
	if err != nil {
		return 0, 0, err
	}
	for _, name := range files {
		if _, ok := appliedSet[name]; ok {
			skipped++
			m.logf("skip  %s (already applied)", name)
			continue
		}
		body, readErr := fs.ReadFile(m.FS, name)
		if readErr != nil {
			return applied, skipped, fmt.Errorf("chmigrate: read %s: %w", name, readErr)
		}
		for i, stmt := range splitMigrationStatements(string(body)) {
			if execErr := m.DB.Exec(ctx, stmt); execErr != nil {
				return applied, skipped, fmt.Errorf("chmigrate: %s: statement %d: %w\n--- SQL ---\n%s\n-----------", name, i+1, execErr, stmt)
			}
		}
		if insertErr := m.recordApplied(ctx, name); insertErr != nil {
			return applied, skipped, fmt.Errorf("chmigrate: record %s: %w", name, insertErr)
		}
		applied++
		m.logf("apply %s", name)
	}
	return applied, skipped, nil
}

func (m *Migrator) ensureTrackingTable(ctx context.Context) error {
	if m.Database == "" {
		m.Database = "optikk"
	}

	if err := m.DB.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+m.Database); err != nil {
		return err
	}
	return m.DB.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
		    version    String,
		    applied_at DateTime DEFAULT now()
		) ENGINE = MergeTree()
		ORDER BY version
	`, m.Database, migrationTrackingTable))
}

func (m *Migrator) listFiles() ([]string, error) {
	var names []string
	err := fs.WalkDir(m.FS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".sql") {
			names = append(names, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("chmigrate: walk: %w", err)
	}
	sort.Strings(names)
	return names, nil
}

func (m *Migrator) appliedVersions(ctx context.Context) (map[string]struct{}, error) {
	rows, err := m.DB.Query(ctx, fmt.Sprintf("SELECT version FROM %s.%s", m.Database, migrationTrackingTable))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	set := map[string]struct{}{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		set[v] = struct{}{}
	}
	return set, rows.Err()
}

func (m *Migrator) recordApplied(ctx context.Context, version string) error {
	return m.DB.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s.%s (version) VALUES (?)", m.Database, migrationTrackingTable),
		version,
	)
}

func (m *Migrator) logf(format string, args ...any) {
	if m.Logger == nil {
		return
	}
	m.Logger(format, args...)
}

func splitMigrationStatements(sql string) []string {
	var out []string
	var buf strings.Builder
	inSingle := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if !inSingle && c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if c == '\'' {
			if inSingle && i+1 < len(sql) && sql[i+1] == '\'' {
				buf.WriteByte(c)
				buf.WriteByte(sql[i+1])
				i++
				continue
			}
			inSingle = !inSingle
			buf.WriteByte(c)
			continue
		}
		if c == ';' && !inSingle {
			stmt := strings.TrimSpace(buf.String())
			if stmt != "" {
				out = append(out, stmt)
			}
			buf.Reset()
			continue
		}
		buf.WriteByte(c)
	}
	if tail := strings.TrimSpace(buf.String()); tail != "" {
		out = append(out, tail)
	}
	return out
}
