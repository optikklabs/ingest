package database

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

// MySQLMigrator runs idempotent DDL migrations from FS on server boot in
// lexical order.
type MySQLMigrator struct {
	DB     *sql.DB
	FS     fs.FS
	Logger func(format string, args ...any)
}

// Up applies all SQL migrations in lexical order.
// It returns the statement count and any error.
func (m *MySQLMigrator) Up(ctx context.Context) (stmts int, err error) {
	files, err := m.listFiles()
	if err != nil {
		return 0, err
	}
	for _, name := range files {
		body, readErr := fs.ReadFile(m.FS, name)
		if readErr != nil {
			return stmts, fmt.Errorf("mysqlmigrate: read %s: %w", name, readErr)
		}
		for i, stmt := range splitMigrationStatements(string(body)) {
			if _, execErr := m.DB.ExecContext(ctx, stmt); execErr != nil {
				return stmts, fmt.Errorf("mysqlmigrate: %s: statement %d: %w\n--- SQL ---\n%s\n-----------", name, i+1, execErr, stmt)
			}
			stmts++
		}
		m.logf("apply %s", name)
	}
	return stmts, nil
}

func (m *MySQLMigrator) listFiles() ([]string, error) {
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
		return nil, fmt.Errorf("mysqlmigrate: walk: %w", err)
	}
	sort.Strings(names)
	return names, nil
}

func (m *MySQLMigrator) logf(format string, args ...any) {
	if m.Logger == nil {
		return
	}
	m.Logger(format, args...)
}
