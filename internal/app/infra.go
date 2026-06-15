package app

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	chembed "github.com/optikklabs/ingest/db/clickhouse"
	"github.com/optikklabs/ingest/internal/app/registry"
	"github.com/optikklabs/ingest/internal/auth"
	"github.com/optikklabs/ingest/internal/authrepo"
	"github.com/optikklabs/ingest/internal/config"
	dbutil "github.com/optikklabs/ingest/internal/infra/database"
	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
)

// ConsumerRunner is a Kafka consumer loop that blocks until ctx is cancelled.
type ConsumerRunner interface {
	Run(ctx context.Context)
}

// Infra holds process-wide infrastructure constructed at startup.
type Infra struct {
	DB            *sql.DB
	CH            clickhouse.Conn
	Authenticator *auth.Authenticator
	Ingest        []registry.Module
	LagPollers    []*kafkainfra.LagPoller
	Consumers     []ConsumerRunner

	KafkaProducer   *kgo.Client
	consumerClients []*kgo.Client
}

func newInfra(cfg config.Config) (_ *Infra, err error) {
	dbConn, err := openMySQL(cfg)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = dbConn.Close()
		}
	}()

	chConn, err := openClickHouse(cfg)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = chConn.Close()
		}
	}()

	ingest, err := buildIngest(cfg, chConn)
	if err != nil {
		return nil, err
	}

	authenticator := auth.NewAuthenticator(authrepo.New(dbConn))

	return &Infra{
		DB:              dbConn,
		CH:              chConn,
		Authenticator:   authenticator,
		Ingest:          ingest.modules,
		LagPollers:      ingest.lagPollers,
		Consumers:       ingest.consumers,
		KafkaProducer:   ingest.producerClient,
		consumerClients: ingest.consumerClients,
	}, nil
}

// openMySQL opens the pool read-only for API-key auth. Ingest does not own the
// MySQL schema (query does), so it never migrates.
func openMySQL(cfg config.Config) (*sql.DB, error) {
	dbConn, err := dbutil.Open(cfg.MySQLDSN(), cfg.MySQL.MaxOpenConns, cfg.MySQL.MaxIdleConns)
	if err != nil {
		return nil, fmt.Errorf("mysql: %w", err)
	}
	slog.Info("mysql connected (read-only, auth)",
		slog.String("addr", net.JoinHostPort(cfg.MySQL.Host, cfg.MySQL.Port)),
		slog.String("database", cfg.MySQL.Database),
	)
	return dbConn, nil
}

// openClickHouse opens the connection and runs migrations. Ingest owns the
// ClickHouse write schema, so it migrates on startup.
func openClickHouse(cfg config.Config) (clickhouse.Conn, error) {
	chConn, err := dbutil.OpenClickHouseConn(cfg.ClickHouseDSN())
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	slog.Info("clickhouse connected",
		slog.String("addr", net.JoinHostPort(cfg.ClickHouse.Host, cfg.ClickHouse.Port)),
		slog.String("database", cfg.ClickHouse.Database),
	)
	if err := runMigrate(chConn, cfg.ClickHouse.Database); err != nil {
		_ = chConn.Close()
		return nil, fmt.Errorf("clickhouse migrate: %w", err)
	}
	return chConn, nil
}

func runMigrate(conn clickhouse.Conn, database string) error {
	m := &dbutil.Migrator{
		DB:       conn,
		FS:       chembed.FS,
		Database: database,
		Logger:   func(format string, args ...any) { slog.Info(fmt.Sprintf("chmigrate: "+format, args...)) },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	applied, skipped, err := m.Up(ctx)
	if err != nil {
		return err
	}
	slog.Info("chmigrate: complete", slog.Int("applied", applied), slog.Int("skipped", skipped))
	return nil
}

func (i *Infra) Close() error {
	if i == nil {
		return nil
	}
	if n := len(i.consumerClients); n > 0 {
		for _, c := range i.consumerClients {
			c.Close()
		}
		slog.Info("kafka consumers closed", slog.Int("count", n))
	}
	if i.KafkaProducer != nil {
		i.KafkaProducer.Close()
		slog.Info("kafka producer closed")
	}
	if i.CH != nil {
		_ = i.CH.Close()
		slog.Info("clickhouse connection closed")
	}
	if i.DB != nil {
		_ = i.DB.Close()
		slog.Info("mysql connection closed")
	}
	return nil
}
