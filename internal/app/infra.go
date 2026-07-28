package app

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/optikklabs/ingest/internal/app/registry"
	"github.com/optikklabs/ingest/internal/auth"
	"github.com/optikklabs/ingest/internal/authrepo"
	"github.com/optikklabs/ingest/internal/config"
	dbutil "github.com/optikklabs/ingest/internal/infra/database"
	kafkainfra "github.com/optikklabs/ingest/internal/infra/kafka"
)

type ConsumerRunner interface {
	Run(ctx context.Context)
}

type Infra struct {
	DB            *sql.DB
	CH            clickhouse.Conn
	Authenticator *auth.Authenticator
	Ingest        []registry.Module
	LagPollers    []*kafkainfra.LagPoller
	Consumers     []ConsumerRunner

	KafkaProducer   *kgo.Client
	consumerClients []*kgo.Client
	closers         []func()
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

	authenticator := auth.NewAuthenticator(authrepo.New(dbConn), cfg.APIKeyCacheTTL(), cfg.APIKeyCacheSize())

	return &Infra{
		DB:              dbConn,
		CH:              chConn,
		Authenticator:   authenticator,
		Ingest:          ingest.modules,
		LagPollers:      ingest.lagPollers,
		Consumers:       ingest.consumers,
		KafkaProducer:   ingest.producerClient,
		consumerClients: ingest.consumerClients,
		closers:         ingest.closers,
	}, nil
}

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

func openClickHouse(cfg config.Config) (clickhouse.Conn, error) {
	chConn, err := dbutil.OpenClickHouseConn(cfg.ClickHouseDSN(), cfg.ClickHouseMaxOpenConns(), cfg.ClickHouseMaxIdleConns())
	if err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}
	slog.Info("clickhouse connected",
		slog.String("addr", net.JoinHostPort(cfg.ClickHouse.Host, cfg.ClickHouse.Port)),
		slog.String("database", cfg.ClickHouse.Database),
	)
	return chConn, nil
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

	for _, closeFn := range i.closers {
		closeFn()
	}
	if n := len(i.closers); n > 0 {
		slog.Info("async side-publishers drained", slog.Int("count", n))
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
