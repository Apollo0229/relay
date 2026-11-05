// Command relay publishes the Postgres outbox to Kafka.
//
// Configuration comes from the environment:
//
//	RELAY_DATABASE_URL    Postgres connection string (required)
//	RELAY_KAFKA_BROKERS   comma-separated seed brokers (required)
//	RELAY_BATCH_SIZE      messages per round (default 500)
//	RELAY_POLL_INTERVAL   wait between rounds while the outbox is empty (default 100ms)
//	RELAY_MAX_BACKOFF     longest wait after a failed round (default 30s)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Apollo0229/relay/services/relay/internal/relay"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("relay stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.databaseURL)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("database: %w", err)
	}

	pub, err := relay.NewKafkaPublisher(cfg.brokers)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	defer pub.Close()
	if err := pub.Ping(ctx); err != nil {
		return fmt.Errorf("kafka: %w", err)
	}

	r := &relay.Relay{
		Store:        &relay.PostgresStore{Pool: pool},
		Publisher:    pub,
		Logger:       log,
		BatchSize:    cfg.batchSize,
		PollInterval: cfg.pollInterval,
		MaxBackoff:   cfg.maxBackoff,
	}
	log.Info("relay started", "brokers", cfg.brokers, "batch_size", cfg.batchSize)
	err = r.Run(ctx)
	log.Info("relay shutting down")
	return err
}

type config struct {
	databaseURL  string
	brokers      []string
	batchSize    int
	pollInterval time.Duration
	maxBackoff   time.Duration
}

func loadConfig() (config, error) {
	cfg := config{
		databaseURL:  os.Getenv("RELAY_DATABASE_URL"),
		batchSize:    500,
		pollInterval: 100 * time.Millisecond,
		maxBackoff:   30 * time.Second,
	}
	if cfg.databaseURL == "" {
		return cfg, fmt.Errorf("RELAY_DATABASE_URL is required")
	}
	for _, b := range strings.Split(os.Getenv("RELAY_KAFKA_BROKERS"), ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.brokers = append(cfg.brokers, b)
		}
	}
	if len(cfg.brokers) == 0 {
		return cfg, fmt.Errorf("RELAY_KAFKA_BROKERS is required")
	}
	if v := os.Getenv("RELAY_BATCH_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return cfg, fmt.Errorf("RELAY_BATCH_SIZE must be a positive integer, got %q", v)
		}
		cfg.batchSize = n
	}
	for name, dst := range map[string]*time.Duration{
		"RELAY_POLL_INTERVAL": &cfg.pollInterval,
		"RELAY_MAX_BACKOFF":   &cfg.maxBackoff,
	} {
		if v := os.Getenv(name); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil || d <= 0 {
				return cfg, fmt.Errorf("%s must be a positive duration such as 500ms, got %q", name, v)
			}
			*dst = d
		}
	}
	return cfg, nil
}
