package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr string

	DatabaseURL string

	OIDCIssuer   string
	OIDCAudience string
	OIDCJWKSURL  string

	AWSRegion          string
	AWSEndpoint        string
	SQSQueueURL        string
	SQSOutboxQueueURL  string
	SQSVisibilitySec   int32
	SQSMaxReceiveCount int32

	ConsumerName string

	OutboxPollInterval time.Duration
	OutboxLease        time.Duration
	ReferenceMaxTries  int
	ReferenceTTL       time.Duration

	ShutdownTimeout time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:           env("HTTP_ADDR", ":8080"),
		DatabaseURL:        env("DATABASE_URL", "postgres://betting:betting@localhost:5432/betting?sslmode=disable"),
		OIDCIssuer:         env("OIDC_ISSUER", "http://localhost:8081/realms/betting"),
		OIDCAudience:       env("OIDC_AUDIENCE", "betting-api"),
		OIDCJWKSURL:        env("OIDC_JWKS_URL", "http://localhost:8081/realms/betting/protocol/openid-connect/certs"),
		AWSRegion:          env("AWS_REGION", "us-east-1"),
		AWSEndpoint:        env("AWS_ENDPOINT", "http://localhost:4566"),
		SQSQueueURL:        env("SQS_WAGER_QUEUE_URL", ""),
		SQSOutboxQueueURL:  env("SQS_OUTBOX_QUEUE_URL", ""),
		SQSVisibilitySec:   int32(envInt("SQS_VISIBILITY_TIMEOUT_SEC", 30)),
		SQSMaxReceiveCount: int32(envInt("SQS_MAX_RECEIVE_COUNT", 5)),
		ConsumerName:       env("SQS_CONSUMER_NAME", "wager-consumer"),
		OutboxPollInterval: envDuration("OUTBOX_POLL_INTERVAL", 2*time.Second),
		OutboxLease:        envDuration("OUTBOX_LEASE", 30*time.Second),
		ReferenceMaxTries:  envInt("REFERENCE_MAX_TRIES", 20),
		ReferenceTTL:       envDuration("REFERENCE_TTL", 15*time.Minute),
		ShutdownTimeout:    envDuration("SHUTDOWN_TIMEOUT", 25*time.Second),
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return def
}
