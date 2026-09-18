package app

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/felipemalves/distributed-betting-processor/internal/application"
	"github.com/felipemalves/distributed-betting-processor/internal/auth"
	"github.com/felipemalves/distributed-betting-processor/internal/config"
	"github.com/felipemalves/distributed-betting-processor/internal/httpapi"
	"github.com/felipemalves/distributed-betting-processor/internal/messaging"
	"github.com/felipemalves/distributed-betting-processor/internal/observe"
	"github.com/felipemalves/distributed-betting-processor/internal/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

func Module() fx.Option {
	return fx.Options(
		fx.Provide(
			config.Load,
			observe.NewMetrics,
			newLogger,
			newPool,
			postgres.NewUnitOfWork,
			postgres.NewRepos,
			newWalletService,
			newWagerService,
			newVerifier,
			newHTTPServer,
			messaging.NewSQSClient,
			messaging.NewWagerConsumer,
			newOutboxAdapter,
			provideOutboxPublisher,
			newReferenceWorker,
		),
		fx.Invoke(messaging.RegisterLifecycle),
		fx.Invoke(startHTTPServer),
	)
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func newPool(lc fx.Lifecycle, cfg config.Config) (*pgxpool.Pool, error) {
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			pool.Close()
			return nil
		},
	})
	return pool, nil
}

func newWalletService(uow *postgres.UnitOfWork, repos *postgres.Repos, log *slog.Logger, met *observe.Metrics) *application.WalletService {
	pr := application.PostgresReposFrom(repos)
	return application.NewWalletService(uow.WithinTx, pr, log, met)
}

func newWagerService(uow *postgres.UnitOfWork, repos *postgres.Repos, log *slog.Logger, met *observe.Metrics, cfg config.Config) *application.WagerService {
	wr := application.WagerReposFrom(repos)
	return application.NewWagerService(uow.WithinTx, wr, log, met, cfg.ReferenceMaxTries, cfg.ReferenceTTL)
}

func newVerifier(cfg config.Config) (*auth.Verifier, error) {
	if cfg.OIDCIssuer == "" {
		return nil, nil
	}
	return auth.NewVerifier(context.Background(), cfg.OIDCIssuer, cfg.OIDCAudience)
}

func newHTTPServer(cfg config.Config, wallets *application.WalletService, wagers *application.WagerService, verifier *auth.Verifier, pool *pgxpool.Pool, sqs *messaging.SQSClient, log *slog.Logger) *httpapi.Server {
	ready := func(ctx context.Context) error {
		if err := postgres.Ping(ctx, pool); err != nil {
			return err
		}
		return sqs.Ping(ctx, cfg.SQSQueueURL)
	}
	return httpapi.NewServer(wallets, wagers, verifier, log, ready)
}

type httpServer struct {
	srv *http.Server
}

func startHTTPServer(lc fx.Lifecycle, cfg config.Config, api *httpapi.Server) {
	hs := &httpServer{
		srv: &http.Server{
			Addr:              cfg.HTTPAddr,
			Handler:           api.Router(),
			ReadHeaderTimeout: 10 * time.Second,
		},
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				if err := hs.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					slog.Error("http server failed", "error", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			shCtx, cancel := context.WithTimeout(ctx, cfg.ShutdownTimeout)
			defer cancel()
			return hs.srv.Shutdown(shCtx)
		},
	})
}

func newOutboxAdapter(repos *postgres.Repos) messaging.OutboxRepoAdapter {
	return messaging.OutboxRepoAdapter{Repos: repos}
}

// satisfy fx provide for interface
func provideOutboxPublisher(cfg config.Config, sqs *messaging.SQSClient, adapter messaging.OutboxRepoAdapter, met *observe.Metrics, log *slog.Logger) *messaging.OutboxPublisher {
	return messaging.NewOutboxPublisher(cfg, sqs, adapter, met, log)
}

func newReferenceWorker(wagers *application.WagerService, cfg config.Config) *messaging.ReferenceWorker {
	return messaging.NewReferenceWorker(wagers, cfg.OutboxPollInterval)
}
