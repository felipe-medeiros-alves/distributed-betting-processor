//go:build integration

package integration

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/felipemalves/distributed-betting-processor/internal/application"
	"github.com/felipemalves/distributed-betting-processor/internal/domain"
	"github.com/felipemalves/distributed-betting-processor/internal/observe"
	"github.com/felipemalves/distributed-betting-processor/internal/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupPostgres(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "betting",
			"POSTGRES_PASSWORD": "betting",
			"POSTGRES_DB":       "betting",
		},
		WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(2 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "5432")
	url := fmt.Sprintf("postgres://betting:betting@%s:%s/betting?sslmode=disable", host, port.Port())
	pool, err := postgres.NewPool(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	migrationSQL, err := os.ReadFile("../../migrations/000001_init.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
		t.Fatal(err)
	}
	return pool, func() {
		pool.Close()
		_ = c.Terminate(ctx)
	}
}

func newServices(pool *pgxpool.Pool) (*application.WalletService, *application.WagerService) {
	repos := postgres.NewRepos(pool)
	uow := postgres.NewUnitOfWork(pool)
	log := slog.Default()
	met := observe.NewMetrics()
	wallets := application.NewWalletService(uow.WithinTx, application.PostgresReposFrom(repos), log, met)
	wagers := application.NewWagerService(uow.WithinTx, application.WagerReposFrom(repos), log, met, 20, 15*time.Minute)
	return wallets, wagers
}

func TestConcurrentDualBet80(t *testing.T) {
	pool, cleanup := setupPostgres(t)
	defer cleanup()
	wallets, wagers := newServices(pool)
	player := uuid.Must(uuid.NewV7())
	open, err := wallets.OpenWallet(context.Background(), application.OpenWalletInput{
		PlayerID:       player,
		InitialBalance: domain.MustMoney("100.00", "BRL"),
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make([]*application.SubmitWagerResult, 2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ext := fmt.Sprintf("bet-%d", idx)
			fields := domain.WagerPayloadFields{
				ProviderID: "provider-a", ExternalTransactionID: ext,
				PlayerID: player.String(), WalletID: open.Wallet.ID().String(),
				RoundID: "r1", GameID: "g1", Kind: "BET",
				MoneyAmount: "80.00", MoneyCurrency: "BRL",
			}
			results[idx], errs[idx] = wagers.Submit(context.Background(), application.SubmitWagerInput{
				ProviderID: "provider-a", ExternalTransactionID: ext,
				IdempotencyKey: "provider-a:" + ext,
				PlayerID: player, WalletID: open.Wallet.ID(),
				RoundID: "r1", GameID: "g1", Kind: domain.KindBet,
				Money: domain.MustMoney("80.00", "BRL"), PayloadHash: domain.HashWagerPayload(fields),
			})
		}(i)
	}
	wg.Wait()
	processed, rejected := 0, 0
	for i := 0; i < 2; i++ {
		if errs[i] != nil {
			t.Fatalf("err %d: %v", i, errs[i])
		}
		switch results[i].Status {
		case domain.StatusProcessed:
			processed++
		case domain.StatusRejected:
			rejected++
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("processed=%d rejected=%d", processed, rejected)
	}
	w, err := wallets.GetWallet(context.Background(), open.Wallet.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !w.Balance().Equal(domain.MustMoney("20.00", "BRL")) {
		t.Fatalf("balance %s", w.Balance().FormatAmount())
	}
}

func TestIdempotentFiftyParallelBets(t *testing.T) {
	pool, cleanup := setupPostgres(t)
	defer cleanup()
	wallets, wagers := newServices(pool)
	player := uuid.Must(uuid.NewV7())
	open, _ := wallets.OpenWallet(context.Background(), application.OpenWalletInput{
		PlayerID: player, InitialBalance: domain.MustMoney("5000.00", "BRL"),
	})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fields := domain.WagerPayloadFields{
				ProviderID: "provider-a", ExternalTransactionID: "same-bet",
				PlayerID: player.String(), WalletID: open.Wallet.ID().String(),
				RoundID: "r1", GameID: "g1", Kind: "BET",
				MoneyAmount: "10.00", MoneyCurrency: "BRL",
			}
			_, _ = wagers.Submit(context.Background(), application.SubmitWagerInput{
				ProviderID: "provider-a", ExternalTransactionID: "same-bet",
				IdempotencyKey: "provider-a:same-bet",
				PlayerID: player, WalletID: open.Wallet.ID(),
				RoundID: "r1", GameID: "g1", Kind: domain.KindBet,
				Money: domain.MustMoney("10.00", "BRL"), PayloadHash: domain.HashWagerPayload(fields),
			})
		}()
	}
	wg.Wait()
	w, _ := wallets.GetWallet(context.Background(), open.Wallet.ID())
	if !w.Balance().Equal(domain.MustMoney("4990.00", "BRL")) {
		t.Fatalf("balance %s", w.Balance().FormatAmount())
	}
}
