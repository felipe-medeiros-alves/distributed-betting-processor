package application

import (
	"context"
	"time"

	"github.com/felipemalves/distributed-betting-processor/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type UnitOfWork interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error
}

type WalletRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, w *domain.Wallet) error
	GetByIDForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.Wallet, error)
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	Update(ctx context.Context, tx pgx.Tx, w *domain.Wallet, expectedVersion int64) error
	FindByPlayerCurrency(ctx context.Context, playerID uuid.UUID, currency string) (*domain.Wallet, error)
}

type WagerRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction) error
	Update(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	GetByProviderExternal(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*domain.WagerTransaction, error)
	FindSuccessfulReversal(ctx context.Context, tx pgx.Tx, providerID, refExternalID string, kind domain.TransactionKind) (*domain.WagerTransaction, error)
	ListPendingReferenceDue(ctx context.Context, tx pgx.Tx, limit int) ([]*domain.WagerTransaction, error)
}

type LedgerRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, e *domain.LedgerEntry) error
	ListByWallet(ctx context.Context, walletID uuid.UUID, afterCreatedAt *time.Time, afterID *uuid.UUID, limit int) ([]*domain.LedgerEntry, error)
	CountByWallet(ctx context.Context, walletID uuid.UUID) (int, error)
	SumCreditsDebits(ctx context.Context, walletID uuid.UUID) (credits, debits int64, err error)
}

type InboxRepository interface {
	TryInsert(ctx context.Context, tx pgx.Tx, consumerName, messageID, payloadHash string) (inserted bool, err error)
	Complete(ctx context.Context, tx pgx.Tx, consumerName, messageID string) error
}

type OutboxRepository interface {
	Insert(ctx context.Context, tx pgx.Tx, e domain.OutboxEvent) error
	ClaimPending(ctx context.Context, workerID string, lease time.Duration, limit int) ([]domain.OutboxEvent, error)
	MarkPublished(ctx context.Context, eventID uuid.UUID) error
	ReleaseClaim(ctx context.Context, eventID uuid.UUID) error
	ScheduleRetry(ctx context.Context, eventID uuid.UUID, next time.Time) error
	CountPending(ctx context.Context) (int64, error)
}
