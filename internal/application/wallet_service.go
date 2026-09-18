package application

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/felipemalves/distributed-betting-processor/internal/domain"
	"github.com/felipemalves/distributed-betting-processor/internal/observe"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type WalletService struct {
	uow  *postgresUnitOfWork
	repo *postgresRepos
	log  *slog.Logger
	met  *observe.Metrics
}

type postgresUnitOfWork struct {
	Within func(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error
}

type postgresRepos struct {
	InsertWallet            func(ctx context.Context, tx pgx.Tx, w *domain.Wallet) error
	GetWalletByIDForUpdate  func(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.Wallet, error)
	GetWalletByID           func(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	UpdateWallet            func(ctx context.Context, tx pgx.Tx, w *domain.Wallet, expectedVersion int64) error
	FindWalletByPlayerCurrency func(ctx context.Context, playerID uuid.UUID, currency string) (*domain.Wallet, error)
	InsertWager             func(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction) error
	InsertLedger            func(ctx context.Context, tx pgx.Tx, e *domain.LedgerEntry) error
	InsertOutbox            func(ctx context.Context, tx pgx.Tx, e domain.OutboxEvent) error
	ListLedger              func(ctx context.Context, walletID uuid.UUID, afterCreatedAt *time.Time, afterID *uuid.UUID, limit int) ([]*domain.LedgerEntry, error)
	CountLedger             func(ctx context.Context, walletID uuid.UUID) (int, error)
	SumCreditsDebits        func(ctx context.Context, walletID uuid.UUID) (int64, int64, error)
}

func NewWalletService(
	uowFn func(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error,
	repos postgresRepos,
	log *slog.Logger,
	met *observe.Metrics,
) *WalletService {
	return &WalletService{
		uow:  &postgresUnitOfWork{Within: uowFn},
		repo: &repos,
		log:  log,
		met:  met,
	}
}

type OpenWalletInput struct {
	PlayerID       uuid.UUID
	InitialBalance domain.Money
}

type OpenWalletResult struct {
	Wallet *domain.Wallet
}

func (s *WalletService) OpenWallet(ctx context.Context, in OpenWalletInput) (*OpenWalletResult, error) {
	existing, err := s.repo.FindWalletByPlayerCurrency(ctx, in.PlayerID, in.InitialBalance.Currency())
	if err == nil && existing != nil {
		return nil, domain.ErrAlreadyExists
	}
	if err != nil && err != domain.ErrNotFound {
		return nil, err
	}
	w, err := domain.NewWallet(in.PlayerID, in.InitialBalance)
	if err != nil {
		return nil, err
	}
	err = s.uow.Within(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := s.repo.InsertWallet(ctx, tx, w); err != nil {
			return err
		}
		if in.InitialBalance.IsPositive() {
			opening, err := domain.NewOpeningTransaction(w.ID(), w.PlayerID(), in.InitialBalance)
			if err != nil {
				return err
			}
			if err := s.repo.InsertWager(ctx, tx, opening); err != nil {
				return err
			}
			before := domain.ZeroMoney(w.Currency())
			entry, err := domain.NewLedgerEntry(w.ID(), opening.ID(), domain.DirectionCredit, in.InitialBalance, before, in.InitialBalance)
			if err != nil {
				return err
			}
			if err := s.repo.InsertLedger(ctx, tx, entry); err != nil {
				return err
			}
			if err := s.emitBalanceChanged(ctx, tx, w, opening.ID(), domain.DirectionCredit, in.InitialBalance, before, in.InitialBalance); err != nil {
				return err
			}
			if err := s.emitWagerProcessed(ctx, tx, opening, w); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &OpenWalletResult{Wallet: w}, nil
}

func (s *WalletService) emitBalanceChanged(ctx context.Context, tx pgx.Tx, w *domain.Wallet, txID uuid.UUID, dir domain.LedgerDirection, amount, before, after domain.Money) error {
	ev, err := domain.NewOutboxEvent(domain.EventWalletBalanceChanged, w.ID(), txID.String(), domain.WalletBalanceChangedData{
		WalletID:      w.ID(),
		TransactionID: txID,
		Direction:     string(dir),
		Money:         domain.MoneyToDTO(amount),
		BalanceBefore: domain.MoneyToDTO(before),
		BalanceAfter:  domain.MoneyToDTO(after),
		WalletVersion: w.Version(),
	})
	if err != nil {
		return err
	}
	return s.repo.InsertOutbox(ctx, tx, ev)
}

func (s *WalletService) emitWagerProcessed(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction, w *domain.Wallet) error {
	bal := domain.MoneyToDTO(w.Balance())
	ev, err := domain.NewOutboxEvent(domain.EventWagerProcessed, t.ID(), t.ID().String(), domain.WagerProcessedData{
		TransactionID: t.ID(),
		WalletID:      w.ID(),
		ProviderID:    t.ProviderID(),
		ExternalID:    t.ExternalID(),
		Kind:          string(t.Kind()),
		Status:        string(t.Status()),
		Balance:       &bal,
	})
	if err != nil {
		return err
	}
	return s.repo.InsertOutbox(ctx, tx, ev)
}

func (s *WalletService) GetWallet(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return s.repo.GetWalletByID(ctx, id)
}

type LedgerPage struct {
	Entries    []*domain.LedgerEntry
	NextCursor string
}

func (s *WalletService) ListLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (*LedgerPage, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	var afterTime *time.Time
	var afterID *uuid.UUID
	if cursor != "" {
		t, id, err := decodeCursor(cursor)
		if err != nil {
			return nil, err
		}
		afterTime = &t
		afterID = &id
	}
	entries, err := s.repo.ListLedger(ctx, walletID, afterTime, afterID, limit+1)
	if err != nil {
		return nil, err
	}
	page := &LedgerPage{}
	if len(entries) > limit {
		last := entries[limit-1]
		page.NextCursor = encodeCursor(last.CreatedAt(), last.ID())
		page.Entries = entries[:limit]
	} else {
		page.Entries = entries
	}
	return page, nil
}

func encodeCursor(t time.Time, id uuid.UUID) string {
	b, _ := json.Marshal(struct {
		T time.Time `json:"t"`
		I uuid.UUID `json:"i"`
	}{t, id})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(c string) (time.Time, uuid.UUID, error) {
	b, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, uuid.UUID{}, err
	}
	var v struct {
		T time.Time `json:"t"`
		I uuid.UUID `json:"i"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		return time.Time{}, uuid.UUID{}, err
	}
	return v.T, v.I, nil
}

type ReconciliationResult struct {
	WalletID          uuid.UUID
	StoredBalance     domain.Money
	CalculatedBalance domain.Money
	Difference        domain.Money
	Consistent        bool
	CheckedEntries    int
}

func (s *WalletService) Reconcile(ctx context.Context, walletID uuid.UUID) (*ReconciliationResult, error) {
	w, err := s.repo.GetWalletByID(ctx, walletID)
	if err != nil {
		return nil, err
	}
	credits, debits, err := s.repo.SumCreditsDebits(ctx, walletID)
	if err != nil {
		return nil, err
	}
	calcMinor := credits - debits
	calc := domain.MoneyFromMinor(calcMinor, w.Currency())
	diff, err := w.Balance().Sub(calc)
	if err != nil {
		return nil, err
	}
	count, err := s.repo.CountLedger(ctx, walletID)
	if err != nil {
		return nil, err
	}
	consistent := diff.IsZero()
	if !consistent {
		s.met.ReconciliationMismatch.Inc()
		s.log.Warn("reconciliation mismatch",
			"walletId", walletID.String(),
			"stored", w.Balance().FormatAmount(),
			"calculated", calc.FormatAmount(),
		)
	}
	return &ReconciliationResult{
		WalletID:          walletID,
		StoredBalance:     w.Balance(),
		CalculatedBalance: calc,
		Difference:        diff,
		Consistent:        consistent,
		CheckedEntries:    count,
	}, nil
}

