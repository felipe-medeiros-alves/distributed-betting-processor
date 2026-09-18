package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felipemalves/distributed-betting-processor/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type UnitOfWork struct {
	pool *pgxpool.Pool
}

func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork {
	return &UnitOfWork{pool: pool}
}

func (u *UnitOfWork) WithinTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := fn(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type Repos struct {
	Pool *pgxpool.Pool
}

func NewRepos(pool *pgxpool.Pool) *Repos {
	return &Repos{Pool: pool}
}

func (r *Repos) InsertWallet(ctx context.Context, tx pgx.Tx, w *domain.Wallet) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), w.Currency(), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	return err
}

func (r *Repos) GetWalletByIDForUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.Wallet, error) {
	row := tx.QueryRow(ctx, `
		SELECT id, player_id, currency, balance_minor, version, created_at, updated_at
		FROM wallets WHERE id = $1 FOR UPDATE`, id)
	return scanWallet(row)
}

func (r *Repos) GetWalletByID(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	row := r.Pool.QueryRow(ctx, `
		SELECT id, player_id, currency, balance_minor, version, created_at, updated_at
		FROM wallets WHERE id = $1`, id)
	return scanWallet(row)
}

func scanWallet(row pgx.Row) (*domain.Wallet, error) {
	var id, playerID uuid.UUID
	var currency string
	var balanceMinor, version int64
	var createdAt, updatedAt time.Time
	if err := row.Scan(&id, &playerID, &currency, &balanceMinor, &version, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	bal := domain.MoneyFromMinor(balanceMinor, currency)
	return domain.RehydrateWallet(id, playerID, bal, version, createdAt, updatedAt)
}

func (r *Repos) UpdateWallet(ctx context.Context, tx pgx.Tx, w *domain.Wallet, expectedVersion int64) error {
	tag, err := tx.Exec(ctx, `
		UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4
		WHERE id = $1 AND version = $5`,
		w.ID(), w.Balance().Minor(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("wallet version conflict")
	}
	return nil
}

func (r *Repos) FindWalletByPlayerCurrency(ctx context.Context, playerID uuid.UUID, currency string) (*domain.Wallet, error) {
	row := r.Pool.QueryRow(ctx, `
		SELECT id, player_id, currency, balance_minor, version, created_at, updated_at
		FROM wallets WHERE player_id = $1 AND currency = $2`, playerID, currency)
	return scanWallet(row)
}

func (r *Repos) InsertWager(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction) error {
	var refID *uuid.UUID
	if t.RefTransactionID() != nil {
		refID = t.RefTransactionID()
	}
	var obsMinor *int64
	var obsCur *string
	if t.ObservedBalance() != nil {
		m := t.ObservedBalance().Minor()
		c := t.ObservedBalance().Currency()
		obsMinor = &m
		obsCur = &c
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, origin, kind, status, wallet_id, player_id, provider_id, external_transaction_id,
			idempotency_key, payload_hash, round_id, game_id, reference_external_transaction_id,
			reference_transaction_id, amount_minor, currency, failure_code, observed_balance_minor,
			observed_balance_currency, reference_attempts, next_reference_attempt_at, created_at, updated_at, processed_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24
		)`,
		t.ID(), t.Origin(), t.Kind(), t.Status(), t.WalletID(), t.PlayerID(), nullStr(t.ProviderID()),
		nullStr(t.ExternalID()), nullStr(t.IdempotencyKey()), nullStr(t.PayloadHash()), nullStr(t.RoundID()),
		nullStr(t.GameID()), nullStr(t.RefExternalID()), refID, t.Money().Minor(), t.Money().Currency(),
		nullFailure(t.FailureCode()), obsMinor, obsCur, t.ReferenceAttempts(), t.NextReferenceAttemptAt(),
		t.CreatedAt(), t.UpdatedAt(), t.ProcessedAt())
	return err
}

func (r *Repos) UpdateWager(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction) error {
	var refID *uuid.UUID
	if t.RefTransactionID() != nil {
		refID = t.RefTransactionID()
	}
	var obsMinor *int64
	var obsCur *string
	if t.ObservedBalance() != nil {
		m := t.ObservedBalance().Minor()
		c := t.ObservedBalance().Currency()
		obsMinor = &m
		obsCur = &c
	}
	_, err := tx.Exec(ctx, `
		UPDATE wager_transactions SET
			status = $2, reference_transaction_id = $3, failure_code = $4,
			observed_balance_minor = $5, observed_balance_currency = $6,
			reference_attempts = $7, next_reference_attempt_at = $8,
			updated_at = $9, processed_at = $10
		WHERE id = $1`,
		t.ID(), t.Status(), refID, nullFailure(t.FailureCode()), obsMinor, obsCur,
		t.ReferenceAttempts(), t.NextReferenceAttemptAt(), t.UpdatedAt(), t.ProcessedAt())
	return err
}

func nullStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullFailure(c domain.FailureCode) *string {
	if c == "" {
		return nil
	}
	s := string(c)
	return &s
}

func (r *Repos) GetWagerByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	row := r.Pool.QueryRow(ctx, wagerSelect+" WHERE id = $1", id)
	return scanWager(row)
}

func (r *Repos) GetWagerByProviderExternal(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	row := r.Pool.QueryRow(ctx, wagerSelect+" WHERE provider_id = $1 AND external_transaction_id = $2", providerID, externalID)
	return scanWager(row)
}

func (r *Repos) GetWagerByIdempotencyKey(ctx context.Context, key string) (*domain.WagerTransaction, error) {
	row := r.Pool.QueryRow(ctx, wagerSelect+" WHERE idempotency_key = $1", key)
	return scanWager(row)
}

const wagerSelect = `
	SELECT id, origin, kind, status, wallet_id, player_id,
		COALESCE(provider_id,''), COALESCE(external_transaction_id,''), COALESCE(idempotency_key,''),
		COALESCE(payload_hash,''), COALESCE(round_id,''), COALESCE(game_id,''),
		COALESCE(reference_external_transaction_id,''), reference_transaction_id,
		amount_minor, currency, COALESCE(failure_code,''), observed_balance_minor, observed_balance_currency,
		reference_attempts, next_reference_attempt_at, created_at, updated_at, processed_at
	FROM wager_transactions`

func scanWager(row pgx.Row) (*domain.WagerTransaction, error) {
	var id, walletID, playerID uuid.UUID
	var origin, kind, status string
	var providerID, externalID, idempotencyKey, payloadHash, roundID, gameID, refExternal, failureCode string
	var refTxID *uuid.UUID
	var amountMinor int64
	var currency string
	var obsMinor *int64
	var obsCur *string
	var refAttempts int
	var nextRef *time.Time
	var createdAt, updatedAt time.Time
	var processedAt *time.Time
	if err := row.Scan(&id, &origin, &kind, &status, &walletID, &playerID,
		&providerID, &externalID, &idempotencyKey, &payloadHash, &roundID, &gameID, &refExternal, &refTxID,
		&amountMinor, &currency, &failureCode, &obsMinor, &obsCur, &refAttempts, &nextRef,
		&createdAt, &updatedAt, &processedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	money := domain.MoneyFromMinor(amountMinor, currency)
	var obs *domain.Money
	if obsMinor != nil && obsCur != nil {
		o := domain.MoneyFromMinor(*obsMinor, *obsCur)
		obs = &o
	}
	return domain.RehydrateWagerTransaction(
		id, domain.TransactionOrigin(origin), domain.TransactionKind(kind), domain.TransactionStatus(status),
		walletID, playerID, providerID, externalID, idempotencyKey, payloadHash, roundID, gameID, refExternal,
		refTxID, money, domain.FailureCode(failureCode), obs, refAttempts, nextRef, createdAt, updatedAt, processedAt,
	), nil
}

func (r *Repos) FindSuccessfulReversal(ctx context.Context, tx pgx.Tx, providerID, refExternalID string, kind domain.TransactionKind) (*domain.WagerTransaction, error) {
	row := tx.QueryRow(ctx, wagerSelect+`
		WHERE provider_id = $1 AND reference_external_transaction_id = $2 AND kind = $3 AND status = 'PROCESSED'`,
		providerID, refExternalID, kind)
	return scanWager(row)
}

func (r *Repos) ListPendingReferenceDue(ctx context.Context, tx pgx.Tx, limit int) ([]*domain.WagerTransaction, error) {
	rows, err := tx.Query(ctx, wagerSelect+`
		WHERE status = 'PENDING_REFERENCE' AND (next_reference_attempt_at IS NULL OR next_reference_attempt_at <= NOW())
		ORDER BY next_reference_attempt_at NULLS FIRST
		LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.WagerTransaction
	for rows.Next() {
		var id, walletID, playerID uuid.UUID
		var origin, kind, status string
		var providerID, externalID, idempotencyKey, payloadHash, roundID, gameID, refExternal, failureCode string
		var refTxID *uuid.UUID
		var amountMinor int64
		var currency string
		var obsMinor *int64
		var obsCur *string
		var refAttempts int
		var nextRef *time.Time
		var createdAt, updatedAt time.Time
		var processedAt *time.Time
		if err := rows.Scan(&id, &origin, &kind, &status, &walletID, &playerID,
			&providerID, &externalID, &idempotencyKey, &payloadHash, &roundID, &gameID, &refExternal, &refTxID,
			&amountMinor, &currency, &failureCode, &obsMinor, &obsCur, &refAttempts, &nextRef,
			&createdAt, &updatedAt, &processedAt); err != nil {
			return nil, err
		}
		money := domain.MoneyFromMinor(amountMinor, currency)
		var obs *domain.Money
		if obsMinor != nil && obsCur != nil {
			o := domain.MoneyFromMinor(*obsMinor, *obsCur)
			obs = &o
		}
		out = append(out, domain.RehydrateWagerTransaction(
			id, domain.TransactionOrigin(origin), domain.TransactionKind(kind), domain.TransactionStatus(status),
			walletID, playerID, providerID, externalID, idempotencyKey, payloadHash, roundID, gameID, refExternal,
			refTxID, money, domain.FailureCode(failureCode), obs, refAttempts, nextRef, createdAt, updatedAt, processedAt,
		))
	}
	return out, rows.Err()
}

func (r *Repos) InsertLedger(ctx context.Context, tx pgx.Tx, e *domain.LedgerEntry) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount_minor, currency,
			balance_before_minor, balance_after_minor, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		e.ID(), e.WalletID(), e.TransactionID(), e.Direction(), e.Amount().Minor(), e.Amount().Currency(),
		e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.CreatedAt())
	return err
}

func (r *Repos) ListLedger(ctx context.Context, walletID uuid.UUID, afterCreatedAt *time.Time, afterID *uuid.UUID, limit int) ([]*domain.LedgerEntry, error) {
	var rows pgx.Rows
	var err error
	if afterCreatedAt != nil && afterID != nil {
		rows, err = r.Pool.Query(ctx, `
			SELECT id, wallet_id, transaction_id, direction, amount_minor, currency,
				balance_before_minor, balance_after_minor, created_at
			FROM wallet_ledger_entries
			WHERE wallet_id = $1 AND (created_at, id) > ($2, $3)
			ORDER BY created_at, id LIMIT $4`, walletID, *afterCreatedAt, *afterID, limit)
	} else {
		rows, err = r.Pool.Query(ctx, `
			SELECT id, wallet_id, transaction_id, direction, amount_minor, currency,
				balance_before_minor, balance_after_minor, created_at
			FROM wallet_ledger_entries
			WHERE wallet_id = $1
			ORDER BY created_at, id LIMIT $2`, walletID, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLedgerRows(rows)
}

func scanLedgerRows(rows pgx.Rows) ([]*domain.LedgerEntry, error) {
	var out []*domain.LedgerEntry
	for rows.Next() {
		var id, walletID, txID uuid.UUID
		var direction string
		var amountMinor, beforeMinor, afterMinor int64
		var currency string
		var createdAt time.Time
		if err := rows.Scan(&id, &walletID, &txID, &direction, &amountMinor, &currency, &beforeMinor, &afterMinor, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, domain.RehydrateLedgerEntry(
			id, walletID, txID, domain.LedgerDirection(direction),
			domain.MoneyFromMinor(amountMinor, currency),
			domain.MoneyFromMinor(beforeMinor, currency),
			domain.MoneyFromMinor(afterMinor, currency),
			createdAt,
		))
	}
	return out, rows.Err()
}

func (r *Repos) CountLedger(ctx context.Context, walletID uuid.UUID) (int, error) {
	var n int
	err := r.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&n)
	return n, err
}

func (r *Repos) SumCreditsDebits(ctx context.Context, walletID uuid.UUID) (int64, int64, error) {
	var credits, debits int64
	err := r.Pool.QueryRow(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_minor ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount_minor ELSE 0 END), 0)
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&credits, &debits)
	return credits, debits, err
}

func (r *Repos) TryInsertInbox(ctx context.Context, tx pgx.Tx, consumerName, messageID, payloadHash string) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO inbox_messages (consumer_name, message_id, payload_hash)
		VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		consumerName, messageID, payloadHash)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func (r *Repos) CompleteInbox(ctx context.Context, tx pgx.Tx, consumerName, messageID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE inbox_messages SET completed_at = NOW()
		WHERE consumer_name = $1 AND message_id = $2`, consumerName, messageID)
	return err
}

func (r *Repos) InsertOutbox(ctx context.Context, tx pgx.Tx, e domain.OutboxEvent) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (event_id, aggregate_id, event_type, payload, correlation_id, causation_id, occurred_at, version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		e.EventID, e.AggregateID, e.EventType, e.Payload, e.CorrelationID, e.CausationID, e.OccurredAt, e.Version)
	return err
}

func (r *Repos) ClaimOutbox(ctx context.Context, workerID string, lease time.Duration, limit int) ([]domain.OutboxEvent, error) {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
		SELECT event_id, aggregate_id, event_type, payload, correlation_id, COALESCE(causation_id,''), occurred_at, version
		FROM outbox_events
		WHERE published_at IS NULL AND next_attempt_at <= NOW()
		  AND (locked_until IS NULL OR locked_until < NOW())
		ORDER BY next_attempt_at
		LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	var ids []uuid.UUID
	var out []domain.OutboxEvent
	for rows.Next() {
		var e domain.OutboxEvent
		if err := rows.Scan(&e.EventID, &e.AggregateID, &e.EventType, &e.Payload, &e.CorrelationID, &e.CausationID, &e.OccurredAt, &e.Version); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, e)
		ids = append(ids, e.EventID)
	}
	rows.Close()
	until := time.Now().UTC().Add(lease)
	for _, id := range ids {
		_, err := tx.Exec(ctx, `
			UPDATE outbox_events SET locked_until = $2, locked_by = $3, attempts = attempts + 1
			WHERE event_id = $1`, id, until, workerID)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Repos) MarkOutboxPublished(ctx context.Context, eventID uuid.UUID) error {
	_, err := r.Pool.Exec(ctx, `UPDATE outbox_events SET published_at = NOW(), locked_until = NULL WHERE event_id = $1`, eventID)
	return err
}

func (r *Repos) ReleaseOutboxClaim(ctx context.Context, eventID uuid.UUID) error {
	_, err := r.Pool.Exec(ctx, `UPDATE outbox_events SET locked_until = NULL, locked_by = NULL WHERE event_id = $1`, eventID)
	return err
}

func (r *Repos) ScheduleOutboxRetry(ctx context.Context, eventID uuid.UUID, next time.Time) error {
	_, err := r.Pool.Exec(ctx, `UPDATE outbox_events SET next_attempt_at = $2, locked_until = NULL WHERE event_id = $1`, eventID, next)
	return err
}

func (r *Repos) CountPendingOutbox(ctx context.Context) (int64, error) {
	var n int64
	err := r.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&n)
	return n, err
}
