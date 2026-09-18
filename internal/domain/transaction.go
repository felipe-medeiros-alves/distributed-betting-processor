package domain

import (
	"time"

	"github.com/google/uuid"
)

type TransactionOrigin string

const (
	OriginInternal TransactionOrigin = "INTERNAL"
	OriginExternal TransactionOrigin = "EXTERNAL"
)

type TransactionKind string

const (
	KindOpening  TransactionKind = "OPENING"
	KindBet      TransactionKind = "BET"
	KindWin      TransactionKind = "WIN"
	KindLoss     TransactionKind = "LOSS"
	KindRefund   TransactionKind = "REFUND"
	KindRollback TransactionKind = "ROLLBACK"
)

type TransactionStatus string

const (
	StatusPending           TransactionStatus = "PENDING"
	StatusPendingReference  TransactionStatus = "PENDING_REFERENCE"
	StatusProcessed         TransactionStatus = "PROCESSED"
	StatusRejected          TransactionStatus = "REJECTED"
	StatusFailed            TransactionStatus = "FAILED"
)

type LedgerDirection string

const (
	DirectionDebit  LedgerDirection = "DEBIT"
	DirectionCredit LedgerDirection = "CREDIT"
)

func (s TransactionStatus) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

type WagerTransaction struct {
	id                            uuid.UUID
	origin                        TransactionOrigin
	kind                          TransactionKind
	status                        TransactionStatus
	walletID                      uuid.UUID
	playerID                      uuid.UUID
	providerID                    string
	externalID                    string
	idempotencyKey                string
	payloadHash                   string
	roundID                       string
	gameID                        string
	refExternalID                 string
	refTransactionID              *uuid.UUID
	money                         Money
	failureCode                   FailureCode
	observedBalance               *Money
	referenceAttempts             int
	nextReferenceAttemptAt        *time.Time
	createdAt                     time.Time
	updatedAt                     time.Time
	processedAt                   *time.Time
}

func NewOpeningTransaction(walletID, playerID uuid.UUID, amount Money) (*WagerTransaction, error) {
	if amount.IsNegative() {
		return nil, ErrInvalidMoney
	}
	now := time.Now().UTC()
	return &WagerTransaction{
		id:         uuid.Must(uuid.NewV7()),
		origin:     OriginInternal,
		kind:       KindOpening,
		status:     StatusProcessed,
		walletID:   walletID,
		playerID:   playerID,
		money:      amount,
		createdAt:  now,
		updatedAt:  now,
		processedAt: &now,
	}, nil
}

func NewExternalTransaction(
	kind TransactionKind,
	walletID, playerID uuid.UUID,
	providerID, externalID, idempotencyKey, payloadHash, roundID, gameID string,
	money Money,
	refExternalID string,
) (*WagerTransaction, error) {
	if kind == KindOpening {
		return nil, BusinessRejection{Code: FailureOpeningNotAllowed, Err: ErrInvalidKind}
	}
	now := time.Now().UTC()
	return &WagerTransaction{
		id:             uuid.Must(uuid.NewV7()),
		origin:         OriginExternal,
		kind:           kind,
		status:         StatusPending,
		walletID:       walletID,
		playerID:       playerID,
		providerID:     providerID,
		externalID:     externalID,
		idempotencyKey: idempotencyKey,
		payloadHash:    payloadHash,
		roundID:        roundID,
		gameID:         gameID,
		refExternalID:  refExternalID,
		money:          money,
		createdAt:      now,
		updatedAt:      now,
	}, nil
}

func RehydrateWagerTransaction(
	id uuid.UUID,
	origin TransactionOrigin,
	kind TransactionKind,
	status TransactionStatus,
	walletID, playerID uuid.UUID,
	providerID, externalID, idempotencyKey, payloadHash, roundID, gameID, refExternalID string,
	refTransactionID *uuid.UUID,
	money Money,
	failureCode FailureCode,
	observedBalance *Money,
	referenceAttempts int,
	nextReferenceAttemptAt *time.Time,
	createdAt, updatedAt time.Time,
	processedAt *time.Time,
) *WagerTransaction {
	return &WagerTransaction{
		id:                     id,
		origin:                 origin,
		kind:                   kind,
		status:                 status,
		walletID:               walletID,
		playerID:               playerID,
		providerID:             providerID,
		externalID:             externalID,
		idempotencyKey:         idempotencyKey,
		payloadHash:            payloadHash,
		roundID:                roundID,
		gameID:                 gameID,
		refExternalID:          refExternalID,
		refTransactionID:       refTransactionID,
		money:                  money,
		failureCode:            failureCode,
		observedBalance:        observedBalance,
		referenceAttempts:      referenceAttempts,
		nextReferenceAttemptAt: nextReferenceAttemptAt,
		createdAt:              createdAt,
		updatedAt:              updatedAt,
		processedAt:            processedAt,
	}
}

func (t *WagerTransaction) ID() uuid.UUID                    { return t.id }
func (t *WagerTransaction) Origin() TransactionOrigin        { return t.origin }
func (t *WagerTransaction) Kind() TransactionKind            { return t.kind }
func (t *WagerTransaction) Status() TransactionStatus          { return t.status }
func (t *WagerTransaction) WalletID() uuid.UUID                { return t.walletID }
func (t *WagerTransaction) PlayerID() uuid.UUID                { return t.playerID }
func (t *WagerTransaction) ProviderID() string                 { return t.providerID }
func (t *WagerTransaction) ExternalID() string                 { return t.externalID }
func (t *WagerTransaction) IdempotencyKey() string             { return t.idempotencyKey }
func (t *WagerTransaction) PayloadHash() string                { return t.payloadHash }
func (t *WagerTransaction) RoundID() string                    { return t.roundID }
func (t *WagerTransaction) GameID() string                     { return t.gameID }
func (t *WagerTransaction) RefExternalID() string              { return t.refExternalID }
func (t *WagerTransaction) RefTransactionID() *uuid.UUID       { return t.refTransactionID }
func (t *WagerTransaction) Money() Money                       { return t.money }
func (t *WagerTransaction) FailureCode() FailureCode           { return t.failureCode }
func (t *WagerTransaction) ObservedBalance() *Money            { return t.observedBalance }
func (t *WagerTransaction) ReferenceAttempts() int             { return t.referenceAttempts }
func (t *WagerTransaction) NextReferenceAttemptAt() *time.Time  { return t.nextReferenceAttemptAt }
func (t *WagerTransaction) CreatedAt() time.Time               { return t.createdAt }
func (t *WagerTransaction) UpdatedAt() time.Time               { return t.updatedAt }
func (t *WagerTransaction) ProcessedAt() *time.Time            { return t.processedAt }

func (t *WagerTransaction) SetRefTransactionID(id uuid.UUID) {
	t.refTransactionID = &id
}

func (t *WagerTransaction) MarkPendingReference(nextAttempt time.Time) error {
	if t.status.IsTerminal() {
		return ErrTerminalState
	}
	if t.status != StatusPending && t.status != StatusPendingReference {
		return ErrInvalidTransition
	}
	t.status = StatusPendingReference
	t.nextReferenceAttemptAt = &nextAttempt
	t.updatedAt = time.Now().UTC()
	return nil
}

func (t *WagerTransaction) ScheduleReferenceRetry(nextAttempt time.Time) error {
	if t.status != StatusPendingReference {
		return ErrInvalidTransition
	}
	t.referenceAttempts++
	t.nextReferenceAttemptAt = &nextAttempt
	t.updatedAt = time.Now().UTC()
	return nil
}

func (t *WagerTransaction) MarkProcessed(observed Money) error {
	if t.status.IsTerminal() {
		return ErrTerminalState
	}
	now := time.Now().UTC()
	t.status = StatusProcessed
	t.observedBalance = &observed
	t.processedAt = &now
	t.updatedAt = now
	return nil
}

func (t *WagerTransaction) MarkRejected(code FailureCode, observed *Money) error {
	if t.status.IsTerminal() {
		return ErrTerminalState
	}
	now := time.Now().UTC()
	t.status = StatusRejected
	t.failureCode = code
	t.observedBalance = observed
	t.processedAt = &now
	t.updatedAt = now
	return nil
}

func (t *WagerTransaction) MarkFailed() error {
	if t.status.IsTerminal() {
		return ErrTerminalState
	}
	now := time.Now().UTC()
	t.status = StatusFailed
	t.processedAt = &now
	t.updatedAt = now
	return nil
}

type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     LedgerDirection
	amount        Money
	balanceBefore Money
	balanceAfter  Money
	createdAt     time.Time
}

func NewLedgerEntry(walletID, transactionID uuid.UUID, direction LedgerDirection, amount, before, after Money) (*LedgerEntry, error) {
	if !amount.IsPositive() {
		return nil, ErrInvalidMoney
	}
	if amount.Currency() != before.Currency() || amount.Currency() != after.Currency() {
		return nil, ErrCurrencyMismatch
	}
	switch direction {
	case DirectionDebit:
		expected, err := before.Sub(amount)
		if err != nil || !expected.Equal(after) {
			return nil, ErrInvalidMoney
		}
	case DirectionCredit:
		expected, err := before.Add(amount)
		if err != nil || !expected.Equal(after) {
			return nil, ErrInvalidMoney
		}
	default:
		return nil, ErrInvalidMoney
	}
	return &LedgerEntry{
		id:            uuid.Must(uuid.NewV7()),
		walletID:      walletID,
		transactionID: transactionID,
		direction:     direction,
		amount:        amount,
		balanceBefore: before,
		balanceAfter:  after,
		createdAt:     time.Now().UTC(),
	}, nil
}

func RehydrateLedgerEntry(id, walletID, transactionID uuid.UUID, direction LedgerDirection, amount, before, after Money, createdAt time.Time) *LedgerEntry {
	return &LedgerEntry{
		id:            id,
		walletID:      walletID,
		transactionID: transactionID,
		direction:     direction,
		amount:        amount,
		balanceBefore: before,
		balanceAfter:  after,
		createdAt:     createdAt,
	}
}

func (e *LedgerEntry) ID() uuid.UUID            { return e.id }
func (e *LedgerEntry) WalletID() uuid.UUID      { return e.walletID }
func (e *LedgerEntry) TransactionID() uuid.UUID { return e.transactionID }
func (e *LedgerEntry) Direction() LedgerDirection { return e.direction }
func (e *LedgerEntry) Amount() Money            { return e.amount }
func (e *LedgerEntry) BalanceBefore() Money     { return e.balanceBefore }
func (e *LedgerEntry) BalanceAfter() Money      { return e.balanceAfter }
func (e *LedgerEntry) CreatedAt() time.Time     { return e.createdAt }
