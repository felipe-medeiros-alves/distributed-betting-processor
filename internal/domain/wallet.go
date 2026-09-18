package domain

import (
	"time"

	"github.com/google/uuid"
)

type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	currency  string
	balance   Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

func NewWallet(playerID uuid.UUID, initial Money) (*Wallet, error) {
	if initial.IsNegative() {
		return nil, ErrInvalidMoney
	}
	now := time.Now().UTC()
	return &Wallet{
		id:        uuid.Must(uuid.NewV7()),
		playerID:  playerID,
		currency:  initial.Currency(),
		balance:   initial,
		version:   1,
		createdAt: now,
		updatedAt: now,
	}, nil
}

func RehydrateWallet(id, playerID uuid.UUID, balance Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	if version < 1 {
		return nil, ErrInvalidMoney
	}
	if balance.IsNegative() {
		return nil, ErrInvalidMoney
	}
	return &Wallet{
		id:        id,
		playerID:  playerID,
		currency:  balance.Currency(),
		balance:   balance,
		version:   version,
		createdAt: createdAt,
		updatedAt: updatedAt,
	}, nil
}

func (w *Wallet) ID() uuid.UUID          { return w.id }
func (w *Wallet) PlayerID() uuid.UUID    { return w.playerID }
func (w *Wallet) Currency() string       { return w.currency }
func (w *Wallet) Balance() Money         { return w.balance }
func (w *Wallet) Version() int64         { return w.version }
func (w *Wallet) CreatedAt() time.Time   { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time   { return w.updatedAt }

func (w *Wallet) Debit(amount Money) (Money, Money, error) {
	if amount.Currency() != w.currency {
		return Money{}, Money{}, ErrCurrencyMismatch
	}
	if !amount.IsPositive() {
		return Money{}, Money{}, ErrInvalidMoney
	}
	before := w.balance
	after, err := before.Sub(amount)
	if err != nil {
		return Money{}, Money{}, err
	}
	if after.IsNegative() {
		return Money{}, Money{}, BusinessRejection{Code: FailureInsufficientFunds, Err: ErrInsufficientFunds}
	}
	w.balance = after
	w.version++
	w.updatedAt = time.Now().UTC()
	return before, after, nil
}

func (w *Wallet) Credit(amount Money) (Money, Money, error) {
	if amount.Currency() != w.currency {
		return Money{}, Money{}, ErrCurrencyMismatch
	}
	if !amount.IsPositive() {
		return Money{}, Money{}, ErrInvalidMoney
	}
	before := w.balance
	after, err := before.Add(amount)
	if err != nil {
		return Money{}, Money{}, err
	}
	w.balance = after
	w.version++
	w.updatedAt = time.Now().UTC()
	return before, after, nil
}
