package domain

import (
	"testing"

	"github.com/google/uuid"
)

func TestTransactionTerminalImmutable(t *testing.T) {
	tx, _ := NewExternalTransaction(
		KindBet, uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()),
		"p", "ext", "key", "hash", "r", "g", MustMoney("1.00", "BRL"), "",
	)
	_ = tx.MarkRejected(FailureInsufficientFunds, nil)
	if err := tx.MarkProcessed(MustMoney("0.00", "BRL")); err != ErrTerminalState {
		t.Fatalf("got %v", err)
	}
}

func TestOpeningRejectedForExternal(t *testing.T) {
	_, err := NewExternalTransaction(
		KindOpening, uuid.Must(uuid.NewV7()), uuid.Must(uuid.NewV7()),
		"p", "ext", "key", "hash", "r", "g", MustMoney("1.00", "BRL"), "",
	)
	if err == nil {
		t.Fatal("expected error")
	}
}
