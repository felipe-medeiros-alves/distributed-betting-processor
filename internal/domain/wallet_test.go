package domain

import (
	"testing"

	"github.com/google/uuid"
)

func TestWalletDebitInsufficient(t *testing.T) {
	w, err := NewWallet(uuid.Must(uuid.NewV7()), MustMoney("100.00", "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = w.Debit(MustMoney("100.01", "BRL"))
	if err == nil {
		t.Fatal("expected rejection")
	}
	br, ok := IsBusinessRejection(err)
	if !ok || br.Code != FailureInsufficientFunds {
		t.Fatalf("got %v", err)
	}
}

func TestWalletDebitSuccess(t *testing.T) {
	w, _ := NewWallet(uuid.Must(uuid.NewV7()), MustMoney("100.00", "BRL"))
	before, after, err := w.Debit(MustMoney("20.00", "BRL"))
	if err != nil {
		t.Fatal(err)
	}
	if !before.Equal(MustMoney("100.00", "BRL")) || !after.Equal(MustMoney("80.00", "BRL")) {
		t.Fatalf("balances unexpected")
	}
	if w.Version() != 2 {
		t.Fatalf("version %d", w.Version())
	}
}
