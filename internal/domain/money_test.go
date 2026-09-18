package domain

import (
	"testing"
)

func TestParseMoneyValid(t *testing.T) {
	m, err := ParseMoney("25.00", "BRL")
	if err != nil {
		t.Fatal(err)
	}
	if m.Minor() != 2500 || m.Currency() != "BRL" {
		t.Fatalf("got %+v", m)
	}
}

func TestParseMoneyRejectsScientific(t *testing.T) {
	_, err := ParseMoney("1e2", "BRL")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseMoneyRejectsNegativeExternal(t *testing.T) {
	_, err := ParseMoney("-1.00", "BRL")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestMoneyCurrencyMismatch(t *testing.T) {
	a := MustMoney("1.00", "BRL")
	b := MustMoney("1.00", "USD")
	_, err := a.Add(b)
	if err != ErrCurrencyMismatch {
		t.Fatalf("got %v", err)
	}
}

func TestMoneyOverflow(t *testing.T) {
	huge := MoneyFromMinor(9223372036854775800, "BRL")
	_, err := huge.Add(MoneyFromMinor(100, "BRL"))
	if err != ErrOverflow {
		t.Fatalf("got %v", err)
	}
}
