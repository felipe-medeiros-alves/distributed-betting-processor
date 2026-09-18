package domain

import "testing"

func TestHashWagerPayloadStable(t *testing.T) {
	f := WagerPayloadFields{
		ProviderID: "provider-a", ExternalTransactionID: "tx-1",
		PlayerID: "p", WalletID: "w", RoundID: "r", GameID: "g",
		Kind: "BET", MoneyAmount: "1.00", MoneyCurrency: "BRL",
	}
	h1 := HashWagerPayload(f)
	h2 := HashWagerPayload(f)
	if h1 != h2 || len(h1) != 64 {
		t.Fatalf("hash %s", h1)
	}
}
