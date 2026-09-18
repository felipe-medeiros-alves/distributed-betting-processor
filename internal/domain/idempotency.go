package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
)

type WagerPayloadFields struct {
	ProviderID                    string `json:"providerId"`
	ExternalTransactionID         string `json:"externalTransactionId"`
	PlayerID                      string `json:"playerId"`
	WalletID                      string `json:"walletId"`
	RoundID                       string `json:"roundId"`
	GameID                        string `json:"gameId"`
	Kind                          string `json:"kind"`
	MoneyAmount                   string `json:"moneyAmount"`
	MoneyCurrency                 string `json:"moneyCurrency"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

func HashWagerPayload(f WagerPayloadFields) string {
	m := map[string]string{
		"externalTransactionId": f.ExternalTransactionID,
		"gameId":                f.GameID,
		"kind":                  f.Kind,
		"moneyAmount":           f.MoneyAmount,
		"moneyCurrency":         f.MoneyCurrency,
		"playerId":              f.PlayerID,
		"providerId":            f.ProviderID,
		"roundId":               f.RoundID,
		"walletId":              f.WalletID,
	}
	if f.ReferenceExternalTransactionID != "" {
		m["referenceExternalTransactionId"] = f.ReferenceExternalTransactionID
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make(map[string]string, len(keys))
	for _, k := range keys {
		ordered[k] = m[k]
	}
	b, _ := json.Marshal(ordered)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
