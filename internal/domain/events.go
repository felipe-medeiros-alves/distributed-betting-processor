package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const EventVersion = 1

type OutboxEvent struct {
	EventID       uuid.UUID
	AggregateID   uuid.UUID
	EventType     string
	Payload       json.RawMessage
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
	Version       int
}

const (
	EventWagerProcessed        = "WagerTransactionProcessed"
	EventWagerRejected         = "WagerTransactionRejected"
	EventWagerPendingReference = "WagerTransactionPendingReference"
	EventWalletBalanceChanged  = "WalletBalanceChanged"
)

type EventEnvelope struct {
	EventID       uuid.UUID       `json:"eventId"`
	EventType     string          `json:"eventType"`
	AggregateID   uuid.UUID       `json:"aggregateId"`
	CorrelationID string          `json:"correlationId"`
	CausationID   string          `json:"causationId,omitempty"`
	OccurredAt    string          `json:"occurredAt"`
	Version       int             `json:"version"`
	Data          json.RawMessage `json:"data"`
}

type WalletBalanceChangedData struct {
	WalletID      uuid.UUID `json:"walletId"`
	TransactionID uuid.UUID `json:"transactionId"`
	Direction     string    `json:"direction"`
	Money         MoneyDTO  `json:"money"`
	BalanceBefore MoneyDTO  `json:"balanceBefore"`
	BalanceAfter  MoneyDTO  `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

type WagerProcessedData struct {
	TransactionID uuid.UUID `json:"transactionId"`
	WalletID      uuid.UUID `json:"walletId"`
	ProviderID    string    `json:"providerId,omitempty"`
	ExternalID    string    `json:"externalTransactionId,omitempty"`
	Kind          string    `json:"kind"`
	Status        string    `json:"status"`
	Balance       *MoneyDTO `json:"balance,omitempty"`
}

type WagerRejectedData struct {
	TransactionID uuid.UUID `json:"transactionId"`
	WalletID      uuid.UUID `json:"walletId"`
	ProviderID    string    `json:"providerId,omitempty"`
	ExternalID    string    `json:"externalTransactionId,omitempty"`
	Kind          string    `json:"kind"`
	FailureCode   string    `json:"failureCode"`
}

type MoneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func MoneyToDTO(m Money) MoneyDTO {
	return MoneyDTO{Amount: m.FormatAmount(), Currency: m.Currency()}
}

func NewOutboxEvent(eventType string, aggregateID uuid.UUID, correlationID string, data any) (OutboxEvent, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return OutboxEvent{}, err
	}
	return OutboxEvent{
		EventID:       uuid.Must(uuid.NewV7()),
		AggregateID:   aggregateID,
		EventType:     eventType,
		Payload:       raw,
		CorrelationID: correlationID,
		OccurredAt:    time.Now().UTC(),
		Version:       EventVersion,
	}, nil
}
