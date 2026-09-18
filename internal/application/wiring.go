package application

import (
	"github.com/felipemalves/distributed-betting-processor/internal/postgres"
)

func PostgresReposFrom(r *postgres.Repos) postgresRepos {
	return postgresRepos{
		InsertWallet:               r.InsertWallet,
		GetWalletByIDForUpdate:     r.GetWalletByIDForUpdate,
		GetWalletByID:              r.GetWalletByID,
		UpdateWallet:               r.UpdateWallet,
		FindWalletByPlayerCurrency: r.FindWalletByPlayerCurrency,
		InsertWager:                r.InsertWager,
		InsertLedger:               r.InsertLedger,
		InsertOutbox:               r.InsertOutbox,
		ListLedger:                 r.ListLedger,
		CountLedger:                r.CountLedger,
		SumCreditsDebits:           r.SumCreditsDebits,
	}
}

func WagerReposFrom(r *postgres.Repos) wagerRepos {
	return wagerRepos{
		postgresRepos:              PostgresReposFrom(r),
		GetWagerByID:               r.GetWagerByID,
		GetWagerByProviderExternal: r.GetWagerByProviderExternal,
		GetWagerByIdempotencyKey:   r.GetWagerByIdempotencyKey,
		UpdateWager:                r.UpdateWager,
		FindSuccessfulReversal:     r.FindSuccessfulReversal,
		ListPendingReferenceDue:    r.ListPendingReferenceDue,
		TryInsertInbox:             r.TryInsertInbox,
		CompleteInbox:              r.CompleteInbox,
	}
}
