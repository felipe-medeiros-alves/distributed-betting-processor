package application

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	"github.com/felipemalves/distributed-betting-processor/internal/domain"
	"github.com/felipemalves/distributed-betting-processor/internal/observe"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type WagerService struct {
	uow            *postgresUnitOfWork
	repo           *wagerRepos
	log            *slog.Logger
	met            *observe.Metrics
	refMaxTries    int
	refTTL         time.Duration
}

type wagerRepos struct {
	postgresRepos
	GetWagerByID              func(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	GetWagerByProviderExternal func(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error)
	GetWagerByIdempotencyKey    func(ctx context.Context, key string) (*domain.WagerTransaction, error)
	UpdateWager                 func(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction) error
	FindSuccessfulReversal      func(ctx context.Context, tx pgx.Tx, providerID, refExternalID string, kind domain.TransactionKind) (*domain.WagerTransaction, error)
	ListPendingReferenceDue     func(ctx context.Context, tx pgx.Tx, limit int) ([]*domain.WagerTransaction, error)
	TryInsertInbox              func(ctx context.Context, tx pgx.Tx, consumerName, messageID, payloadHash string) (bool, error)
	CompleteInbox               func(ctx context.Context, tx pgx.Tx, consumerName, messageID string) error
}

func NewWagerService(
	uowFn func(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error,
	repos wagerRepos,
	log *slog.Logger,
	met *observe.Metrics,
	refMaxTries int,
	refTTL time.Duration,
) *WagerService {
	return &WagerService{
		uow:         &postgresUnitOfWork{Within: uowFn},
		repo:        &repos,
		log:         log,
		met:         met,
		refMaxTries: refMaxTries,
		refTTL:      refTTL,
	}
}

type SubmitWagerInput struct {
	ProviderID                    string
	ExternalTransactionID         string
	IdempotencyKey                string
	PlayerID                      uuid.UUID
	WalletID                      uuid.UUID
	RoundID                       string
	GameID                        string
	Kind                          domain.TransactionKind
	Money                         domain.Money
	ReferenceExternalTransactionID string
	PayloadHash                   string
}

type SubmitWagerResult struct {
	TransactionID    uuid.UUID
	Status           domain.TransactionStatus
	Balance          *domain.Money
	IdempotentReplay bool
	FailureCode      domain.FailureCode
}

func (s *WagerService) Submit(ctx context.Context, in SubmitWagerInput) (*SubmitWagerResult, error) {
	if replay, err := s.checkIdempotency(ctx, in); replay != nil || err != nil {
		return replay, err
	}
	var result *SubmitWagerResult
	err := s.uow.Within(ctx, func(ctx context.Context, tx pgx.Tx) error {
		w, err := s.repo.GetWalletByIDForUpdate(ctx, tx, in.WalletID)
		if err != nil {
			return err
		}
		if w.PlayerID() != in.PlayerID {
			return domain.BusinessRejection{Code: domain.FailureInvalidReference, Err: domain.ErrUnauthorized}
		}
		if w.Currency() != in.Money.Currency() {
			return domain.BusinessRejection{Code: domain.FailureInvalidAmount, Err: domain.ErrCurrencyMismatch}
		}
		txn, err := domain.NewExternalTransaction(
			in.Kind, in.WalletID, in.PlayerID, in.ProviderID, in.ExternalTransactionID,
			in.IdempotencyKey, in.PayloadHash, in.RoundID, in.GameID, in.Money, in.ReferenceExternalTransactionID,
		)
		if err != nil {
			return err
		}
		if err := s.validateKindAmount(in.Kind, in.Money); err != nil {
			return s.rejectInTx(ctx, tx, txn, w, err, &result)
		}
		if in.Kind == domain.KindRefund || in.Kind == domain.KindRollback {
			if in.ReferenceExternalTransactionID == "" {
				return s.rejectInTx(ctx, tx, txn, w, domain.BusinessRejection{Code: domain.FailureInvalidReference, Err: domain.ErrInvalidMoney}, &result)
			}
			ref, pending, err := s.resolveReference(ctx, tx, in.ProviderID, in.ReferenceExternalTransactionID)
			if pending {
				next := time.Now().UTC().Add(time.Second)
				_ = txn.MarkPendingReference(next)
				if err := s.repo.InsertWager(ctx, tx, txn); err != nil {
					return err
				}
				if err := s.emitPendingReference(ctx, tx, txn); err != nil {
					return err
				}
				result = &SubmitWagerResult{TransactionID: txn.ID(), Status: domain.StatusPendingReference}
				return nil
			}
			if err != nil {
				return s.rejectInTx(ctx, tx, txn, w, err, &result)
			}
			if err := s.validateReferenceMatch(in, ref); err != nil {
				return s.rejectInTx(ctx, tx, txn, w, err, &result)
			}
			if err := s.validateReversalRules(ctx, tx, in, ref); err != nil {
				return s.rejectInTx(ctx, tx, txn, w, err, &result)
			}
			txn.SetRefTransactionID(ref.ID())
		}
		if err := s.repo.InsertWager(ctx, tx, txn); err != nil {
			return err
		}
		res, err := s.applyTransaction(ctx, tx, w, txn)
		if err != nil {
			if br, ok := domain.IsBusinessRejection(err); ok {
				if err := s.rejectPersisted(ctx, tx, txn, w, br); err != nil {
					return err
				}
				result = rejectionResult(txn, w, br)
				return nil
			}
			return err
		}
		result = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func rejectionResult(txn *domain.WagerTransaction, w *domain.Wallet, br domain.BusinessRejection) *SubmitWagerResult {
	bal := w.Balance()
	return &SubmitWagerResult{
		TransactionID: txn.ID(),
		Status:        domain.StatusRejected,
		FailureCode:   br.Code,
		Balance:       &bal,
	}
}

func (s *WagerService) checkIdempotency(ctx context.Context, in SubmitWagerInput) (*SubmitWagerResult, error) {
	byKey, errKey := s.repo.GetWagerByIdempotencyKey(ctx, in.IdempotencyKey)
	if errKey != nil && !errors.Is(errKey, domain.ErrNotFound) {
		return nil, errKey
	}
	if byKey != nil {
		if byKey.PayloadHash() != in.PayloadHash {
			return nil, domain.ErrIdempotencyConflict
		}
		s.met.IdempotentReplays.Inc()
		return s.replayResult(byKey), nil
	}
	byExt, errExt := s.repo.GetWagerByProviderExternal(ctx, in.ProviderID, in.ExternalTransactionID)
	if errExt != nil && !errors.Is(errExt, domain.ErrNotFound) {
		return nil, errExt
	}
	if byExt != nil {
		if byExt.IdempotencyKey() != in.IdempotencyKey {
			return nil, domain.ErrIdempotencyConflict
		}
		s.met.IdempotentReplays.Inc()
		return s.replayResult(byExt), nil
	}
	return nil, nil
}

func (s *WagerService) replayResult(t *domain.WagerTransaction) *SubmitWagerResult {
	res := &SubmitWagerResult{
		TransactionID:    t.ID(),
		Status:           t.Status(),
		IdempotentReplay: true,
		FailureCode:      t.FailureCode(),
	}
	if t.ObservedBalance() != nil {
		b := *t.ObservedBalance()
		res.Balance = &b
	}
	return res
}

func (s *WagerService) validateKindAmount(kind domain.TransactionKind, money domain.Money) error {
	switch kind {
	case domain.KindLoss:
		if !money.IsZero() {
			return domain.BusinessRejection{Code: domain.FailureInvalidAmount, Err: domain.ErrInvalidMoney}
		}
	case domain.KindBet, domain.KindWin, domain.KindRefund, domain.KindRollback:
		if !money.IsPositive() {
			return domain.BusinessRejection{Code: domain.FailureInvalidAmount, Err: domain.ErrInvalidMoney}
		}
	}
	return nil
}

func (s *WagerService) resolveReference(ctx context.Context, tx pgx.Tx, providerID, refExternalID string) (*domain.WagerTransaction, bool, error) {
	ref, err := s.getReference(ctx, providerID, refExternalID)
	if errors.Is(err, domain.ErrNotFound) {
		return nil, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	switch ref.Status() {
	case domain.StatusPending, domain.StatusPendingReference:
		return nil, true, nil
	case domain.StatusRejected, domain.StatusFailed:
		return nil, false, domain.BusinessRejection{Code: domain.FailureReferenceNotSuccessful, Err: domain.ErrReferenceNotReady}
	case domain.StatusProcessed:
		return ref, false, nil
	default:
		return nil, false, domain.ErrInvalidTransition
	}
}

func (s *WagerService) getReference(ctx context.Context, providerID, refExternalID string) (*domain.WagerTransaction, error) {
	return s.repo.GetWagerByProviderExternal(ctx, providerID, refExternalID)
}

func (s *WagerService) validateReferenceMatch(in SubmitWagerInput, ref *domain.WagerTransaction) error {
	if ref.ProviderID() != in.ProviderID || ref.PlayerID() != in.PlayerID || ref.WalletID() != in.WalletID {
		return domain.BusinessRejection{Code: domain.FailureInvalidReference, Err: domain.ErrInvalidMoney}
	}
	if ref.RoundID() != in.RoundID {
		return domain.BusinessRejection{Code: domain.FailureInvalidReference, Err: domain.ErrInvalidMoney}
	}
	if !ref.Money().Equal(in.Money) {
		return domain.BusinessRejection{Code: domain.FailureInvalidAmount, Err: domain.ErrInvalidMoney}
	}
	switch in.Kind {
	case domain.KindRefund:
		if ref.Kind() != domain.KindBet {
			return domain.BusinessRejection{Code: domain.FailureInvalidReference, Err: domain.ErrInvalidKind}
		}
	case domain.KindRollback:
		if ref.Kind() != domain.KindBet && ref.Kind() != domain.KindWin && ref.Kind() != domain.KindRefund {
			return domain.BusinessRejection{Code: domain.FailureInvalidReference, Err: domain.ErrInvalidKind}
		}
	}
	return nil
}

func (s *WagerService) validateReversalRules(ctx context.Context, tx pgx.Tx, in SubmitWagerInput, ref *domain.WagerTransaction) error {
	sameType, err := s.repo.FindSuccessfulReversal(ctx, tx, in.ProviderID, in.ReferenceExternalTransactionID, in.Kind)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return err
	}
	if sameType != nil {
		return domain.BusinessRejection{Code: domain.FailureReferenceAlreadyReversed, Err: domain.ErrReferenceReversed}
	}
	if ref.Kind() == domain.KindBet {
		for _, k := range []domain.TransactionKind{domain.KindRefund, domain.KindRollback} {
			other, err := s.repo.FindSuccessfulReversal(ctx, tx, in.ProviderID, in.ReferenceExternalTransactionID, k)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return err
			}
			if other != nil {
				return domain.BusinessRejection{Code: domain.FailureReferenceAlreadyReversed, Err: domain.ErrReferenceReversed}
			}
		}
	}
	return nil
}

func (s *WagerService) applyTransaction(ctx context.Context, tx pgx.Tx, w *domain.Wallet, txn *domain.WagerTransaction) (*SubmitWagerResult, error) {
	expectedVersion := w.Version()
	var observed domain.Money
	switch txn.Kind() {
	case domain.KindLoss:
		observed = w.Balance()
		if err := txn.MarkProcessed(observed); err != nil {
			return nil, err
		}
		if err := s.repo.UpdateWager(ctx, tx, txn); err != nil {
			return nil, err
		}
		if err := s.emitProcessed(ctx, tx, txn, w); err != nil {
			return nil, err
		}
		s.met.TransactionsTotal.WithLabelValues(string(domain.StatusProcessed)).Inc()
		return &SubmitWagerResult{TransactionID: txn.ID(), Status: domain.StatusProcessed, Balance: &observed}, nil
	case domain.KindBet:
		before, after, err := w.Debit(txn.Money())
		if err != nil {
			return nil, err
		}
		return s.finishWithLedger(ctx, tx, w, txn, expectedVersion, domain.DirectionDebit, txn.Money(), before, after)
	case domain.KindWin, domain.KindRefund:
		before, after, err := w.Credit(txn.Money())
		if err != nil {
			return nil, err
		}
		return s.finishWithLedger(ctx, tx, w, txn, expectedVersion, domain.DirectionCredit, txn.Money(), before, after)
	case domain.KindRollback:
		refKind := domain.KindBet
		if txn.RefTransactionID() != nil {
			ref, _ := s.repo.GetWagerByID(ctx, *txn.RefTransactionID())
			if ref != nil {
				refKind = ref.Kind()
			}
		}
		switch refKind {
		case domain.KindBet, domain.KindRefund:
			before, after, err := w.Credit(txn.Money())
			if err != nil {
				return nil, err
			}
			return s.finishWithLedger(ctx, tx, w, txn, expectedVersion, domain.DirectionCredit, txn.Money(), before, after)
		case domain.KindWin:
			before, after, err := w.Debit(txn.Money())
			if err != nil {
				if br, ok := domain.IsBusinessRejection(err); ok {
					return nil, domain.BusinessRejection{Code: domain.FailureInsufficientFundsReversal, Err: br.Err}
				}
				return nil, err
			}
			return s.finishWithLedger(ctx, tx, w, txn, expectedVersion, domain.DirectionDebit, txn.Money(), before, after)
		}
	}
	return nil, domain.ErrInvalidKind
}

func (s *WagerService) finishWithLedger(ctx context.Context, tx pgx.Tx, w *domain.Wallet, txn *domain.WagerTransaction, expectedVersion int64, dir domain.LedgerDirection, amount, before, after domain.Money) (*SubmitWagerResult, error) {
	entry, err := domain.NewLedgerEntry(w.ID(), txn.ID(), dir, amount, before, after)
	if err != nil {
		return nil, err
	}
	if err := s.repo.UpdateWallet(ctx, tx, w, expectedVersion); err != nil {
		s.met.ConcurrencyConflicts.Inc()
		return nil, err
	}
	if err := s.repo.InsertLedger(ctx, tx, entry); err != nil {
		return nil, err
	}
	observed := after
	if err := txn.MarkProcessed(observed); err != nil {
		return nil, err
	}
	if err := s.repo.UpdateWager(ctx, tx, txn); err != nil {
		return nil, err
	}
	ws := &WalletService{uow: s.uow, repo: &s.repo.postgresRepos, log: s.log, met: s.met}
	if err := ws.emitBalanceChanged(ctx, tx, w, txn.ID(), dir, amount, before, after); err != nil {
		return nil, err
	}
	if err := s.emitProcessed(ctx, tx, txn, w); err != nil {
		return nil, err
	}
	s.met.TransactionsTotal.WithLabelValues(string(domain.StatusProcessed)).Inc()
	return &SubmitWagerResult{TransactionID: txn.ID(), Status: domain.StatusProcessed, Balance: &observed}, nil
}

func (s *WagerService) rejectInTx(ctx context.Context, tx pgx.Tx, txn *domain.WagerTransaction, w *domain.Wallet, bizErr error, result **SubmitWagerResult) error {
	br := domain.AsBusinessRejection(bizErr)
	if err := s.rejectExisting(ctx, tx, txn, w, br); err != nil {
		return err
	}
	*result = rejectionResult(txn, w, br)
	return nil
}

func (s *WagerService) rejectExisting(ctx context.Context, tx pgx.Tx, txn *domain.WagerTransaction, w *domain.Wallet, br domain.BusinessRejection) error {
	bal := w.Balance()
	if err := txn.MarkRejected(br.Code, &bal); err != nil {
		return err
	}
	if err := s.repo.InsertWager(ctx, tx, txn); err != nil {
		return err
	}
	ev, err := domain.NewOutboxEvent(domain.EventWagerRejected, txn.ID(), txn.ID().String(), domain.WagerRejectedData{
		TransactionID: txn.ID(),
		WalletID:      w.ID(),
		ProviderID:    txn.ProviderID(),
		ExternalID:    txn.ExternalID(),
		Kind:          string(txn.Kind()),
		FailureCode:   string(br.Code),
	})
	if err != nil {
		return err
	}
	if err := s.repo.InsertOutbox(ctx, tx, ev); err != nil {
		return err
	}
	s.met.TransactionsTotal.WithLabelValues(string(domain.StatusRejected)).Inc()
	return nil
}

func (s *WagerService) rejectPersisted(ctx context.Context, tx pgx.Tx, txn *domain.WagerTransaction, w *domain.Wallet, br domain.BusinessRejection) error {
	bal := w.Balance()
	if err := txn.MarkRejected(br.Code, &bal); err != nil {
		return err
	}
	if err := s.repo.UpdateWager(ctx, tx, txn); err != nil {
		return err
	}
	ev, err := domain.NewOutboxEvent(domain.EventWagerRejected, txn.ID(), txn.ID().String(), domain.WagerRejectedData{
		TransactionID: txn.ID(),
		WalletID:      w.ID(),
		ProviderID:    txn.ProviderID(),
		ExternalID:    txn.ExternalID(),
		Kind:          string(txn.Kind()),
		FailureCode:   string(br.Code),
	})
	if err != nil {
		return err
	}
	if err := s.repo.InsertOutbox(ctx, tx, ev); err != nil {
		return err
	}
	s.met.TransactionsTotal.WithLabelValues(string(domain.StatusRejected)).Inc()
	return nil
}

func (s *WagerService) emitProcessed(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction, w *domain.Wallet) error {
	ws := &WalletService{uow: s.uow, repo: &s.repo.postgresRepos, log: s.log, met: s.met}
	return ws.emitWagerProcessed(ctx, tx, t, w)
}

func (s *WagerService) emitPendingReference(ctx context.Context, tx pgx.Tx, t *domain.WagerTransaction) error {
	ev, err := domain.NewOutboxEvent(domain.EventWagerPendingReference, t.ID(), t.ID().String(), domain.WagerProcessedData{
		TransactionID: t.ID(),
		WalletID:      t.WalletID(),
		ProviderID:    t.ProviderID(),
		ExternalID:    t.ExternalID(),
		Kind:          string(t.Kind()),
		Status:        string(t.Status()),
	})
	if err != nil {
		return err
	}
	return s.repo.InsertOutbox(ctx, tx, ev)
}

func (s *WagerService) GetByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return s.repo.GetWagerByID(ctx, id)
}

func (s *WagerService) GetByProviderExternal(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	return s.repo.GetWagerByProviderExternal(ctx, providerID, externalID)
}

func (s *WagerService) ProcessInboxMessage(ctx context.Context, consumerName, messageID, payloadHash string, in SubmitWagerInput) (*SubmitWagerResult, error) {
	var result *SubmitWagerResult
	err := s.uow.Within(ctx, func(ctx context.Context, tx pgx.Tx) error {
		inserted, err := s.repo.TryInsertInbox(ctx, tx, consumerName, messageID, payloadHash)
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}
		res, err := s.submitWithinExistingTx(ctx, tx, in)
		if err != nil {
			return err
		}
		result = res
		if err := s.repo.CompleteInbox(ctx, tx, consumerName, messageID); err != nil {
			return err
		}
		return nil
	})
	if result == nil && err == nil {
		return &SubmitWagerResult{Status: domain.StatusProcessed, IdempotentReplay: true}, nil
	}
	return result, err
}

func (s *WagerService) submitWithinExistingTx(ctx context.Context, tx pgx.Tx, in SubmitWagerInput) (*SubmitWagerResult, error) {
	if replay, err := s.checkIdempotency(ctx, in); replay != nil || err != nil {
		return replay, err
	}
	w, err := s.repo.GetWalletByIDForUpdate(ctx, tx, in.WalletID)
	if err != nil {
		return nil, err
	}
	txn, err := domain.NewExternalTransaction(
		in.Kind, in.WalletID, in.PlayerID, in.ProviderID, in.ExternalTransactionID,
		in.IdempotencyKey, in.PayloadHash, in.RoundID, in.GameID, in.Money, in.ReferenceExternalTransactionID,
	)
	if err != nil {
		return nil, err
	}
	var inlineResult *SubmitWagerResult
	if err := s.validateKindAmount(in.Kind, in.Money); err != nil {
		if err := s.rejectInTx(ctx, tx, txn, w, err, &inlineResult); err != nil {
			return nil, err
		}
		return inlineResult, nil
	}
	if in.Kind == domain.KindRefund || in.Kind == domain.KindRollback {
		ref, pending, err := s.resolveReference(ctx, tx, in.ProviderID, in.ReferenceExternalTransactionID)
		if pending {
			next := time.Now().UTC().Add(time.Second)
			_ = txn.MarkPendingReference(next)
			if err := s.repo.InsertWager(ctx, tx, txn); err != nil {
				return nil, err
			}
			_ = s.emitPendingReference(ctx, tx, txn)
			return &SubmitWagerResult{TransactionID: txn.ID(), Status: domain.StatusPendingReference}, nil
		}
		if err != nil {
			if err := s.rejectInTx(ctx, tx, txn, w, err, &inlineResult); err != nil {
				return nil, err
			}
			return inlineResult, nil
		}
		if err := s.validateReferenceMatch(in, ref); err != nil {
			if err := s.rejectInTx(ctx, tx, txn, w, err, &inlineResult); err != nil {
				return nil, err
			}
			return inlineResult, nil
		}
		if err := s.validateReversalRules(ctx, tx, in, ref); err != nil {
			if err := s.rejectInTx(ctx, tx, txn, w, err, &inlineResult); err != nil {
				return nil, err
			}
			return inlineResult, nil
		}
		txn.SetRefTransactionID(ref.ID())
	}
	if err := s.repo.InsertWager(ctx, tx, txn); err != nil {
		return nil, err
	}
	res, err := s.applyTransaction(ctx, tx, w, txn)
	if err != nil {
		if br, ok := domain.IsBusinessRejection(err); ok {
			if err := s.rejectPersisted(ctx, tx, txn, w, br); err != nil {
				return nil, err
			}
			return rejectionResult(txn, w, br), nil
		}
		return nil, err
	}
	return res, nil
}

func (s *WagerService) ResumePendingReferences(ctx context.Context, batch int) error {
	return s.uow.Within(ctx, func(ctx context.Context, tx pgx.Tx) error {
		pending, err := s.repo.ListPendingReferenceDue(ctx, tx, batch)
		if err != nil {
			return err
		}
		for _, txn := range pending {
			if time.Since(txn.CreatedAt()) > s.refTTL {
				w, _ := s.repo.GetWalletByIDForUpdate(ctx, tx, txn.WalletID())
				if w != nil {
					_ = s.rejectExisting(ctx, tx, txn, w, domain.BusinessRejection{Code: domain.FailureReferenceNotFound, Err: domain.ErrReferenceNotFound})
				}
				continue
			}
			in := SubmitWagerInput{
				ProviderID:                    txn.ProviderID(),
				ExternalTransactionID:         txn.ExternalID(),
				IdempotencyKey:                txn.IdempotencyKey(),
				PlayerID:                      txn.PlayerID(),
				WalletID:                      txn.WalletID(),
				RoundID:                       txn.RoundID(),
				GameID:                        txn.GameID(),
				Kind:                          txn.Kind(),
				Money:                         txn.Money(),
				ReferenceExternalTransactionID: txn.RefExternalID(),
				PayloadHash:                   txn.PayloadHash(),
			}
			ref, pendingRef, err := s.resolveReference(ctx, tx, in.ProviderID, in.ReferenceExternalTransactionID)
			if pendingRef {
				next := time.Now().UTC().Add(referenceBackoff(txn.ReferenceAttempts()))
				_ = txn.ScheduleReferenceRetry(next)
				if txn.ReferenceAttempts() >= s.refMaxTries {
					w, _ := s.repo.GetWalletByIDForUpdate(ctx, tx, txn.WalletID())
					if w != nil {
						_ = s.rejectExisting(ctx, tx, txn, w, domain.BusinessRejection{Code: domain.FailureReferenceNotFound, Err: domain.ErrReferenceNotFound})
					}
					continue
				}
				_ = s.repo.UpdateWager(ctx, tx, txn)
				continue
			}
			if err != nil {
				w, _ := s.repo.GetWalletByIDForUpdate(ctx, tx, txn.WalletID())
				if w != nil {
					_ = s.rejectExisting(ctx, tx, txn, w, domain.BusinessRejection{Code: domain.FailureReferenceNotSuccessful, Err: err})
				}
				continue
			}
			if err := s.validateReferenceMatch(in, ref); err != nil {
				w, _ := s.repo.GetWalletByIDForUpdate(ctx, tx, txn.WalletID())
				if w != nil {
					_ = s.rejectExisting(ctx, tx, txn, w, domain.AsBusinessRejection(err))
				}
				continue
			}
			if err := s.validateReversalRules(ctx, tx, in, ref); err != nil {
				w, _ := s.repo.GetWalletByIDForUpdate(ctx, tx, txn.WalletID())
				if w != nil {
					_ = s.rejectExisting(ctx, tx, txn, w, domain.AsBusinessRejection(err))
				}
				continue
			}
			txn.SetRefTransactionID(ref.ID())
			w, err := s.repo.GetWalletByIDForUpdate(ctx, tx, txn.WalletID())
			if err != nil {
				return err
			}
			_, err = s.applyTransaction(ctx, tx, w, txn)
			if err != nil {
				if br, ok := domain.IsBusinessRejection(err); ok {
					_ = s.rejectExisting(ctx, tx, txn, w, br)
				}
			}
		}
		return nil
	})
}

func referenceBackoff(attempts int) time.Duration {
	sec := math.Min(30, math.Pow(2, float64(attempts)))
	return time.Duration(sec) * time.Second
}
