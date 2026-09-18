package domain

import "errors"

var (
	ErrInvalidMoney       = errors.New("invalid money")
	ErrCurrencyMismatch   = errors.New("currency mismatch")
	ErrOverflow           = errors.New("money overflow")
	ErrInsufficientFunds  = errors.New("insufficient funds")
	ErrInvalidTransition  = errors.New("invalid state transition")
	ErrTerminalState      = errors.New("terminal state")
	ErrInvalidKind        = errors.New("invalid transaction kind")
	ErrIdempotencyConflict = errors.New("idempotency conflict")
	ErrNotFound           = errors.New("not found")
	ErrAlreadyExists      = errors.New("already exists")
	ErrUnauthorized       = errors.New("unauthorized")
	ErrReferenceNotFound  = errors.New("reference not found")
	ErrReferenceNotReady  = errors.New("reference not ready")
	ErrReferenceReversed  = errors.New("reference already reversed")
	ErrInsufficientReversal = errors.New("insufficient funds for reversal")
)

type FailureCode string

const (
	FailureInsufficientFunds        FailureCode = "INSUFFICIENT_FUNDS"
	FailureInsufficientFundsReversal FailureCode = "INSUFFICIENT_FUNDS_REVERSAL"
	FailureReferenceNotFound        FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferenceNotSuccessful   FailureCode = "REFERENCE_NOT_SUCCESSFUL"
	FailureReferenceAlreadyReversed FailureCode = "REFERENCE_ALREADY_REVERSED"
	FailureInvalidAmount            FailureCode = "INVALID_AMOUNT"
	FailureInvalidReference         FailureCode = "INVALID_REFERENCE"
	FailureOpeningNotAllowed        FailureCode = "OPENING_NOT_ALLOWED"
)

type BusinessRejection struct {
	Code FailureCode
	Err  error
}

func (b BusinessRejection) Error() string {
	if b.Err != nil {
		return b.Err.Error()
	}
	return string(b.Code)
}

func (b BusinessRejection) Unwrap() error {
	return b.Err
}

func IsBusinessRejection(err error) (BusinessRejection, bool) {
	var br BusinessRejection
	if errors.As(err, &br) {
		return br, true
	}
	return BusinessRejection{}, false
}

func AsBusinessRejection(err error) BusinessRejection {
	if br, ok := IsBusinessRejection(err); ok {
		return br
	}
	return BusinessRejection{Code: FailureInvalidReference, Err: err}
}
