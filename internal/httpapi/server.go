package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/felipemalves/distributed-betting-processor/internal/application"
	"github.com/felipemalves/distributed-betting-processor/internal/auth"
	"github.com/felipemalves/distributed-betting-processor/internal/domain"
	"github.com/felipemalves/distributed-betting-processor/internal/observe"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

type Server struct {
	wallets *application.WalletService
	wagers  *application.WagerService
	auth    *auth.Verifier
	log     *slog.Logger
	ready   func(context context.Context) error
}

func NewServer(wallets *application.WalletService, wagers *application.WagerService, verifier *auth.Verifier, log *slog.Logger, ready func(context context.Context) error) *Server {
	return &Server{wallets: wallets, wagers: wagers, auth: verifier, log: log, ready: ready}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Get("/health/live", s.live)
	r.Get("/health/ready", s.readyHandler)
	r.Handle("/metrics", observe.MetricsHandler())
	r.Group(func(r chi.Router) {
		r.Use(s.authenticate)
		r.Post("/wallets", s.openWallet)
		r.Get("/wallets/{walletId}", s.getWallet)
		r.Get("/wallets/{walletId}/ledger", s.listLedger)
		r.Post("/wallets/{walletId}/reconciliation", s.reconcile)
		r.Post("/wagering/transactions", s.submitWager)
		r.Get("/wagering/transactions/{transactionId}", s.getWagerByID)
		r.Get("/providers/{providerId}/wagering/transactions/{externalTransactionId}", s.getWagerByExternal)
	})
	return r
}

func (s *Server) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyHandler(w http.ResponseWriter, r *http.Request) {
	if s.ready != nil {
		if err := s.ready(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "error": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type ctxKey int

const principalKey ctxKey = 1

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.auth == nil {
			next.ServeHTTP(w, r)
			return
		}
		token, err := auth.BearerToken(r)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		p, err := s.auth.Verify(r.Context(), token)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid_token"})
			return
		}
		ctx := contextWithPrincipal(r.Context(), p)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func contextWithPrincipal(ctx context.Context, p *auth.Principal) context.Context {
	return context.WithValue(ctx, principalKey, p)
}

func principalFrom(ctx context.Context) (*auth.Principal, bool) {
	p, ok := ctx.Value(principalKey).(*auth.Principal)
	return p, ok
}

func (s *Server) openWallet(w http.ResponseWriter, r *http.Request) {
	p, ok := principalFrom(r.Context())
	if !ok || !p.Internal {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
		return
	}
	var body struct {
		PlayerID       string          `json:"playerId"`
		InitialBalance domain.MoneyDTO `json:"initialBalance"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_body"})
		return
	}
	playerID, err := uuid.Parse(body.PlayerID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_player_id"})
		return
	}
	money, err := domain.ParseMoney(body.InitialBalance.Amount, body.InitialBalance.Currency)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	res, err := s.wallets.OpenWallet(r.Context(), application.OpenWalletInput{PlayerID: playerID, InitialBalance: money})
	if errors.Is(err, domain.ErrAlreadyExists) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "wallet_exists"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, walletResponse(res.Wallet))
}

func walletResponse(w *domain.Wallet) map[string]any {
	return map[string]any{
		"id":       w.ID().String(),
		"playerId": w.PlayerID().String(),
		"balance":  domain.MoneyToDTO(w.Balance()),
		"version":  w.Version(),
	}
}

func (s *Server) getWallet(w http.ResponseWriter, r *http.Request) {
	if !s.allowWalletRead(r) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "walletId"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_wallet_id"})
		return
	}
	wallet, err := s.wallets.GetWallet(r.Context(), id)
	if errors.Is(err, domain.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, walletResponse(wallet))
}

func (s *Server) allowWalletRead(r *http.Request) bool {
	p, ok := principalFrom(r.Context())
	return ok && p.Internal
}

func (s *Server) listLedger(w http.ResponseWriter, r *http.Request) {
	if !s.allowWalletRead(r) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "walletId"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_wallet_id"})
		return
	}
	page, err := s.wallets.ListLedger(r.Context(), id, r.URL.Query().Get("cursor"), queryLimit(r))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	entries := make([]map[string]any, 0, len(page.Entries))
	for _, e := range page.Entries {
		entries = append(entries, map[string]any{
			"id":            e.ID().String(),
			"transactionId": e.TransactionID().String(),
			"direction":     string(e.Direction()),
			"money":         domain.MoneyToDTO(e.Amount()),
			"balanceBefore": domain.MoneyToDTO(e.BalanceBefore()),
			"balanceAfter":  domain.MoneyToDTO(e.BalanceAfter()),
			"createdAt":     e.CreatedAt().Format(timeRFC3339),
		})
	}
	resp := map[string]any{"entries": entries}
	if page.NextCursor != "" {
		resp["nextCursor"] = page.NextCursor
	}
	writeJSON(w, http.StatusOK, resp)
}

const timeRFC3339 = "2006-01-02T15:04:05.000Z07:00"

func queryLimit(r *http.Request) int {
	return 50
}

func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	if !s.allowWalletRead(r) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "walletId"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_wallet_id"})
		return
	}
	res, err := s.wallets.Reconcile(r.Context(), id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"walletId":          res.WalletID.String(),
		"storedBalance":     domain.MoneyToDTO(res.StoredBalance),
		"calculatedBalance": domain.MoneyToDTO(res.CalculatedBalance),
		"difference":        domain.MoneyToDTO(res.Difference),
		"consistent":        res.Consistent,
		"checkedEntries":    res.CheckedEntries,
	})
}

func (s *Server) submitWager(w http.ResponseWriter, r *http.Request) {
	p, ok := principalFrom(r.Context())
	if !ok || p.Internal {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
		return
	}
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if idempotencyKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing_idempotency_key"})
		return
	}
	var body wagerBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_body"})
		return
	}
	if body.ProviderID != p.ProviderID {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "provider_mismatch"})
		return
	}
	in, err := body.toInput(idempotencyKey)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	res, err := s.wagers.Submit(r.Context(), in)
	if errors.Is(err, domain.ErrIdempotencyConflict) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "idempotency_conflict"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if res.Status == domain.StatusRejected {
		writeJSON(w, http.StatusUnprocessableEntity, wagerResultResponse(res))
		return
	}
	status := http.StatusOK
	if res.Status == domain.StatusPending || res.Status == domain.StatusPendingReference {
		status = http.StatusAccepted
	}
	writeJSON(w, status, wagerResultResponse(res))
}

type wagerBody struct {
	ProviderID                    string          `json:"providerId"`
	ExternalTransactionID         string          `json:"externalTransactionId"`
	PlayerID                      string          `json:"playerId"`
	WalletID                      string          `json:"walletId"`
	RoundID                       string          `json:"roundId"`
	GameID                        string          `json:"gameId"`
	Kind                          string          `json:"kind"`
	Money                         domain.MoneyDTO `json:"money"`
	ReferenceExternalTransactionID string         `json:"referenceExternalTransactionId"`
}

func (b wagerBody) toInput(idempotencyKey string) (application.SubmitWagerInput, error) {
	playerID, err := uuid.Parse(b.PlayerID)
	if err != nil {
		return application.SubmitWagerInput{}, err
	}
	walletID, err := uuid.Parse(b.WalletID)
	if err != nil {
		return application.SubmitWagerInput{}, err
	}
	money, err := domain.ParseMoney(b.Money.Amount, b.Money.Currency)
	if err != nil {
		return application.SubmitWagerInput{}, err
	}
	fields := domain.WagerPayloadFields{
		ProviderID:                    b.ProviderID,
		ExternalTransactionID:         b.ExternalTransactionID,
		PlayerID:                      b.PlayerID,
		WalletID:                      b.WalletID,
		RoundID:                       b.RoundID,
		GameID:                        b.GameID,
		Kind:                          b.Kind,
		MoneyAmount:                   b.Money.Amount,
		MoneyCurrency:                 b.Money.Currency,
		ReferenceExternalTransactionID: b.ReferenceExternalTransactionID,
	}
	return application.SubmitWagerInput{
		ProviderID:                    b.ProviderID,
		ExternalTransactionID:         b.ExternalTransactionID,
		IdempotencyKey:                idempotencyKey,
		PlayerID:                      playerID,
		WalletID:                      walletID,
		RoundID:                       b.RoundID,
		GameID:                        b.GameID,
		Kind:                          domain.TransactionKind(b.Kind),
		Money:                         money,
		ReferenceExternalTransactionID: b.ReferenceExternalTransactionID,
		PayloadHash:                   domain.HashWagerPayload(fields),
	}, nil
}

func wagerResultResponse(res *application.SubmitWagerResult) map[string]any {
	out := map[string]any{
		"transactionId":    res.TransactionID.String(),
		"status":           string(res.Status),
		"idempotentReplay": res.IdempotentReplay,
	}
	if res.Balance != nil {
		out["balance"] = domain.MoneyToDTO(*res.Balance)
	}
	if res.FailureCode != "" {
		out["failureCode"] = string(res.FailureCode)
	}
	return out
}

func (s *Server) getWagerByID(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "transactionId"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_id"})
		return
	}
	tx, err := s.wagers.GetByID(r.Context(), id)
	if errors.Is(err, domain.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !s.canReadWager(r, tx.ProviderID()) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
		return
	}
	writeJSON(w, http.StatusOK, wagerTransactionResponse(tx))
}

func (s *Server) getWagerByExternal(w http.ResponseWriter, r *http.Request) {
	providerID := chi.URLParam(r, "providerId")
	externalID := chi.URLParam(r, "externalTransactionId")
	if !s.canReadWager(r, providerID) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
		return
	}
	tx, err := s.wagers.GetByProviderExternal(r.Context(), providerID, externalID)
	if errors.Is(err, domain.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, wagerTransactionResponse(tx))
}

func (s *Server) canReadWager(r *http.Request, providerID string) bool {
	p, ok := principalFrom(r.Context())
	if !ok {
		return false
	}
	if p.Internal {
		return true
	}
	return p.ProviderID == providerID
}

func wagerTransactionResponse(t *domain.WagerTransaction) map[string]any {
	out := map[string]any{
		"transactionId": t.ID().String(),
		"status":        string(t.Status()),
		"kind":          string(t.Kind()),
		"walletId":      t.WalletID().String(),
	}
	if t.FailureCode() != "" {
		out["failureCode"] = string(t.FailureCode())
	}
	if t.ObservedBalance() != nil {
		out["balance"] = domain.MoneyToDTO(*t.ObservedBalance())
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
