package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

type Principal struct {
	Subject    string
	ProviderID string
	Internal   bool
}

type Verifier struct {
	verifier *oidc.IDTokenVerifier
}

func NewVerifier(ctx context.Context, issuer, audience string) (*Verifier, error) {
	provider, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, err
	}
	cfg := &oidc.Config{SkipClientIDCheck: true}
	if audience != "" {
		cfg.ClientID = audience
	}
	return &Verifier{verifier: provider.Verifier(cfg)}, nil
}

func (v *Verifier) Verify(ctx context.Context, rawToken string) (*Principal, error) {
	idToken, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		return nil, err
	}
	var claims struct {
		Azp              string `json:"azp"`
		PreferredUsername string `json:"preferred_username"`
		ClientID         string `json:"client_id"`
	}
	if err := idToken.Claims(&claims); err != nil {
		return nil, err
	}
	clientID := claims.Azp
	if clientID == "" {
		clientID = claims.ClientID
	}
	if clientID == "" {
		clientID = claims.PreferredUsername
	}
	if clientID == "" {
		return nil, errors.New("missing client identity")
	}
	p := &Principal{Subject: idToken.Subject, ProviderID: clientID}
	if clientID == "internal-service" {
		p.Internal = true
	}
	return p, nil
}

func BearerToken(r *http.Request) (string, error) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", errors.New("missing authorization")
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return "", errors.New("invalid authorization scheme")
	}
	return strings.TrimSpace(h[len(prefix):]), nil
}
