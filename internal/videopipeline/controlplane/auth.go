package controlplane

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type authenticatedActorKey struct{}

type bearerHeader struct {
	Algorithm string `json:"alg"`
	Type      string `json:"typ"`
}

type bearerClaims struct {
	Subject  string `json:"sub"`
	Role     string `json:"role"`
	Audience string `json:"aud"`
	Expires  int64  `json:"exp"`
}

func (s *Server) authentication(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if publicControlPlanePath(r.URL.Path) || !s.authRequired() {
			next.ServeHTTP(w, r)
			return
		}
		actor, err := s.verifyBearer(r.Header.Get("Authorization"))
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="video-control-plane"`)
			writeProblem(w, traceID(r), domainError(
				CodeAuthentication,
				http.StatusUnauthorized,
				"Bearer authentication is missing or invalid",
				"provide a current HS256 control-plane token",
				err,
			))
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), authenticatedActorKey{}, actor)))
	})
}

func (s *Server) authRequired() bool {
	if s.config.AuthHMACSecret != "" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(s.config.Environment)) {
	case "", "development", "local", "test":
		return false
	default:
		return true
	}
}

func publicControlPlanePath(path string) bool {
	return strings.HasPrefix(path, "/health/") ||
		strings.HasPrefix(path, APIBase+"/system/") ||
		path == APIBase+"/providers/status"
}

func (s *Server) verifyBearer(value string) (Actor, error) {
	const prefix = "Bearer "
	if !strings.HasPrefix(value, prefix) {
		return Actor{}, errors.New("Bearer token is required")
	}
	parts := strings.Split(strings.TrimSpace(strings.TrimPrefix(value, prefix)), ".")
	if len(parts) != 3 {
		return Actor{}, errors.New("token must contain three segments")
	}
	var header bearerHeader
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || json.Unmarshal(headerJSON, &header) != nil || header.Algorithm != "HS256" {
		return Actor{}, errors.New("token header is invalid")
	}
	signingInput := parts[0] + "." + parts[1]
	expected := hmac.New(sha256.New, []byte(s.config.AuthHMACSecret))
	_, _ = expected.Write([]byte(signingInput))
	actual, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(actual, expected.Sum(nil)) {
		return Actor{}, errors.New("token signature is invalid")
	}
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Actor{}, errors.New("token claims are invalid")
	}
	var claims bearerClaims
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		return Actor{}, errors.New("token claims are invalid")
	}
	actor := normalizedActor(Actor{ActorID: claims.Subject, Role: claims.Role})
	if actor.ActorID == "" || actor.Role == "" ||
		claims.Audience != s.config.AuthAudience ||
		claims.Expires <= s.now().UTC().Unix() {
		return Actor{}, errors.New("token claims are expired or outside the control-plane audience")
	}
	return actor, nil
}

func authenticatedActor(ctx context.Context) (Actor, bool) {
	actor, ok := ctx.Value(authenticatedActorKey{}).(Actor)
	return actor, ok
}

func (s *Server) authorizeCommandActor(r *http.Request, actor Actor) error {
	authenticated, ok := authenticatedActor(r.Context())
	if !ok {
		if s.authRequired() {
			return domainError(
				CodeAuthentication,
				http.StatusUnauthorized,
				"authenticated actor is unavailable",
				"provide a current control-plane token",
				nil,
			)
		}
		return nil
	}
	requested := normalizedActor(actor)
	if authenticated != requested {
		return forbiddenError("request actor must match the signed token subject and role")
	}
	return nil
}
