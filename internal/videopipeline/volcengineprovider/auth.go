package volcengineprovider

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
)

const (
	serviceAuthScheme          = "Video-HMAC-SHA256"
	serviceAuthTimestampHeader = "X-Video-Auth-Timestamp"
	serviceAuthNonceHeader     = "X-Video-Auth-Nonce"
	serviceAuthBodyHashHeader  = "X-Video-Auth-Content-SHA256"
	serviceAuthMaxSkew         = 2 * time.Minute
)

type serviceAuthenticator struct {
	secret []byte
	now    func() time.Time

	mu     sync.Mutex
	nonces map[string]time.Time
}

func newServiceAuthenticator(secret string, now func() time.Time) (*serviceAuthenticator, error) {
	if len(secret) < 32 {
		return nil, errors.New("provider service authentication secret must contain at least 32 bytes")
	}
	if now == nil {
		now = time.Now
	}
	return &serviceAuthenticator{
		secret: []byte(secret),
		now:    now,
		nonces: make(map[string]time.Time),
	}, nil
}

func (a *serviceAuthenticator) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusUnauthorized, unauthenticatedError())
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		if err := a.verify(r, body); err != nil {
			writeError(w, http.StatusUnauthorized, unauthenticatedError())
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *serviceAuthenticator) verify(request *http.Request, body []byte) error {
	timestampRaw := strings.TrimSpace(request.Header.Get(serviceAuthTimestampHeader))
	nonce := strings.TrimSpace(request.Header.Get(serviceAuthNonceHeader))
	bodyHash := strings.TrimSpace(request.Header.Get(serviceAuthBodyHashHeader))
	authorization := strings.TrimSpace(request.Header.Get("Authorization"))
	if timestampRaw == "" || nonce == "" || bodyHash == "" || authorization == "" {
		return errors.New("missing service authentication")
	}
	nonceBytes, err := hex.DecodeString(nonce)
	if err != nil || len(nonceBytes) != 16 {
		return errors.New("invalid service authentication nonce")
	}
	timestampSeconds, err := strconv.ParseInt(timestampRaw, 10, 64)
	if err != nil {
		return errors.New("invalid service authentication timestamp")
	}
	now := a.now().UTC()
	signedAt := time.Unix(timestampSeconds, 0).UTC()
	if signedAt.Before(now.Add(-serviceAuthMaxSkew)) || signedAt.After(now.Add(serviceAuthMaxSkew)) {
		return errors.New("expired service authentication")
	}
	bodySum := sha256.Sum256(body)
	if !hmac.Equal([]byte(strings.ToLower(bodyHash)), []byte(hex.EncodeToString(bodySum[:]))) {
		return errors.New("service authentication body mismatch")
	}
	prefix := serviceAuthScheme + " "
	if !strings.HasPrefix(authorization, prefix) {
		return errors.New("invalid service authentication scheme")
	}
	provided, err := hex.DecodeString(strings.TrimPrefix(authorization, prefix))
	if err != nil || len(provided) != sha256.Size {
		return errors.New("invalid service authentication signature")
	}
	expected := signServiceRequest(
		a.secret,
		request.Method,
		request.URL.EscapedPath(),
		request.URL.RawQuery,
		timestampRaw,
		nonce,
		hex.EncodeToString(bodySum[:]),
	)
	if !hmac.Equal(provided, expected) {
		return errors.New("invalid service authentication signature")
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	for knownNonce, expiresAt := range a.nonces {
		if !expiresAt.After(now) {
			delete(a.nonces, knownNonce)
		}
	}
	if _, replayed := a.nonces[nonce]; replayed {
		return errors.New("replayed service authentication")
	}
	a.nonces[nonce] = now.Add(serviceAuthMaxSkew)
	return nil
}

func unauthenticatedError() *providercontract.Error {
	return safeError(
		providercontract.CodeUnauthenticated,
		"provider adapter service authentication is required",
		false,
	)
}

func signServiceRequest(secret []byte, method, path, rawQuery, timestamp, nonce, bodyHash string) []byte {
	canonical := strings.Join([]string{
		method,
		path,
		rawQuery,
		timestamp,
		nonce,
		bodyHash,
	}, "\n")
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(canonical))
	return mac.Sum(nil)
}

type serviceAuthTransport struct {
	base   http.RoundTripper
	secret []byte
	now    func() time.Time
	random io.Reader
}

// AuthenticatedHTTPClient clones base and signs every adapter request with a
// short-lived, nonce-bound HMAC. The secret is retained only in memory.
func AuthenticatedHTTPClient(base *http.Client, secret string) (*http.Client, error) {
	return authenticatedHTTPClient(base, secret, time.Now, rand.Reader)
}

func authenticatedHTTPClient(
	base *http.Client,
	secret string,
	now func() time.Time,
	random io.Reader,
) (*http.Client, error) {
	if len(secret) < 32 {
		return nil, errors.New("provider service authentication secret must contain at least 32 bytes")
	}
	if base == nil {
		base = &http.Client{}
	}
	if now == nil || random == nil {
		return nil, errors.New("provider service authentication clock and entropy source are required")
	}
	clone := *base
	transport := base.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	clone.Transport = &serviceAuthTransport{
		base: transport, secret: []byte(secret), now: now, random: random,
	}
	return &clone, nil
}

func (t *serviceAuthTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	body, err := requestBody(request)
	if err != nil {
		return nil, fmt.Errorf("read provider adapter request for signing: %w", err)
	}
	clone.Body = io.NopCloser(bytes.NewReader(body))
	clone.ContentLength = int64(len(body))

	nonceBytes := make([]byte, 16)
	if _, err := io.ReadFull(t.random, nonceBytes); err != nil {
		return nil, fmt.Errorf("generate provider adapter authentication nonce: %w", err)
	}
	timestamp := strconv.FormatInt(t.now().UTC().Unix(), 10)
	nonce := hex.EncodeToString(nonceBytes)
	bodySum := sha256.Sum256(body)
	bodyHash := hex.EncodeToString(bodySum[:])
	signature := signServiceRequest(
		t.secret,
		clone.Method,
		clone.URL.EscapedPath(),
		clone.URL.RawQuery,
		timestamp,
		nonce,
		bodyHash,
	)
	clone.Header.Set(serviceAuthTimestampHeader, timestamp)
	clone.Header.Set(serviceAuthNonceHeader, nonce)
	clone.Header.Set(serviceAuthBodyHashHeader, bodyHash)
	clone.Header.Set("Authorization", serviceAuthScheme+" "+hex.EncodeToString(signature))
	return t.base.RoundTrip(clone)
}

func requestBody(request *http.Request) ([]byte, error) {
	if request.Body == nil {
		return nil, nil
	}
	if request.GetBody != nil {
		body, err := request.GetBody()
		if err != nil {
			return nil, err
		}
		defer body.Close()
		return io.ReadAll(body)
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}
