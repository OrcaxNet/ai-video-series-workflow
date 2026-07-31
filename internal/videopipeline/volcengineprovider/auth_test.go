package volcengineprovider

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestServiceAuthenticatorRejectsExpiredTamperedAndReplayedRequests(t *testing.T) {
	t.Parallel()
	fixedNow := time.Unix(1_800_000_000, 0).UTC()
	authenticator, err := newServiceAuthenticator(testServiceAuthSecret, func() time.Time { return fixedNow })
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server := httptest.NewServer(authenticator.middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})))
	defer server.Close()

	valid := signedTestServiceRequest(t, server.URL+"/v1/jobs", []byte(`{"job":"one"}`), fixedNow, "00000000000000000000000000000001")
	response, err := http.DefaultClient.Do(valid)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent || calls.Load() != 1 {
		t.Fatalf("valid request HTTP %d, calls=%d", response.StatusCode, calls.Load())
	}

	tests := []struct {
		name       string
		body       []byte
		signedBody []byte
		signedAt   time.Time
		nonce      string
	}{
		{
			name: "replayed nonce", body: []byte(`{"job":"one"}`), signedBody: []byte(`{"job":"one"}`),
			signedAt: fixedNow, nonce: "00000000000000000000000000000001",
		},
		{
			name: "expired signature", body: []byte(`{"job":"two"}`), signedBody: []byte(`{"job":"two"}`),
			signedAt: fixedNow.Add(-serviceAuthMaxSkew - time.Second), nonce: "00000000000000000000000000000002",
		},
		{
			name: "tampered body", body: []byte(`{"job":"changed"}`), signedBody: []byte(`{"job":"three"}`),
			signedAt: fixedNow, nonce: "00000000000000000000000000000003",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := signedTestServiceRequestWithBody(t, server.URL+"/v1/jobs", tt.body, tt.signedBody, tt.signedAt, tt.nonce)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("HTTP status = %d, want 401", response.StatusCode)
			}
		})
	}
	if calls.Load() != 1 {
		t.Fatalf("rejected requests reached protected handler; calls=%d", calls.Load())
	}
}

func signedTestServiceRequest(
	t *testing.T,
	url string,
	body []byte,
	signedAt time.Time,
	nonce string,
) *http.Request {
	t.Helper()
	return signedTestServiceRequestWithBody(t, url, body, body, signedAt, nonce)
}

func signedTestServiceRequestWithBody(
	t *testing.T,
	url string,
	body []byte,
	signedBody []byte,
	signedAt time.Time,
	nonce string,
) *http.Request {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	timestamp := strconv.FormatInt(signedAt.Unix(), 10)
	bodySum := sha256.Sum256(signedBody)
	bodyHash := hex.EncodeToString(bodySum[:])
	signature := signServiceRequest(
		[]byte(testServiceAuthSecret),
		request.Method,
		request.URL.EscapedPath(),
		request.URL.RawQuery,
		timestamp,
		nonce,
		bodyHash,
	)
	request.Header.Set(serviceAuthTimestampHeader, timestamp)
	request.Header.Set(serviceAuthNonceHeader, nonce)
	request.Header.Set(serviceAuthBodyHashHeader, bodyHash)
	request.Header.Set("Authorization", serviceAuthScheme+" "+hex.EncodeToString(signature))
	return request
}
