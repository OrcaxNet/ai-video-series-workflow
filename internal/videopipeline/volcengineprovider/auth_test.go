package volcengineprovider

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
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

func TestServiceAuthenticatorRejectsConcurrentReplayBeforeProtectedHandler(t *testing.T) {
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

	const attempts = 16
	statuses := make(chan int, attempts)
	errorsSeen := make(chan error, attempts)
	var start sync.WaitGroup
	start.Add(1)
	var workers sync.WaitGroup
	workers.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer workers.Done()
			start.Wait()
			request, err := signedServiceRequest(
				http.MethodPost,
				server.URL+"/v1/jobs",
				[]byte(`{"job":"concurrent"}`),
				[]byte(`{"job":"concurrent"}`),
				fixedNow.Add(119*time.Second),
				"00000000000000000000000000000009",
			)
			if err != nil {
				errorsSeen <- err
				return
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				errorsSeen <- err
				return
			}
			_ = response.Body.Close()
			statuses <- response.StatusCode
		}()
	}
	start.Done()
	workers.Wait()
	close(statuses)
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}

	accepted := 0
	rejected := 0
	for status := range statuses {
		switch status {
		case http.StatusNoContent:
			accepted++
		case http.StatusUnauthorized:
			rejected++
		default:
			t.Fatal(fmt.Errorf("unexpected replay response status %d", status))
		}
	}
	if accepted != 1 || rejected != attempts-1 || calls.Load() != 1 {
		t.Fatalf("accepted=%d rejected=%d protected calls=%d", accepted, rejected, calls.Load())
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
	return signedTestServiceRequestMethodWithBody(t, http.MethodPost, url, body, body, signedAt, nonce)
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
	return signedTestServiceRequestMethodWithBody(t, http.MethodPost, url, body, signedBody, signedAt, nonce)
}

func signedTestServiceRequestMethodWithBody(
	t *testing.T,
	method string,
	url string,
	body []byte,
	signedBody []byte,
	signedAt time.Time,
	nonce string,
) *http.Request {
	t.Helper()
	request, err := signedServiceRequest(method, url, body, signedBody, signedAt, nonce)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func signedServiceRequest(
	method string,
	url string,
	body []byte,
	signedBody []byte,
	signedAt time.Time,
	nonce string,
) (*http.Request, error) {
	request, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
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
	return request, nil
}
