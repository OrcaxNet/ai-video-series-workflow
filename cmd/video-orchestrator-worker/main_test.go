package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWorkerProviderHTTPClientSignsSpeechAdapterRequests(t *testing.T) {
	var received bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		received = true
		if !strings.HasPrefix(request.Header.Get("Authorization"), "Video-HMAC-SHA256 ") ||
			request.Header.Get("X-Video-Auth-Timestamp") == "" ||
			request.Header.Get("X-Video-Auth-Nonce") == "" ||
			request.Header.Get("X-Video-Auth-Content-SHA256") == "" {
			t.Errorf("speech Adapter request was not service-authenticated")
		}
		body, err := io.ReadAll(request.Body)
		if err != nil || string(body) != `{"capability":"speech.primary"}` {
			t.Errorf("request body = %q, err = %v", body, err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := workerProviderHTTPClient(
		server.Client(), "stage1-worker-service-auth-secret-32-bytes",
	)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(
		http.MethodPost, server.URL+"/v1/jobs",
		bytes.NewBufferString(`{"capability":"speech.primary"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent || !received {
		t.Fatalf("status = %d, received = %v", response.StatusCode, received)
	}
}

func TestWorkerProviderHTTPClientKeepsMockPathUnsigned(t *testing.T) {
	base := &http.Client{}
	client, err := workerProviderHTTPClient(base, "")
	if err != nil {
		t.Fatal(err)
	}
	if client != base {
		t.Fatal("mock provider client was unexpectedly wrapped")
	}
}
