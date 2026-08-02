package artifactstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"testing"
)

func TestStore_PutAndOpen(t *testing.T) {
	t.Parallel()

	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	const content = "immutable-video-artifact"
	wantDigestBytes := sha256.Sum256([]byte(content))
	wantDigest := hex.EncodeToString(wantDigestBytes[:])

	first, err := store.Put(context.Background(), strings.NewReader(content))
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	second, err := store.Put(context.Background(), strings.NewReader(content))
	if err != nil {
		t.Fatalf("second Put() error = %v", err)
	}

	if first.Digest != wantDigest {
		t.Fatalf("Put() digest = %q, want %q", first.Digest, wantDigest)
	}
	if first != second {
		t.Fatalf("deduplicated artifact = %#v, want %#v", second, first)
	}
	if first.URI != "cas://sha256/"+wantDigest {
		t.Fatalf("Put() URI = %q", first.URI)
	}

	reader, err := store.Open(first.Digest)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != content {
		t.Fatalf("Open() content = %q, want %q", got, content)
	}
	resolved, err := store.Resolve(context.Background(), wantDigest)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resolved != first {
		t.Fatalf("Resolve() = %#v, want %#v", resolved, first)
	}
}

func TestStore_OpenRejectsInvalidDigest(t *testing.T) {
	t.Parallel()

	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, digest := range []string{"", "../escape", strings.Repeat("A", 64), strings.Repeat("a", 63)} {
		digest := digest
		t.Run(digest, func(t *testing.T) {
			t.Parallel()
			if _, err := store.Open(digest); err == nil {
				t.Fatal("Open() error = nil, want validation error")
			}
		})
	}
}

func TestStore_PutHonorsCancelledContext(t *testing.T) {
	t.Parallel()

	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Put(ctx, strings.NewReader("ignored")); err == nil {
		t.Fatal("Put() error = nil, want context cancellation")
	}
}

func TestStore_ResolveRejectsMissingAndCorruptedObject(t *testing.T) {
	t.Parallel()

	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	missingBytes := sha256.Sum256([]byte("missing"))
	missing := hex.EncodeToString(missingBytes[:])
	if _, err := store.Resolve(context.Background(), missing); err == nil {
		t.Fatal("Resolve(missing) error = nil")
	}

	artifact, err := store.Put(context.Background(), strings.NewReader("original"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(artifact.Path, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifact.Path, []byte("tampered"), 0o440); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Resolve(context.Background(), artifact.Digest); err == nil ||
		!strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("Resolve(corrupted) error = %v", err)
	}
}
