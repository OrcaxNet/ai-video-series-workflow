package artifactstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
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
