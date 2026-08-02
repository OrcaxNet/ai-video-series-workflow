// Package artifactstore implements the local content-addressed artifact store
// used by the video-pipeline PoC.
package artifactstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const digestLength = sha256.Size * 2

// Artifact is an immutable object committed to content-addressed storage.
type Artifact struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
	URI    string `json:"uri"`
	Path   string `json:"-"`
}

// Store persists immutable artifacts beneath a filesystem root.
//
// Objects are stored as:
//
//	<root>/sha256/<first-two-hex-characters>/<full-digest>
type Store struct {
	root string
}

// New creates or opens a local artifact store.
func New(root string) (*Store, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("artifact store root is required")
	}
	cleanRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve artifact store root: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(cleanRoot, "sha256"), 0o750); err != nil {
		return nil, fmt.Errorf("create artifact store root: %w", err)
	}
	return &Store{root: cleanRoot}, nil
}

// Root returns the absolute storage root for health checks and diagnostics.
func (s *Store) Root() string { return s.root }

// Put streams content to a temporary file, computes its SHA-256 digest, and
// atomically commits it. Repeated content returns the existing object.
func (s *Store) Put(ctx context.Context, source io.Reader) (Artifact, error) {
	if err := ctx.Err(); err != nil {
		return Artifact{}, err
	}

	tmp, err := os.CreateTemp(s.root, ".artifact-*")
	if err != nil {
		return Artifact{}, fmt.Errorf("create artifact temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	hash := sha256.New()
	written, copyErr := copyWithContext(ctx, io.MultiWriter(tmp, hash), source)
	syncErr := tmp.Sync()
	closeErr := tmp.Close()
	if copyErr != nil {
		return Artifact{}, fmt.Errorf("stream artifact: %w", copyErr)
	}
	if syncErr != nil {
		return Artifact{}, fmt.Errorf("sync artifact: %w", syncErr)
	}
	if closeErr != nil {
		return Artifact{}, fmt.Errorf("close artifact: %w", closeErr)
	}

	digest := hex.EncodeToString(hash.Sum(nil))
	finalDir := filepath.Join(s.root, "sha256", digest[:2])
	finalPath := filepath.Join(finalDir, digest)
	if err := os.MkdirAll(finalDir, 0o750); err != nil {
		return Artifact{}, fmt.Errorf("create artifact digest directory: %w", err)
	}

	if info, err := os.Stat(finalPath); err == nil {
		return artifactFor(finalPath, digest, info.Size()), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return Artifact{}, fmt.Errorf("inspect existing artifact: %w", err)
	}

	if err := os.Rename(tmpName, finalPath); err != nil {
		// Another writer may have committed the same digest after our Stat.
		if info, statErr := os.Stat(finalPath); statErr == nil {
			return artifactFor(finalPath, digest, info.Size()), nil
		}
		return Artifact{}, fmt.Errorf("commit artifact: %w", err)
	}
	if err := os.Chmod(finalPath, 0o440); err != nil {
		return Artifact{}, fmt.Errorf("protect artifact: %w", err)
	}
	return artifactFor(finalPath, digest, written), nil
}

// Open returns the immutable object for a validated SHA-256 digest.
func (s *Store) Open(digest string) (io.ReadCloser, error) {
	if !validDigest(digest) {
		return nil, errors.New("artifact digest must be 64 lowercase hexadecimal characters")
	}
	file, err := os.Open(s.pathFor(digest))
	if err != nil {
		return nil, fmt.Errorf("open artifact %s: %w", digest, err)
	}
	return file, nil
}

// Resolve verifies an immutable object against its content-addressed identity
// and returns the local path required by offline media inspectors. Callers must
// not trust a path derived from a digest without re-hashing the stored bytes:
// this method deliberately detects missing or externally corrupted objects.
func (s *Store) Resolve(ctx context.Context, digest string) (Artifact, error) {
	if !validDigest(digest) {
		return Artifact{}, errors.New("artifact digest must be 64 lowercase hexadecimal characters")
	}
	if err := ctx.Err(); err != nil {
		return Artifact{}, err
	}
	path := s.pathFor(digest)
	file, err := os.Open(path)
	if err != nil {
		return Artifact{}, fmt.Errorf("open artifact %s: %w", digest, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return Artifact{}, fmt.Errorf("inspect artifact %s: %w", digest, err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return Artifact{}, fmt.Errorf("artifact %s is not a regular file", digest)
	}
	hash := sha256.New()
	size, copyErr := copyWithContext(ctx, hash, file)
	closeErr := file.Close()
	if copyErr != nil {
		return Artifact{}, fmt.Errorf("verify artifact %s: %w", digest, copyErr)
	}
	if closeErr != nil {
		return Artifact{}, fmt.Errorf("close artifact %s: %w", digest, closeErr)
	}
	actual := hex.EncodeToString(hash.Sum(nil))
	if actual != digest {
		return Artifact{}, fmt.Errorf("artifact %s content digest mismatch", digest)
	}
	return artifactFor(path, digest, size), nil
}

// Exists reports whether a validated digest is present.
func (s *Store) Exists(digest string) (bool, error) {
	if !validDigest(digest) {
		return false, errors.New("artifact digest must be 64 lowercase hexadecimal characters")
	}
	_, err := os.Stat(s.pathFor(digest))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("inspect artifact %s: %w", digest, err)
}

func (s *Store) pathFor(digest string) string {
	return filepath.Join(s.root, "sha256", digest[:2], digest)
}

func validDigest(digest string) bool {
	if len(digest) != digestLength {
		return false
	}
	for _, r := range digest {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func artifactFor(path, digest string, size int64) Artifact {
	return Artifact{
		Digest: digest,
		Size:   size,
		URI:    "cas://sha256/" + digest,
		Path:   path,
	}
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	var written int64
	buffer := make([]byte, 128*1024)
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		n, readErr := src.Read(buffer)
		if n > 0 {
			wrote, writeErr := dst.Write(buffer[:n])
			written += int64(wrote)
			if writeErr != nil {
				return written, writeErr
			}
			if wrote != n {
				return written, io.ErrShortWrite
			}
		}
		if errors.Is(readErr, io.EOF) {
			return written, nil
		}
		if readErr != nil {
			return written, readErr
		}
	}
}
