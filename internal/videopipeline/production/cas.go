package production

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/OrcaxNet/ai-video-series-workflow/internal/providercontract"
	"github.com/OrcaxNet/ai-video-series-workflow/internal/videopipeline/artifactstore"
)

// DownloadingCASCommitter consumes a provider's temporary signed URL, streams
// it into local CAS, verifies any provider-supplied digest, and returns only a
// durable cas:// identity. The signed URL is never copied to a manifest.
type DownloadingCASCommitter struct {
	Store    *artifactstore.Store
	Client   *http.Client
	MaxBytes int64
}

func (c DownloadingCASCommitter) Commit(ctx context.Context, asset providercontract.AssetRef) (providercontract.AssetRef, error) {
	if strings.HasPrefix(asset.URI, "cas://sha256/") {
		return RequireCASCommitter{}.Commit(ctx, asset)
	}
	if c.Store == nil || c.Client == nil || c.MaxBytes <= 0 {
		return providercontract.AssetRef{}, validationf("CAS store, bounded HTTP client, and positive download limit are required")
	}
	if !nonEmpty(asset.ID, asset.Revision, asset.URI, asset.LicenseReference) {
		return providercontract.AssetRef{}, validationf("provider output identity, URI, revision, and license are required")
	}
	parsed, err := url.Parse(asset.URI)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") ||
		parsed.Host == "" || parsed.User != nil {
		return providercontract.AssetRef{}, policyf("provider artifact URL must be an absolute credential-free HTTP(S) URL")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return providercontract.AssetRef{}, fmt.Errorf("create provider artifact request: %w", err)
	}
	response, err := c.Client.Do(request)
	if err != nil {
		return providercontract.AssetRef{}, fmt.Errorf("download provider artifact: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return providercontract.AssetRef{}, fmt.Errorf("download provider artifact: HTTP %d", response.StatusCode)
	}
	if response.ContentLength > c.MaxBytes {
		return providercontract.AssetRef{}, policyf("provider artifact exceeds the configured download limit")
	}
	limited := &io.LimitedReader{R: response.Body, N: c.MaxBytes + 1}
	committed, err := c.Store.Put(ctx, limited)
	if err != nil {
		return providercontract.AssetRef{}, err
	}
	if limited.N <= 0 {
		return providercontract.AssetRef{}, policyf("provider artifact exceeds the configured download limit")
	}
	if validSHA256(asset.SHA256) && asset.SHA256 != committed.Digest {
		return providercontract.AssetRef{}, conflictf("provider artifact digest does not match downloaded bytes")
	}
	asset.SHA256 = committed.Digest
	asset.Revision = committed.Digest
	asset.URI = committed.URI
	asset.SizeBytes = committed.Size
	if asset.MediaType == "" {
		asset.MediaType = strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0])
	}
	if asset.MediaType == "" {
		asset.MediaType = "application/octet-stream"
	}
	return RequireCASCommitter{}.Commit(ctx, asset)
}
