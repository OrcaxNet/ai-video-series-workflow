# FLO-167 provider-free verification

- Baseline: `35f87d664e26b74bc1afb176f40c77405a9997ae`
- No Provider client, credential, reservation, cost row, job, or external request is constructed by the normalization/package tests.
- Canonical package content hash: `75d039d98dba6762e1d5f34d427762377ecdf9f3c19f22a55a577b0b8adf272b`.
- A01 is preserved as the sole legacy terminal: actual `2007900`, expected `2003760`, delta `4140` milli-AFP (`+0.2066%` for display only), `87300` video tokens, zero cash. A01 submission is permanently rejected.
- A02 is exactly allowed at `2254230` milli-AFP. Inclusive `+/-10%` boundaries pass; one milli-AFP beyond either boundary is rejected.
- Checked multiplication/addition rejects overflow. Unknown versions and duration, price snapshot, route, G1, G2, SAFETY, canonical hash, semantic hash, or completed-set drift fail validation.
- Migration `000010` adds immutable supersession, per-shot binding, authorization, and idempotent submission projections. Its down migration works on a fresh/empty projection and refuses to erase existing supersession lineage.
- Tests: `go test ./...`, `go test -race ./...`, and `video-pipeline/scripts/check-secrets.sh`.
