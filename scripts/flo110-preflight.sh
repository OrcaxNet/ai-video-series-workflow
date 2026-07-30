#!/usr/bin/env bash
set -euo pipefail

go run ./cmd/provider-preflight --mode scan
go run ./cmd/provider-preflight --mode validate-plan
go test ./internal/providercontract
go run ./cmd/provider-preflight --mode mock

if [[ "${FLO110_LIVE:-0}" == "1" ]]; then
  go run ./cmd/provider-preflight --mode live-auth --confirm-live
else
  echo '{"evidence":"live_provider_call","status":"pending_key","checks":[]}'
fi
