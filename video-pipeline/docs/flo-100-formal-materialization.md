# FLO-100 formal offline materialization

`video-stage1-materialize --formal-pack` imports the fixed GOLD A/B/C package into PostgreSQL and CAS, then emits three prompt-free execution packages plus one verification report. The formal path has no Provider client, credential input, Adapter URL, submit operation, budget reservation, or cost-ledger write.

It is intentionally not a live-activation command. The imported provider profiles and capabilities are disabled/DRAFT, G1/G2 and budget reviews remain pending independent QA, every plan is `BLOCKED`, and the missing current quota snapshot remains an explicit blocker.

## Fixed input

Use the package attached to FLO-100 with content hash:

```text
68f0a07e2ea2cd2740da07daca3bb2ce2d1a7572ed9a8756cd73101db7fbd835
```

The materializer verifies the independently pinned raw file-manifest hash, all 50 checksum entries, all three product/plan/intent bindings, 30 unique shots and intent keys, eight exact visual AssetVersions, CAS hashes, license/safety evidence, zero-cash budgets, quota absence, and A -> B -> C order before writing.

## Run

Apply the repository migrations to a fresh PostgreSQL database, then run:

```sh
VIDEO_POSTGRES_DSN='postgres://…' \
VIDEO_ARTIFACT_ROOT='./artifacts/flo100-cas' \
go run ./cmd/video-stage1-materialize \
  --formal-pack './FLO-100-stage3-offline-pack-v1' \
  --expected-package-hash '68f0a07e2ea2cd2740da07daca3bb2ce2d1a7572ed9a8756cd73101db7fbd835' \
  --output './artifacts/flo100-packages' \
  --report './artifacts/flo100-packages/flo100.formal-materialization-report.json' \
  --approval-comment '5b92b347-3ce9-4e7b-831a-1f00d1454d78' \
  --approval-actor '16bbc49e-750f-432d-9ba4-b33ef6812026' \
  --approval-valid-until '2026-08-31T15:59:59Z'
```

The output directory contains:

- `flo100-gold-a-v1.execution-package.json`
- `flo100-gold-b-v1.execution-package.json`
- `flo100-gold-c-v1.execution-package.json`

Re-running the same command performs an identity-checked replay. The first materialization seals a canonical hash of every frozen PostgreSQL projection in an immutable audit event. Replay and report generation both recompute that hash in a serializable read-only snapshot before returning data. The seal covers provider profiles and capabilities, generation profiles, assets/CAS/licenses and approval bindings, all gate and budget reviews, every shot/prompt/run/attempt and related audit, plus the absence of Provider jobs, reservations, and cost entries. A partial prior materialization, changed package, approval drift, duplicate/missing row, any field-level route/budget/asset/gate drift, or any paid-boundary record fails closed.

## Independent integration gate

```sh
VIDEO_TEST_POSTGRES_DSN='postgres://…' \
VIDEO_TEST_FLO100_PACK_PATH='./FLO-100-stage3-offline-pack-v1' \
make video-flo100-materialize-integration-test
```

The test asserts the fresh materialization, replay stability, 3/30/8 counts, 30 intent idempotency keys, tamper rejection without a durable side effect, and zero Provider jobs/reservations/cost entries. It also commits and restores three negative SQL probes—enabling the video profile for live use, raising the GOLD-A budget, and approving an asset while G1 remains returned—and requires each replay to fail without emitting a package, report, or durable audit.

## A-only live activation

FLO-165 adds a separate live projection; it does not update the offline rows or
their canonical seal. The live materializer accepts only the exact A-only
authorization envelope, persists the 8 G1 assets, 10 G2 shots and SAFETY
bindings, and emits an independently sealed readiness plan and execution
package. B, C and Stage 4 remain disabled.

```sh
VIDEO_POSTGRES_DSN='postgres://…' \
VIDEO_ARTIFACT_ROOT='./artifacts/flo100-live-cas' \
go run ./cmd/video-stage1-materialize \
  --formal-pack './FLO-100-stage3-offline-pack-v1' \
  --expected-package-hash '68f0a07e2ea2cd2740da07daca3bb2ce2d1a7572ed9a8756cd73101db7fbd835' \
  --live-authorization './FLO-100-batch-a-live-authorization-v1.json' \
  --expected-live-authorization-hash '<independently pinned sha256>' \
  --source-code-commit '<full candidate git sha>' \
  --live-plan-output './artifacts/flo100-live/readiness-plan.json' \
  --output './artifacts/flo100-live/execution-package.json' \
  --report './artifacts/flo100-live/verification-report.json' \
  --approval-comment '5b92b347-3ce9-4e7b-831a-1f00d1454d78' \
  --approval-actor '16bbc49e-750f-432d-9ba4-b33ef6812026' \
  --approval-valid-until '2026-08-31T15:59:59Z'
```

Materialization never contacts a Provider. If the authorization's fixed merge
commit differs from `--source-code-commit`, the live package and projection are
still available for independent review, but no submit authorization is created.
The built runner also verifies its clean Go VCS revision against the package, so
a matching JSON field alone cannot authorize drifted code.

At the first actual paid boundary, PostgreSQL locks the Agent Plan account,
requires a quota snapshot no older than 300 seconds, and atomically reserves
30,306.870 AFP video plus 1.039 AFP speech. The request is accepted only as
`subscription_included_only` with zero non-subscription CNY cash; monthly usage
plus external and local conservative reservations must not exceed 135,000 AFP.
There is no automatic retry or provider switch.

Provider-free integration coverage:

```sh
VIDEO_TEST_POSTGRES_DSN='postgres://…' \
VIDEO_TEST_FLO100_PACK_PATH='./FLO-100-stage3-offline-pack-v1' \
VIDEO_TEST_FLO100_LIVE_AUTHORIZATION_PATH='./FLO-100-batch-a-live-authorization-v1.json' \
make video-flo100-live-activation-integration-test
```

The test covers fresh/replay stability and fail-closed rejection of code,
authorization, package, projection, route, scope, cash and quota drift before
any provider job, cash reservation, cost row or external request is created.
