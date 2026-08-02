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

Re-running the same command performs an identity-checked replay. A partial prior materialization, changed package, approval drift, duplicate/missing asset or shot, route/budget drift, or any paid-boundary record fails closed.

## Independent integration gate

```sh
VIDEO_TEST_POSTGRES_DSN='postgres://…' \
VIDEO_TEST_FLO100_PACK_PATH='./FLO-100-stage3-offline-pack-v1' \
make video-flo100-materialize-integration-test
```

The test asserts the fresh materialization, replay stability, 3/30/8 counts, 30 intent idempotency keys, tamper rejection without a durable side effect, and zero Provider jobs/reservations/cost entries.
