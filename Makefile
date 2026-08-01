.PHONY: test provider-preflight video-bootstrap video-up video-up-tools video-down video-logs video-smoke video-integration-test video-postproduction-integration-test video-migration-v7-rollback-guard-test video-flo104-mock-evidence video-stage1-readiness video-stage1-runner-build video-live-provider-up video-live-probe video-secret-scan video-test web-build web-test

VIDEO_ENV := video-pipeline/.env.video
VIDEO_COMPOSE := docker compose --env-file $(VIDEO_ENV) -f video-pipeline/compose.yaml

test:
	go test ./...

provider-preflight:
	./scripts/flo110-preflight.sh

video-bootstrap:
	@test -f $(VIDEO_ENV) || cp video-pipeline/.env.video.example $(VIDEO_ENV)

video-up: video-bootstrap
	$(VIDEO_COMPOSE) up --build --wait

video-up-tools: video-bootstrap
	$(VIDEO_COMPOSE) --profile tools up --build --wait

video-down: video-bootstrap
	$(VIDEO_COMPOSE) down

video-logs: video-bootstrap
	$(VIDEO_COMPOSE) logs --tail=200

video-smoke:
	./video-pipeline/scripts/smoke.sh

video-integration-test:
	@test -n "$(VIDEO_TEST_POSTGRES_DSN)" || (echo "VIDEO_TEST_POSTGRES_DSN is required" && exit 1)
	go test -tags=integration ./internal/videopipeline/repository

video-postproduction-integration-test:
	go test -tags=integration ./internal/videopipeline/postproduction

video-migration-v7-rollback-guard-test:
	./video-pipeline/scripts/test-migration-v7-rollback-guard.sh

video-flo104-mock-evidence:
	./video-pipeline/scripts/flo104-mock-evidence.sh artifacts/flo104-mock

# Pure validation: this command has no Provider client and cannot incur cost.
video-stage1-readiness:
	go run ./cmd/video-stage1-readiness video-pipeline/config/flo104-stage1-readiness.json

# Build-only QA gate: does not start a container or contact an adapter.
video-stage1-runner-build:
	docker compose --env-file video-pipeline/.env.video.example -f video-pipeline/compose.yaml --profile live --profile stage1 build stage1-runner

# The live profile reads provider and internal service-auth secrets only from
# the invoking environment. The probe output directory is single-use.
video-live-provider-up: video-bootstrap
	@test -n "$${ARK_API_KEY:-}" || (echo "ARK_API_KEY must be injected at runtime" && exit 1)
	@test "$${#VIDEO_PROVIDER_SERVICE_AUTH_SECRET}" -ge 32 || (echo "VIDEO_PROVIDER_SERVICE_AUTH_SECRET must contain at least 32 bytes" && exit 1)
	$(VIDEO_COMPOSE) --profile live up --build --wait volcengine-provider

video-live-probe: video-bootstrap
	@test "$${#VIDEO_PROVIDER_SERVICE_AUTH_SECRET}" -ge 32 || (echo "VIDEO_PROVIDER_SERVICE_AUTH_SECRET must contain at least 32 bytes" && exit 1)
	$(VIDEO_COMPOSE) --profile live run --build --rm live-probe

video-secret-scan:
	./video-pipeline/scripts/check-secrets.sh

video-test:
	go test -race ./...
	go vet ./...
	docker compose --env-file video-pipeline/.env.video.example -f video-pipeline/compose.yaml config --quiet
	./scripts/flo110-preflight.sh
	./video-pipeline/scripts/check-secrets.sh

web-build:
	cd web && npm ci && npm run build

web-test:
	cd web && npm ci && npx playwright install chromium && npm run test
