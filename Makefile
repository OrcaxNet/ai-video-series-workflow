.PHONY: test provider-preflight video-bootstrap video-up video-up-tools video-down video-logs video-smoke video-integration-test video-postproduction-integration-test video-migration-v7-rollback-guard-test video-flo104-mock-evidence video-live-provider-up video-live-probe video-secret-scan video-test web-build web-test

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

# The live profile reads ARK_API_KEY only from the invoking environment. The
# probe output directory is single-use, preventing accidental paid re-submit.
video-live-provider-up: video-bootstrap
	@test -n "$${ARK_API_KEY:-}" || (echo "ARK_API_KEY must be injected at runtime" && exit 1)
	$(VIDEO_COMPOSE) --profile live up --build --wait volcengine-provider

video-live-probe: video-bootstrap
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
