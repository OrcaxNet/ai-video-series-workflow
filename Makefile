.PHONY: test provider-preflight video-bootstrap video-up video-up-tools video-down video-logs video-smoke video-secret-scan video-test web-build web-test

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
