#!/bin/sh
set -eu

control_plane_url="${VIDEO_CONTROL_PLANE_URL:-http://127.0.0.1:18080}"
provider_url="${VIDEO_PROVIDER_ADAPTER_URL:-http://127.0.0.1:8090}"

info="$(curl -fsS "${control_plane_url}/api/v1/system/info")"
printf '%s' "${info}" | grep -Fq '"generationExecution":"remote-provider-api"'
printf '%s' "${info}" | grep -Fq '"gpuRequired":false'

ready="$(curl -fsS "${control_plane_url}/health/ready")"
printf '%s' "${ready}" | grep -Fq '"status":"ready"'

provider_status="$(curl -fsS "${control_plane_url}/api/v1/providers/status")"
printf '%s' "${provider_status}" | grep -Fq '"mode":"dry-run"'
test "$(printf '%s' "${provider_status}" | grep -o '"liveConfigured":false' | wc -l | tr -d ' ')" = "4"

payload='{"schema_version":"v1","job_id":"smoke-job","run_id":"smoke-run","capability":"video.primary","input_hash":"0000000000000000000000000000000000000000000000000000000000000000","model_snapshot":{"capability_alias":"video.primary","provider":"fake","model_id":"fixture-video-v1","route_version":"mock-routes-v1","capability_hash":"0000000000000000000000000000000000000000000000000000000000000000","verification":"mock_only"},"request":{"request_id":"smoke-request","idempotency_key":"smoke-job","modality":"video","prompt":"smoke fixture","prompt_snapshot_id":"smoke-prompt-v1","context":{"series_snapshot_id":"series-context-v1","episode_snapshot_id":"episode-context-v1","scene_snapshot_id":"scene-context-v1","shot_snapshot_id":"shot-context-v1"},"output":{"width":1280,"height":720,"aspect_ratio":"16:9","fps":24,"duration_millis":5000,"format":"mp4"},"budget":{"estimated_cost_micros":100,"max_cost_micros":150,"max_attempts":1}},"budget_reservation":{"reservation_id":"smoke-budget","currency":"CNY","amount_micros":150,"pricing_version":"mock-pricing-v1","confirmed_by":"smoke-reviewer","binding_hash":"eca6cdd0d058692fdec0593cd73b96b6c89151836c394de4b5a93ceeaa189510"},"trace_id":"smoke-trace"}'
first="$(curl -fsS -H 'Content-Type: application/json' -H 'Idempotency-Key: smoke-job' --data "${payload}" "${provider_url}/v1/jobs")"
second="$(curl -fsS -H 'Content-Type: application/json' -H 'Idempotency-Key: smoke-job' --data "${payload}" "${provider_url}/v1/jobs")"
first_task="$(printf '%s' "${first}" | sed -n 's/.*"upstream_task_id":"\([^"]*\)".*/\1/p')"
second_task="$(printf '%s' "${second}" | sed -n 's/.*"upstream_task_id":"\([^"]*\)".*/\1/p')"
test -n "${first_task}"
test "${first_task}" = "${second_task}"
completed="$(curl -fsS "${provider_url}/v1/jobs/smoke-job")"
printf '%s' "${completed}" | grep -Fq '"state":"succeeded"'
printf '%s' "${completed}" | grep -Eq '"uri":"cas://sha256/[0-9a-f]{64}"'

postgres_container="${VIDEO_POSTGRES_CONTAINER:-ai-video-series-workflow-postgres-1}"
migration_version="$(docker exec "${postgres_container}" psql -U video -d video_pipeline -Atc 'SELECT version FROM public.schema_migrations;')"
migration_dirty="$(docker exec "${postgres_container}" psql -U video -d video_pipeline -Atc 'SELECT dirty FROM public.schema_migrations;')"
test "${migration_version}" = "11"
test "${migration_dirty}" = "f"
table_count="$(docker exec "${postgres_container}" psql -U video -d video_pipeline -Atc "SELECT count(*) FROM information_schema.tables WHERE table_schema='video_pipeline';")"
test "${table_count}" -ge 59

postgres_user="${VIDEO_POSTGRES_USER:-video}"
postgres_password="${VIDEO_POSTGRES_PASSWORD:-video-local-only}"
postgres_database="${VIDEO_POSTGRES_DATABASE:-video_pipeline}"
postgres_port="${VIDEO_POSTGRES_PORT:-55432}"
temporal_port="${VIDEO_TEMPORAL_PORT:-7233}"
VIDEO_TEST_POSTGRES_DSN="postgres://${postgres_user}:${postgres_password}@127.0.0.1:${postgres_port}/${postgres_database}?sslmode=disable" \
  go test -count=1 -tags=integration ./internal/videopipeline/repository
VIDEO_TEST_POSTGRES_DSN="postgres://${postgres_user}:${postgres_password}@127.0.0.1:${postgres_port}/${postgres_database}?sslmode=disable" \
  go test -count=1 -tags=integration -run 'TestFLO167(MaterializesIdenticallyAcrossFreshPostgresAndReplay|RunnerPaidBoundary|ControlledRetryIsBoundOnceAndConcurrentSubmitPostsOnce)' \
    ./internal/videopipeline/stage1materialize
VIDEO_TEST_POSTGRES_DSN="postgres://${postgres_user}:${postgres_password}@127.0.0.1:${postgres_port}/${postgres_database}?sslmode=disable" \
VIDEO_TEST_TEMPORAL_ADDRESS="127.0.0.1:${temporal_port}" \
VIDEO_TEST_PROVIDER_URL="${provider_url}" \
VIDEO_TEST_PROVIDER_CONTAINER="${VIDEO_MOCK_PROVIDER_CONTAINER:-ai-video-series-workflow-mock-provider-1}" \
VIDEO_TEST_WORKER_CONTAINER="${VIDEO_WORKER_CONTAINER:-ai-video-series-workflow-orchestrator-worker-1}" \
VIDEO_TEST_COMPOSE_TASK_QUEUE="video-production-v1" \
  go test -count=1 -tags=integration \
    -run TestPostgres_WorkflowProjectionClosesQ1AndManifestLineage \
    ./internal/videopipeline/repository

temporal_container="${VIDEO_TEMPORAL_CONTAINER:-ai-video-series-workflow-temporal-1}"
temporal_ip="$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${temporal_container}")"
workflow_id="video-smoke-$(date +%s)"
temporal_address="${temporal_ip}:7233"

docker exec "${temporal_container}" tctl --address "${temporal_address}" workflow start \
  --taskqueue video-production-v1 \
  --workflow_id "${workflow_id}" \
  --workflow_type video.production.episode.v1 \
  --workflowidreusepolicy RejectDuplicate \
  --execution_timeout 300 \
  --input '{"schemaVersion":"v1","seriesId":"series-smoke","episodeRevisionId":"episode-revision-smoke","shotSpecRevisionIds":["shot-revision-smoke"],"generationProfileRef":"profile-revision-smoke","gate2DecisionId":"gate2-smoke","providerRoute":{"capability_alias":"video.primary","provider":"fake","model_id":"fixture-video-v1","route_version":"mock-routes-v1","capability_hash":"0000000000000000000000000000000000000000000000000000000000000000","verification":"mock_only"},"budgetApprovalId":"budget-smoke","budgetMaximumMicros":500,"budgetCurrency":"CNY","traceId":"trace-smoke"}' >/dev/null

workflow_status=""
status_attempt=0
while [ "${status_attempt}" -lt 30 ]; do
  workflow_status="$(docker exec "${temporal_container}" tctl --address "${temporal_address}" workflow query \
    --workflow_id "${workflow_id}" \
    --query_type video.production.status.v1 2>/dev/null || true)"
  if printf '%s' "${workflow_status}" | grep -Fq '"state":"WAITING_G3"'; then
    break
  fi
  status_attempt=$((status_attempt + 1))
  sleep 1
done
printf '%s' "${workflow_status}" | grep -Fq '"state":"WAITING_G3"'

# Temporal history, not worker memory, is the source of truth. Restart after
# provider/CAS/QC and prove the replacement process consumes the durable signal.
worker_container="${VIDEO_WORKER_CONTAINER:-ai-video-series-workflow-orchestrator-worker-1}"
docker restart "${worker_container}" >/dev/null
docker exec "${temporal_container}" tctl --address "${temporal_address}" workflow signal \
  --workflow_id "${workflow_id}" \
  --name video.production.gate3-decision.v1 \
  --input '{"decisionId":"gate3-smoke","approved":true,"actorId":"reviewer-smoke"}' >/dev/null
workflow_result="$(docker exec "${temporal_container}" tctl --address "${temporal_address}" workflow observe --workflow_id "${workflow_id}")"
printf '%s' "${workflow_result}" | grep -Fq '"state":"LOCKED"'

./video-pipeline/scripts/test-migration-v7-rollback-guard.sh

echo "video-pipeline smoke passed"
