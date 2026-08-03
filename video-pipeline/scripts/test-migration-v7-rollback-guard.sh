#!/bin/sh
set -eu

postgres_container="${VIDEO_POSTGRES_CONTAINER:-ai-video-series-workflow-postgres-1}"
postgres_user="${VIDEO_POSTGRES_USER:-video}"
postgres_password="${VIDEO_POSTGRES_PASSWORD:-video-local-only}"
postgres_database="${VIDEO_POSTGRES_DATABASE:-video_pipeline}"
script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
migration_dir="${script_dir}/../db/migrations"
migration_dsn="postgres://${postgres_user}:${postgres_password}@127.0.0.1:5432/${postgres_database}?sslmode=disable"

psql_value() {
  docker exec "${postgres_container}" psql \
    -U "${postgres_user}" \
    -d "${postgres_database}" \
    -Atc "$1"
}

run_migrate() {
  docker run --rm \
    --network "container:${postgres_container}" \
    -v "${migration_dir}:/migrations:ro" \
    migrate/migrate:v4.17.1 \
    -path=/migrations \
    -database="${migration_dsn}" \
    "$@"
}

version_before="$(psql_value 'SELECT version FROM public.schema_migrations;')"
dirty_before="$(psql_value 'SELECT dirty FROM public.schema_migrations;')"
test "${version_before}" = "11"
test "${dirty_before}" = "f"
test "$(psql_value 'SELECT COUNT(*) FROM video_pipeline.stage1_live_activations;')" = "0"

# This regression specifically exercises the v7 guard. Migrations v8-v11
# permit rollback only before live/retry activation, so a clean smoke database
# can move to v7 for the probe and is restored to the latest version afterwards.
run_migrate down 4 >/dev/null
test "$(psql_value 'SELECT version FROM public.schema_migrations;')" = "7"

lineage_before="$(psql_value "
  SELECT COUNT(*)
  FROM video_pipeline.prompt_snapshot_inputs
  WHERE input_type = 'GENERATION_PROFILE';
")"
test "${lineage_before}" -gt 0

prompt_constraint_before="$(psql_value "
  SELECT pg_get_constraintdef(oid)
  FROM pg_constraint
  WHERE conrelid = 'video_pipeline.prompt_snapshot_inputs'::regclass
    AND conname = 'prompt_snapshot_inputs_input_type_check';
")"
printf '%s' "${prompt_constraint_before}" | grep -Fq "GENERATION_PROFILE"

manifest_unique_before="$(psql_value "
  SELECT COUNT(*)
  FROM pg_constraint
  WHERE conrelid = 'video_pipeline.publication_locks'::regclass
    AND conname = 'publication_locks_manifest_id_key';
")"
test "${manifest_unique_before}" = "0"

rollback_output=""
if rollback_output="$(run_migrate down 1 2>&1)"; then
  echo "unsafe v7 rollback unexpectedly succeeded" >&2
  exit 1
fi
printf '%s' "${rollback_output}" |
  grep -Fq "cannot roll back generation mainline upgrade while GENERATION_PROFILE lineage exists"

version_dirty="$(psql_value 'SELECT version FROM public.schema_migrations;')"
dirty_dirty="$(psql_value 'SELECT dirty FROM public.schema_migrations;')"
test "${version_dirty}" = "6"
test "${dirty_dirty}" = "t"

lineage_after="$(psql_value "
  SELECT COUNT(*)
  FROM video_pipeline.prompt_snapshot_inputs
  WHERE input_type = 'GENERATION_PROFILE';
")"
prompt_constraint_after="$(psql_value "
  SELECT pg_get_constraintdef(oid)
  FROM pg_constraint
  WHERE conrelid = 'video_pipeline.prompt_snapshot_inputs'::regclass
    AND conname = 'prompt_snapshot_inputs_input_type_check';
")"
manifest_unique_after="$(psql_value "
  SELECT COUNT(*)
  FROM pg_constraint
  WHERE conrelid = 'video_pipeline.publication_locks'::regclass
    AND conname = 'publication_locks_manifest_id_key';
")"
test "${lineage_after}" = "${lineage_before}"
test "${prompt_constraint_after}" = "${prompt_constraint_before}"
test "${manifest_unique_after}" = "${manifest_unique_before}"

# The failed statement was the first guarded statement and both v7 schema
# invariants are unchanged, so clearing only golang-migrate's dirty marker is
# safe in this disposable regression database.
run_migrate force 7 >/dev/null
run_migrate up >/dev/null 2>&1

version_recovered="$(psql_value 'SELECT version FROM public.schema_migrations;')"
dirty_recovered="$(psql_value 'SELECT dirty FROM public.schema_migrations;')"
test "${version_recovered}" = "11"
test "${dirty_recovered}" = "f"
test "$(psql_value "
  SELECT COUNT(*)
  FROM video_pipeline.prompt_snapshot_inputs
  WHERE input_type = 'GENERATION_PROFILE';
")" = "${lineage_before}"

echo "v7 protected rollback dirty-state recovery passed; migration v11 restored"
