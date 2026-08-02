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
test "${version_before}" = "8"
test "${dirty_before}" = "f"

# v8 owns creator data that this guard must not delete. Temporarily point only
# golang-migrate's version marker at v7; the v8 schema remains untouched while
# we exercise v7's first-statement rollback guard below.
run_migrate force 7 >/dev/null

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

# The failed statement was the first guarded statement: v7 and v8 schema/data
# are unchanged, so restore only golang-migrate's v8 version marker.
run_migrate force 8 >/dev/null

version_recovered="$(psql_value 'SELECT version FROM public.schema_migrations;')"
dirty_recovered="$(psql_value 'SELECT dirty FROM public.schema_migrations;')"
test "${version_recovered}" = "8"
test "${dirty_recovered}" = "f"
test "$(psql_value "
  SELECT COUNT(*)
  FROM video_pipeline.prompt_snapshot_inputs
  WHERE input_type = 'GENERATION_PROFILE';
")" = "${lineage_before}"

echo "v7 protected rollback dirty-state recovery passed"
