#!/bin/sh
set -eu

script_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
web_dir="$(dirname "${script_dir}")"
runtime_image="${STUDIO_NGINX_RUNTIME_IMAGE:-nginxinc/nginx-unprivileged:1.27.4-alpine3.21}"
test_prefix="flo163-nginx-recovery-$$"
test_network="${test_prefix}-network"
backend_v1="${test_prefix}-control-plane-v1"
backend_v2="${test_prefix}-control-plane-v2"
studio="${test_prefix}-studio"

cleanup() {
  docker rm -f "${studio}" "${backend_v1}" "${backend_v2}" >/dev/null 2>&1 || true
  docker network rm "${test_network}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker network create "${test_network}" >/dev/null

docker run -d --name "${backend_v1}" \
  --network "${test_network}" \
  --network-alias control-plane \
  --mount "type=bind,src=${web_dir}/testdata/nginx-recovery/v1,dst=/usr/share/nginx/html,readonly" \
  "${runtime_image}" >/dev/null

docker run -d --name "${studio}" \
  --network "${test_network}" \
  --mount "type=bind,src=${web_dir}/nginx.conf,dst=/etc/nginx/conf.d/default.conf,readonly" \
  "${runtime_image}" >/dev/null

initial_attempt=0
initial_api=""
initial_health=""
while [ "${initial_attempt}" -lt 20 ]; do
  initial_api="$(docker exec "${studio}" wget -q -O - http://127.0.0.1:8080/api/v1/recovery-probe 2>/dev/null || true)"
  initial_health="$(docker exec "${studio}" wget -q -O - http://127.0.0.1:8080/health/studio 2>/dev/null || true)"
  if [ "${initial_api}" = "control-plane-v1" ] && [ "${initial_health}" = "ready-v1" ]; then
    break
  fi
  initial_attempt=$((initial_attempt + 1))
  sleep 1
done
test "${initial_api}" = "control-plane-v1"
test "${initial_health}" = "ready-v1"

studio_id="$(docker inspect -f '{{.Id}}' "${studio}")"
backend_v1_ip="$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${backend_v1}")"

# Attach the replacement before removing v1 so Docker must allocate a different
# address. Once v1 disappears, embedded DNS publishes only the replacement.
docker run -d --name "${backend_v2}" \
  --network "${test_network}" \
  --network-alias control-plane \
  --mount "type=bind,src=${web_dir}/testdata/nginx-recovery/v2,dst=/usr/share/nginx/html,readonly" \
  "${runtime_image}" >/dev/null
backend_v2_ip="$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${backend_v2}")"
test -n "${backend_v1_ip}"
test -n "${backend_v2_ip}"
test "${backend_v1_ip}" != "${backend_v2_ip}"

docker rm -f "${backend_v1}" >/dev/null

recovery_attempt=0
recovered_api=""
recovered_health=""
while [ "${recovery_attempt}" -lt 20 ]; do
  recovered_api="$(docker exec "${studio}" wget -q -O - http://127.0.0.1:8080/api/v1/recovery-probe 2>/dev/null || true)"
  recovered_health="$(docker exec "${studio}" wget -q -O - http://127.0.0.1:8080/health/studio 2>/dev/null || true)"
  if [ "${recovered_api}" = "control-plane-v2" ] && [ "${recovered_health}" = "ready-v2" ]; then
    break
  fi
  recovery_attempt=$((recovery_attempt + 1))
  sleep 1
done

test "${recovered_api}" = "control-plane-v2"
test "${recovered_health}" = "ready-v2"
test "$(docker inspect -f '{{.Id}}' "${studio}")" = "${studio_id}"

echo "studio nginx recovered after control-plane address change"
