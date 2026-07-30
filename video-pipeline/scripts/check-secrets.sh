#!/bin/sh
set -eu

go run ./cmd/provider-preflight --mode scan >/dev/null

git check-ignore -q video-pipeline/.env.video
grep -Fq 'video-pipeline/.env.video' .dockerignore

echo "video-pipeline tracked secret scan passed"
