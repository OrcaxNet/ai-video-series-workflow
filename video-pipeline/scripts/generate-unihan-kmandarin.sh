#!/bin/sh
set -eu

archive=${1:-}
output=${2:-}
expected_archive_sha=f7a48b2b545acfaa77b2d607ae28747404ce02baefee16396c5d2d7a8ef34b5e

if [ -z "$archive" ] || [ -z "$output" ]; then
  echo "usage: generate-unihan-kmandarin.sh <Unihan-17.0.0.zip> <new-output.txt>" >&2
  exit 2
fi
if [ -e "$output" ]; then
  echo "refusing to overwrite $output" >&2
  exit 1
fi

actual_archive_sha=$(shasum -a 256 "$archive" | awk '{print $1}')
if [ "$actual_archive_sha" != "$expected_archive_sha" ]; then
  echo "Unihan archive SHA-256 mismatch" >&2
  exit 1
fi

temporary="${output}.tmp.$$"
trap 'rm -f "$temporary"' EXIT HUP INT TERM
{
  echo '# Generated from Unicode 17.0.0 Unihan_Readings.txt.'
  echo '# Source: https://www.unicode.org/Public/17.0.0/ucd/Unihan.zip'
  echo '# Source archive SHA-256: f7a48b2b545acfaa77b2d607ae28747404ce02baefee16396c5d2d7a8ef34b5e'
  echo '# Property: kMandarin'
  unzip -p "$archive" Unihan_Readings.txt | awk -F '\t' '$2 == "kMandarin" { print $0 }'
} > "$temporary"
mv "$temporary" "$output"
trap - EXIT HUP INT TERM
