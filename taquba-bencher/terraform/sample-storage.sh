#!/usr/bin/env bash
# Samples object count and bytes under an S3 prefix every interval_sec,
# appending epoch,objects,bytes rows to a CSV to track storage growth
# over a long run. On a missing CLI or aws error it appends an ERR row
# (and an aws-err log) instead of 0,0, so 0,0 means a genuinely empty
# prefix.
#
# Usage: sample-storage.sh s3://bucket/prefix out.csv [interval_sec]
set -euo pipefail

uri="${1:?usage: sample-storage.sh s3://bucket/prefix out.csv [interval_sec]}"
out="${2:?usage: sample-storage.sh s3://bucket/prefix out.csv [interval_sec]}"
interval="${3:-300}"
err_log="${out%.csv}.aws-err.log"

if ! command -v aws >/dev/null 2>&1; then
  echo "sample-storage: aws CLI not found on PATH" >&2
  exit 1
fi

echo "epoch,objects,bytes" > "$out"
while true; do
  epoch=$(date +%s)
  # --summarize appends "Total Objects:" and "Total Size:" lines.
  if summary=$(aws s3 ls --summarize --recursive "$uri" 2>>"$err_log"); then
    objects=$(printf '%s\n' "$summary" | awk '/Total Objects:/ {print $3}')
    bytes=$(printf '%s\n' "$summary" | awk '/Total Size:/ {print $3}')
    echo "$epoch,${objects:-ERR},${bytes:-ERR}" >> "$out"
  else
    echo "$epoch,ERR,ERR" >> "$out"
    echo "sample-storage: aws s3 ls failed at $epoch (see $err_log)" >&2
  fi
  sleep "$interval"
done
