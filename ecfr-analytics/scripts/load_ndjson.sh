#!/usr/bin/env bash
# Usage: ./scripts/load_ndjson.sh PROJECT DATASET TABLE FILE
set -euo pipefail

if [ $# -lt 4 ]; then
  echo "Usage: $0 PROJECT DATASET TABLE FILE.ndjson"
  exit 1
fi

PROJECT=$1
DATASET=$2
TABLE=$3
FILE=$4

bq --project_id "${PROJECT}" load   --source_format=NEWLINE_DELIMITED_JSON   --replace=false   "${DATASET}.${TABLE}"   "${FILE}"
