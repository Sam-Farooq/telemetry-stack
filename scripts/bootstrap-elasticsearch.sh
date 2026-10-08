#!/usr/bin/env bash
# Install the ILM policies, the index templates and the first write index.
#
#   ./scripts/bootstrap-elasticsearch.sh                 # localhost:9200
#   ES=http://elasticsearch:9200 ./scripts/bootstrap-elasticsearch.sh
#
# Needs Elasticsearch reachable. It is safe to run twice: the policy and
# template PUTs replace, and the bootstrap index is skipped if the alias
# already points somewhere.
set -euo pipefail

ES="${ES:-http://localhost:9200}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

put() {
  local path="$1" file="$2"
  printf 'PUT %s\n' "$path"
  curl -sS -f -X PUT "$ES$path" -H 'content-type: application/json' \
    --data-binary "@$file" >/dev/null
}

bootstrap_index() {
  local alias="$1" prefix="$2"
  if curl -sS -f -o /dev/null "$ES/_alias/$alias" 2>/dev/null; then
    printf 'alias %s already exists\n' "$alias"
    return 0
  fi
  printf 'creating %s-000001 with write alias %s\n' "$prefix" "$alias"
  curl -sS -f -X PUT "$ES/%3C$prefix-000001%3E" \
    -H 'content-type: application/json' \
    --data-binary "{\"aliases\":{\"$alias\":{\"is_write_index\":true}}}" >/dev/null
}

curl -sS -f "$ES/_cluster/health?wait_for_status=yellow&timeout=30s" >/dev/null

put "/_ilm/policy/logs-otel"   "$HERE/elasticsearch/ilm-logs.json"
put "/_ilm/policy/traces-otel" "$HERE/elasticsearch/ilm-traces.json"
put "/_index_template/logs-otel"   "$HERE/elasticsearch/template-logs.json"
put "/_index_template/traces-otel" "$HERE/elasticsearch/template-traces.json"

bootstrap_index "logs-otel-write"   "logs-otel"
bootstrap_index "traces-otel-write" "traces-otel"

printf 'done\n'
