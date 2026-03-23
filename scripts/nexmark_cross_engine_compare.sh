#!/usr/bin/env bash
set -uo pipefail

ENGINE="${1:-all}"
QUERY_SELECTOR="${2:-all}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ARTIFACT_ROOT="${ARTIFACT_ROOT:-${REPO_ROOT}/target/third_party_engine_benchmarks_nexmark}"
RUN_ID="$(date +%s%3N)"
NETWORK_NAME="${NETWORK_NAME:-floe-stream-bench-net}"

BID_ROWS="${BID_ROWS:-100000}"
AUCTION_ROWS="${AUCTION_ROWS:-10000}"
PERSON_ROWS="${PERSON_ROWS:-10000}"

POLL_INTERVAL_MS="${POLL_INTERVAL_MS:-250}"
POLL_TIMEOUT_MS="${POLL_TIMEOUT_MS:-240000}"
POLL_ATTEMPTS=$(((POLL_TIMEOUT_MS + POLL_INTERVAL_MS - 1) / POLL_INTERVAL_MS))
PG_QUERY_TIMEOUT_SECONDS="${PG_QUERY_TIMEOUT_SECONDS:-5}"

BROKER_PORT="${BROKER_PORT:-19092}"
BROKER_ADDR="127.0.0.1:${BROKER_PORT}"
REDPANDA_CONTAINER="${REDPANDA_CONTAINER:-floe-stream-bench-redpanda}"
BROKER_ADDR_FROM_CONTAINER="${REDPANDA_CONTAINER}:9092"
REDPANDA_IMAGE="${REDPANDA_IMAGE:-docker.redpanda.com/redpandadata/redpanda:latest}"

MATERIALIZE_CONTAINER="${MATERIALIZE_CONTAINER:-floe-stream-bench-materialize}"
MATERIALIZE_IMAGE="${MATERIALIZE_IMAGE:-materialize/materialized:v26.14.1}"
MATERIALIZE_SQL_PORT="${MATERIALIZE_SQL_PORT:-16875}"
MATERIALIZE_CLUSTER_SIZE="${MATERIALIZE_CLUSTER_SIZE:-25cc}"
MATERIALIZE_BEST_EFFORT_IN_MEMORY="${MATERIALIZE_BEST_EFFORT_IN_MEMORY:-1}"

RISINGWAVE_CONTAINER="${RISINGWAVE_CONTAINER:-floe-stream-bench-risingwave}"
RISINGWAVE_IMAGE="${RISINGWAVE_IMAGE:-risingwavelabs/risingwave:latest}"
RISINGWAVE_SQL_PORT="${RISINGWAVE_SQL_PORT:-14566}"
RISINGWAVE_IN_MEMORY="${RISINGWAVE_IN_MEMORY:-1}"

FELDERA_CONTAINER="${FELDERA_CONTAINER:-floe-stream-bench-feldera}"
FELDERA_IMAGE="${FELDERA_IMAGE:-ghcr.io/feldera/pipeline-manager:latest}"
FELDERA_HTTP_PORT="${FELDERA_HTTP_PORT:-18080}"
FELDERA_WORKERS="${FELDERA_WORKERS:-4}"
FELDERA_BEST_EFFORT_IN_MEMORY="${FELDERA_BEST_EFFORT_IN_MEMORY:-1}"
FELDERA_MIN_STORAGE_BYTES="${FELDERA_MIN_STORAGE_BYTES:-1099511627776}"
FELDERA_MIN_STEP_STORAGE_BYTES="${FELDERA_MIN_STEP_STORAGE_BYTES:-1099511627776}"
KAFKA_LATENCY_FETCH_PROFILE="${KAFKA_LATENCY_FETCH_PROFILE:-1}"
KAFKA_FETCH_WAIT_MAX_MS="${KAFKA_FETCH_WAIT_MAX_MS:-1}"
KAFKA_FETCH_QUEUE_BACKOFF_MS="${KAFKA_FETCH_QUEUE_BACKOFF_MS:-1}"
KAFKA_FETCH_MIN_BYTES="${KAFKA_FETCH_MIN_BYTES:-1}"

FLOE_PG_PORT="${FLOE_PG_PORT:-16432}"
FLOE_NODE_PID=""
FLOE_KAFKA_GROUP_ID_PREFIX="${FLOE_KAFKA_GROUP_ID_PREFIX:-floe-stream-bench}"
FLOE_KAFKA_POLL_MS="${FLOE_KAFKA_POLL_MS:-10}"
FLOE_KAFKA_MAX_MESSAGES_PER_TICK="${FLOE_KAFKA_MAX_MESSAGES_PER_TICK:-16384}"
FLOE_INGEST_QUEUE_CAPACITY="${FLOE_INGEST_QUEUE_CAPACITY:-262144}"
FLOE_INGEST_BATCH_SIZE="${FLOE_INGEST_BATCH_SIZE:-16384}"
FLOE_INGEST_BATCH_PER_SOURCE="${FLOE_INGEST_BATCH_PER_SOURCE:-16384}"
FLOE_INGEST_BATCH_PER_CONNECTOR="${FLOE_INGEST_BATCH_PER_CONNECTOR:-16384}"
FLOE_MV_RETAIN_LAST="${FLOE_MV_RETAIN_LAST:-256}"
FLOE_L0_SST_BYTES="${FLOE_L0_SST_BYTES:-1073741824}"
FLOE_MAX_UNFLUSHED_BYTES="${FLOE_MAX_UNFLUSHED_BYTES:-8589934592}"

KEEP_CONTAINERS="${KEEP_CONTAINERS:-0}"

RUN_DIR="${ARTIFACT_ROOT}/${RUN_ID}"
RESULTS_FILE="${RUN_DIR}/summary.md"
RESULTS_JSONL="${RUN_DIR}/results.jsonl"
mkdir -p "${RUN_DIR}"

declare -a CANONICAL_NEXMARK_QUERY_IDS=(
  q0 q1 q2 q3 q4 q5 q6 q7 q8 q9 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22
)

log() {
  printf '[nexmark-cross-engine] %s\n' "$*"
}

die() {
  printf '[nexmark-cross-engine] ERROR: %s\n' "$*" >&2
  exit 1
}

sleep_ms() {
  local millis="$1"
  sleep "$(awk "BEGIN { printf \"%.3f\", ${millis} / 1000 }")"
}

env_enabled() {
  case "${1}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

has_source() {
  local sources="$1"
  local needle="$2"
  [[ " ${sources} " == *" ${needle} "* ]]
}

source_rows() {
  case "$1" in
    bid) printf '%s\n' "${BID_ROWS}" ;;
    auction) printf '%s\n' "${AUCTION_ROWS}" ;;
    person) printf '%s\n' "${PERSON_ROWS}" ;;
    *) printf '0\n' ;;
  esac
}

required_sources_for_query() {
  case "$1" in
    q3) printf 'auction person\n' ;;
    q4|q6|q9|q13|q20) printf 'bid auction\n' ;;
    q8) printf 'person\n' ;;
    *) printf 'bid\n' ;;
  esac
}

input_rows_total_for_sources() {
  local sources="$1"
  local total=0
  if has_source "${sources}" bid; then
    total=$((total + BID_ROWS))
  fi
  if has_source "${sources}" auction; then
    total=$((total + AUCTION_ROWS))
  fi
  if has_source "${sources}" person; then
    total=$((total + PERSON_ROWS))
  fi
  printf '%s\n' "${total}"
}

floe_result_row_target_for_query() {
  case "$1" in
    q15) printf '1\n' ;;
    q16) printf '5\n' ;;
    q17) printf '10000\n' ;;
    *) printf '\n' ;;
  esac
}

query_sql() {
  case "$1" in
    q0)
      cat <<'SQL'
SELECT auction, bidder, price, channel, url, "dateTime", extra FROM bid
SQL
      ;;
    q1)
      cat <<'SQL'
SELECT auction, bidder, price * 89 / 100 AS converted_price, "dateTime", extra FROM bid
SQL
      ;;
    q2)
      cat <<'SQL'
SELECT auction, price FROM bid WHERE auction % 123 = 0
SQL
      ;;
    q3)
      cat <<'SQL'
SELECT p.name, p.city, p.state, a.id FROM auction AS a JOIN person AS p ON a.seller = p.id WHERE a.category = 10 AND p.state IN ('or', 'id', 'ca')
SQL
      ;;
    q4)
      cat <<'SQL'
SELECT category, AVG(max) FROM (SELECT MAX(b.price) AS max, a.category FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires GROUP BY a.id, a.category) per_auction GROUP BY category
SQL
      ;;
    q5)
      cat <<'SQL'
SELECT auction, COUNT(*) AS num FROM bid GROUP BY auction, HOP("dateTime", 2000, 10000)
SQL
      ;;
    q6)
      cat <<'SQL'
SELECT seller, AVG(price) AS moving_avg_price FROM (SELECT a.seller, b.price, b."dateTime", ROW_NUMBER() OVER (PARTITION BY a.id, a.seller ORDER BY b.price DESC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1 GROUP BY seller
SQL
      ;;
    q7)
      cat <<'SQL'
SELECT MAX(price) AS maxprice FROM bid GROUP BY TUMBLE("dateTime", 10000)
SQL
      ;;
    q8)
      cat <<'SQL'
SELECT id, name, COUNT(*) AS person_count FROM person GROUP BY id, name, TUMBLE("dateTime", 10000)
SQL
      ;;
    q9)
      cat <<'SQL'
SELECT id, "itemName", description, "initialBid", reserve, "dateTime", expires, seller, category, extra, auction, bidder, price, "bidTime", "bidExtra" FROM (SELECT a.id, a."itemName", a.description, a."initialBid", a.reserve, a."dateTime", a.expires, a.seller, a.category, a.extra, b.auction, b.bidder, b.price, b."dateTime" AS "bidTime", b.extra AS "bidExtra", ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.price DESC, b."dateTime" ASC) AS rownum FROM auction a JOIN bid b ON a.id = b.auction WHERE b."dateTime" BETWEEN a."dateTime" AND a.expires) ranked WHERE rownum <= 1
SQL
      ;;
    q12)
      cat <<'SQL'
SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, TUMBLE("dateTime", 10000)
SQL
      ;;
    q13)
      cat <<'SQL'
SELECT b.auction, b.bidder, b.price, b."dateTime", a.seller AS value FROM (SELECT *, PROCTIME() AS p_time FROM bid) b JOIN auction AS a ON b.auction = a.id WHERE b.auction % 10000 = a.id % 10000
SQL
      ;;
    q14)
      cat <<'SQL'
SELECT auction, bidder, price * 908 / 1000 AS price, CASE WHEN HOUR("dateTime") >= 8 AND HOUR("dateTime") <= 18 THEN 'dayTime' WHEN HOUR("dateTime") <= 6 OR HOUR("dateTime") >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bid_time_type, "dateTime", extra, COUNT_CHAR(extra, 'c') AS c_counts FROM bid WHERE price * 908 / 1000 > 1000000 AND price * 908 / 1000 < 50000000
SQL
      ;;
    q15)
      cat <<'SQL'
SELECT DATE_FORMAT("dateTime", 'yyyy-MM-dd') AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY DATE_FORMAT("dateTime", 'yyyy-MM-dd')
SQL
      ;;
    q16)
      cat <<'SQL'
SELECT channel, DATE_FORMAT("dateTime", 'yyyy-MM-dd') AS day, MAX(DATE_FORMAT("dateTime", 'HH:mm')) AS minute, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY channel, DATE_FORMAT("dateTime", 'yyyy-MM-dd')
SQL
      ;;
    q17)
      cat <<'SQL'
SELECT auction, DATE_FORMAT("dateTime", 'yyyy-MM-dd') AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, AVG(price) AS avg_price, SUM(price) AS sum_price FROM bid GROUP BY auction, DATE_FORMAT("dateTime", 'yyyy-MM-dd')
SQL
      ;;
    q18)
      cat <<'SQL'
SELECT auction, bidder, price, channel, url, "dateTime", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY bidder, auction ORDER BY "dateTime" DESC) AS rank_number FROM bid) dedup WHERE rank_number <= 1
SQL
      ;;
    q19)
      cat <<'SQL'
SELECT auction, bidder, price, channel, url, "dateTime", extra FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY auction ORDER BY price DESC) AS rank_number FROM bid) ranked WHERE rank_number <= 10
SQL
      ;;
    q20)
      cat <<'SQL'
SELECT b.auction, b.bidder, b.price, b.channel, b.url, b."dateTime", b.extra, a."itemName", a.description, a."initialBid", a.reserve, a."dateTime" AS auction_time, a.expires, a.seller, a.category, a.extra AS auction_extra FROM bid AS b JOIN auction AS a ON b.auction = a.id WHERE a.category = 10
SQL
      ;;
    q21)
      cat <<'SQL'
SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) END AS channel_id FROM bid WHERE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')
SQL
      ;;
    q22)
      cat <<'SQL'
SELECT auction, bidder, price, channel, SPLIT_INDEX(url, '/', 3) AS dir1, SPLIT_INDEX(url, '/', 4) AS dir2, SPLIT_INDEX(url, '/', 5) AS dir3 FROM bid
SQL
      ;;
    *)
      return 1
      ;;
  esac
}

query_sql_portable() {
  case "$1" in
    q5)
      cat <<'SQL'
SELECT auction, COUNT(*) AS num
FROM (
  SELECT auction, (("dateTime" / 2000) * 2000 - 0) AS hop_start FROM bid
  UNION ALL
  SELECT auction, (("dateTime" / 2000) * 2000 - 2000) AS hop_start FROM bid
  UNION ALL
  SELECT auction, (("dateTime" / 2000) * 2000 - 4000) AS hop_start FROM bid
  UNION ALL
  SELECT auction, (("dateTime" / 2000) * 2000 - 6000) AS hop_start FROM bid
  UNION ALL
  SELECT auction, (("dateTime" / 2000) * 2000 - 8000) AS hop_start FROM bid
) expanded
GROUP BY auction, hop_start
SQL
      ;;
    q7)
      cat <<'SQL'
SELECT MAX(price) AS maxprice FROM bid GROUP BY ("dateTime" / 10000)
SQL
      ;;
    q8)
      cat <<'SQL'
SELECT id, name, COUNT(*) AS person_count FROM person GROUP BY id, name, ("dateTime" / 10000)
SQL
      ;;
    q12)
      cat <<'SQL'
SELECT bidder, COUNT(*) AS bid_count FROM bid GROUP BY bidder, ("dateTime" / 10000)
SQL
      ;;
    q13)
      cat <<'SQL'
SELECT b.auction, b.bidder, b.price, b."dateTime", a.seller AS value FROM bid AS b JOIN auction AS a ON b.auction = a.id WHERE b.auction % 10000 = a.id % 10000
SQL
      ;;
    q14)
      cat <<'SQL'
SELECT auction, bidder, price * 908 / 1000 AS price, CASE WHEN (("dateTime" / 3600000) % 24) >= 8 AND (("dateTime" / 3600000) % 24) <= 18 THEN 'dayTime' WHEN (("dateTime" / 3600000) % 24) <= 6 OR (("dateTime" / 3600000) % 24) >= 20 THEN 'nightTime' ELSE 'otherTime' END AS bid_time_type, "dateTime", extra, LENGTH(extra) - LENGTH(REPLACE(extra, 'c', '')) AS c_counts FROM bid WHERE price * 908 / 1000 > 1000000 AND price * 908 / 1000 < 50000000
SQL
      ;;
    q15)
      cat <<'SQL'
SELECT ("dateTime" / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY ("dateTime" / 86400000)
SQL
      ;;
    q16)
      cat <<'SQL'
SELECT channel, ("dateTime" / 86400000) AS day, MAX((("dateTime" / 60000) % 1440)) AS minute, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, COUNT(DISTINCT bidder) AS total_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price < 10000) AS rank1_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bidders, COUNT(DISTINCT bidder) FILTER (WHERE price >= 1000000) AS rank3_bidders, COUNT(DISTINCT auction) AS total_auctions, COUNT(DISTINCT auction) FILTER (WHERE price < 10000) AS rank1_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_auctions, COUNT(DISTINCT auction) FILTER (WHERE price >= 1000000) AS rank3_auctions FROM bid GROUP BY channel, ("dateTime" / 86400000)
SQL
      ;;
    q17)
      cat <<'SQL'
SELECT auction, ("dateTime" / 86400000) AS day, COUNT(*) AS total_bids, COUNT(*) FILTER (WHERE price < 10000) AS rank1_bids, COUNT(*) FILTER (WHERE price >= 10000 AND price < 1000000) AS rank2_bids, COUNT(*) FILTER (WHERE price >= 1000000) AS rank3_bids, MIN(price) AS min_price, MAX(price) AS max_price, AVG(price) AS avg_price, SUM(price) AS sum_price FROM bid GROUP BY auction, ("dateTime" / 86400000)
SQL
      ;;
    q21)
      cat <<'SQL'
SELECT auction, bidder, price, channel, CASE WHEN lower(channel) = 'apple' THEN '0' WHEN lower(channel) = 'google' THEN '1' WHEN lower(channel) = 'facebook' THEN '2' WHEN lower(channel) = 'baidu' THEN '3' ELSE NULLIF(SPLIT_PART(SPLIT_PART(url, 'channel_id=', 2), '&', 1), '') END AS channel_id FROM bid WHERE NULLIF(SPLIT_PART(SPLIT_PART(url, 'channel_id=', 2), '&', 1), '') IS NOT NULL OR lower(channel) IN ('apple', 'google', 'facebook', 'baidu')
SQL
      ;;
    q22)
      cat <<'SQL'
SELECT auction, bidder, price, channel, SPLIT_PART(url, '/', 4) AS dir1, SPLIT_PART(url, '/', 5) AS dir2, SPLIT_PART(url, '/', 6) AS dir3 FROM bid
SQL
      ;;
    *)
      query_sql "$1"
      ;;
  esac
}

query_sql_for_engine() {
  local engine="$1"
  local query_id="$2"
  case "${engine}" in
    risingwave|feldera|materialize)
      query_sql_portable "${query_id}"
      ;;
    *)
      query_sql "${query_id}"
      ;;
  esac
}

selected_queries() {
  local selector="$1"
  if [[ "${selector}" == "all" || "${selector}" == "nexmark_all" ]]; then
    printf '%s\n' "${CANONICAL_NEXMARK_QUERY_IDS[@]}"
    return
  fi

  local found=0
  local q
  for q in "${CANONICAL_NEXMARK_QUERY_IDS[@]}"; do
    if [[ "${q}" == "${selector}" ]]; then
      found=1
      printf '%s\n' "${q}"
      break
    fi
  done

  if [[ "${found}" != "1" ]]; then
    die "unknown query selector '${selector}' (expected all|nexmark_all|q0..q22 canonical IDs)"
  fi
}

cleanup() {
  stop_floe_process
  if [[ "${KEEP_CONTAINERS}" == "1" ]]; then
    return
  fi
  docker rm -f "${MATERIALIZE_CONTAINER}" >/dev/null 2>&1 || true
  docker rm -f "${RISINGWAVE_CONTAINER}" >/dev/null 2>&1 || true
  docker rm -f "${FELDERA_CONTAINER}" >/dev/null 2>&1 || true
  docker rm -f "${REDPANDA_CONTAINER}" >/dev/null 2>&1 || true
  docker network rm "${NETWORK_NAME}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

ensure_network() {
  if docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
    return 0
  fi
  docker network create "${NETWORK_NAME}" >/dev/null
}

ensure_redpanda() {
  if docker ps --format '{{.Names}}' | grep -Fx "${REDPANDA_CONTAINER}" >/dev/null 2>&1; then
    return 0
  fi

  ensure_network
  log "starting Redpanda ${REDPANDA_CONTAINER}"
  docker rm -f "${REDPANDA_CONTAINER}" >/dev/null 2>&1 || true
  docker pull "${REDPANDA_IMAGE}" >/dev/null
  docker run -d \
    --name "${REDPANDA_CONTAINER}" \
    --network "${NETWORK_NAME}" \
    -p "${BROKER_PORT}:19092" \
    "${REDPANDA_IMAGE}" \
    redpanda start \
      --overprovisioned \
      --smp 1 \
      --memory 1G \
      --reserve-memory 0M \
      --node-id 0 \
      --check=false \
      --kafka-addr "internal://0.0.0.0:9092,external://0.0.0.0:19092" \
      --advertise-kafka-addr "internal://${REDPANDA_CONTAINER}:9092,external://127.0.0.1:${BROKER_PORT}" >/dev/null

  local _
  for _ in $(seq 1 90); do
    if docker exec "${REDPANDA_CONTAINER}" rpk cluster info >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  docker logs "${REDPANDA_CONTAINER}" || true
  die "Redpanda did not become ready"
}

wait_for_pg() {
  local port="$1"
  local user="$2"
  local db="$3"
  local label="$4"
  local _
  for _ in $(seq 1 90); do
    if PGPASSWORD="" psql -h 127.0.0.1 -p "${port}" -U "${user}" -d "${db}" -Atqc "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_http_ok() {
  local url="$1"
  local _
  for _ in $(seq 1 90); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

build_producer() {
  log "building kafka benchmark producer"
  cargo build -p floe-benchmarks --bin kafka_million_bid_producer --release >/dev/null
}

build_floe_node() {
  log "building floe-node release binary"
  cargo build -p floe-node --release >/dev/null
}

reset_topic() {
  local topic="$1"
  docker exec "${REDPANDA_CONTAINER}" rpk topic delete "${topic}" >/dev/null 2>&1 || true
  docker exec "${REDPANDA_CONTAINER}" rpk topic create "${topic}" -p 1 -r 1 >/dev/null
}

PRODUCE_MS=0

produce_topic() {
  local topic="$1"
  local dataset="$2"
  local rows="$3"
  local start_ms end_ms
  start_ms="$(date +%s%3N)"
  "${REPO_ROOT}/target/release/kafka_million_bid_producer" \
    --brokers "${BROKER_ADDR}" \
    --topic "${topic}" \
    --dataset "${dataset}" \
    --rows "${rows}" >/dev/null
  end_ms="$(date +%s%3N)"
  PRODUCE_MS=$((PRODUCE_MS + end_ms - start_ms))
}

relation_specs_for_sources() {
  local sources="$1"
  local relation_prefix="$2"
  local specs=()
  if has_source "${sources}" bid; then
    specs+=("${relation_prefix}_bid:${BID_ROWS}")
  fi
  if has_source "${sources}" auction; then
    specs+=("${relation_prefix}_auction:${AUCTION_ROWS}")
  fi
  if has_source "${sources}" person; then
    specs+=("${relation_prefix}_person:${PERSON_ROWS}")
  fi
  printf '%s\n' "${specs[@]}"
}

poll_pg_source_counts() {
  local port="$1"
  local user="$2"
  local db="$3"
  local label="$4"
  shift 4
  local specs=("$@")

  local start_ms now_ms
  start_ms="$(date +%s%3N)"

  local _
  for _ in $(seq 1 "${POLL_ATTEMPTS}"); do
    local ready=1
    local spec
    for spec in "${specs[@]}"; do
      local relation="${spec%%:*}"
      local target="${spec##*:}"
      local sql="SELECT row_count FROM ${relation}"
      local count
      count="$(PGPASSWORD="" timeout "${PG_QUERY_TIMEOUT_SECONDS}"s psql -h 127.0.0.1 -p "${port}" -U "${user}" -d "${db}" -Atqc "${sql}" 2>/dev/null | tr -d '[:space:]' || true)"
      if [[ -z "${count}" || ! "${count}" =~ ^[0-9]+$ || ${count} -lt ${target} ]]; then
        ready=0
        break
      fi
    done

    if [[ "${ready}" == "1" ]]; then
      now_ms="$(date +%s%3N)"
      POST_PRODUCE_WAIT_MS=$((now_ms - start_ms))
      return 0
    fi
    sleep_ms "${POLL_INTERVAL_MS}"
  done

  return 1
}

poll_feldera_source_counts() {
  local pipeline="$1"
  shift
  local specs=("$@")

  local start_ms now_ms
  start_ms="$(date +%s%3N)"

  local _
  for _ in $(seq 1 "${POLL_ATTEMPTS}"); do
    local ready=1
    local spec
    for spec in "${specs[@]}"; do
      local relation="${spec%%:*}"
      local target="${spec##*:}"
      local response
      response="$(curl -fsS --get \
        "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}/query" \
        --data-urlencode "sql=SELECT row_count FROM ${relation}" \
        --data-urlencode "format=json" 2>/dev/null || true)"
      local count
      count="$(printf '%s' "${response}" | jq -sr 'if length > 0 then (.[0].ROW_COUNT // .[0].row_count // empty) else empty end' 2>/dev/null | tr -d '[:space:]' || true)"
      if [[ -z "${count}" || ! "${count}" =~ ^[0-9]+$ || ${count} -lt ${target} ]]; then
        ready=0
        break
      fi
    done

    if [[ "${ready}" == "1" ]]; then
      now_ms="$(date +%s%3N)"
      POST_PRODUCE_WAIT_MS=$((now_ms - start_ms))
      return 0
    fi
    sleep_ms "${POLL_INTERVAL_MS}"
  done

  return 1
}

poll_floe_kafka_group_catchup() {
  local sources="$1"
  local bid_group_id="$2"
  local auction_group_id="$3"
  local person_group_id="$4"
  local bid_topic="$5"
  local auction_topic="$6"
  local person_topic="$7"

  local start_ms now_ms
  start_ms="$(date +%s%3N)"

  local _
  for _ in $(seq 1 "${POLL_ATTEMPTS}"); do
    now_ms="$(date +%s%3N)"
    if (( now_ms - start_ms >= POLL_TIMEOUT_MS )); then
      return 1
    fi

    local ready=1

    if has_source "${sources}" bid; then
      local bid_status
      bid_status="$(docker exec "${REDPANDA_CONTAINER}" rpk group describe "${bid_group_id}" 2>/dev/null | awk -v t="${bid_topic}" '$1==t {print $3" "$5" "$6; exit}')"
      local bid_current bid_end bid_lag
      read -r bid_current bid_end bid_lag <<< "${bid_status}"
      if [[ -z "${bid_current:-}" || -z "${bid_end:-}" || -z "${bid_lag:-}" || ! "${bid_current}" =~ ^[0-9]+$ || ! "${bid_end}" =~ ^[0-9]+$ || ! "${bid_lag}" =~ ^[0-9]+$ || ${bid_current} -lt ${BID_ROWS} || ${bid_end} -lt ${BID_ROWS} || ${bid_lag} -ne 0 ]]; then
        ready=0
      fi
    fi

    if [[ "${ready}" == "1" ]] && has_source "${sources}" auction; then
      local auction_count
      auction_count="$(fetch_pg_scalar "${FLOE_PG_PORT}" postgres postgres "SELECT row_count FROM benchmark_ingest_auction")"
      if [[ -z "${auction_count}" || ! "${auction_count}" =~ ^[0-9]+$ || ${auction_count} -lt ${AUCTION_ROWS} ]]; then
        ready=0
      fi
    fi

    if [[ "${ready}" == "1" ]] && has_source "${sources}" person; then
      local person_count
      person_count="$(fetch_pg_scalar "${FLOE_PG_PORT}" postgres postgres "SELECT row_count FROM benchmark_ingest_person")"
      if [[ -z "${person_count}" || ! "${person_count}" =~ ^[0-9]+$ || ${person_count} -lt ${PERSON_ROWS} ]]; then
        ready=0
      fi
    fi

    if [[ "${ready}" == "1" ]]; then
      now_ms="$(date +%s%3N)"
      POST_PRODUCE_WAIT_MS=$((now_ms - start_ms))
      return 0
    fi

    sleep_ms "${POLL_INTERVAL_MS}"
  done

  return 1
}

poll_floe_result_rows_at_least() {
  local expected_rows="$1"

  local start_ms now_ms
  start_ms="$(date +%s%3N)"

  local _
  for _ in $(seq 1 "${POLL_ATTEMPTS}"); do
    now_ms="$(date +%s%3N)"
    if (( now_ms - start_ms >= POLL_TIMEOUT_MS )); then
      return 1
    fi

    local result_rows
    result_rows="$(fetch_pg_scalar "${FLOE_PG_PORT}" postgres postgres "SELECT COUNT(*)::BIGINT FROM benchmark_result")"
    if [[ -n "${result_rows}" && "${result_rows}" =~ ^[0-9]+$ && ${result_rows} -ge ${expected_rows} ]]; then
      return 0
    fi

    sleep_ms "${POLL_INTERVAL_MS}"
  done

  return 1
}

poll_floe_query_completion() {
  local query_id="$1"
  local sources="$2"
  local bid_group_id="$3"
  local auction_group_id="$4"
  local person_group_id="$5"
  local bid_topic="$6"
  local auction_topic="$7"
  local person_topic="$8"

  local start_ms now_ms
  start_ms="$(date +%s%3N)"

  if ! poll_floe_kafka_group_catchup \
    "${sources}" \
    "${bid_group_id}" \
    "${auction_group_id}" \
    "${person_group_id}" \
    "${bid_topic}" \
    "${auction_topic}" \
    "${person_topic}"; then
    return 1
  fi

  local expected_result_rows
  expected_result_rows="$(floe_result_row_target_for_query "${query_id}")"
  if [[ -n "${expected_result_rows}" ]] && ! poll_floe_result_rows_at_least "${expected_result_rows}"; then
    return 1
  fi

  now_ms="$(date +%s%3N)"
  POST_PRODUCE_WAIT_MS=$((now_ms - start_ms))
  return 0
}

stop_floe_process() {
  if [[ -z "${FLOE_NODE_PID}" ]]; then
    pkill -f "/target/release/floe-node run" >/dev/null 2>&1 || true
    return
  fi
  if kill -0 "${FLOE_NODE_PID}" >/dev/null 2>&1; then
    kill -INT "${FLOE_NODE_PID}" >/dev/null 2>&1 || true
    wait "${FLOE_NODE_PID}" >/dev/null 2>&1 || true
  fi
  FLOE_NODE_PID=""
  pkill -f "/target/release/floe-node run" >/dev/null 2>&1 || true
}

wait_for_floe_pg() {
  local artifact_dir="$1"
  local stderr_path="${artifact_dir}/floe-node.stderr.log"
  local _
  for _ in $(seq 1 180); do
    if ! kill -0 "${FLOE_NODE_PID}" >/dev/null 2>&1; then
      tail -n 120 "${stderr_path}" >&2 || true
      return 1
    fi
    if PGPASSWORD="" psql -h 127.0.0.1 -p "${FLOE_PG_PORT}" -U postgres -d postgres -Atqc "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  tail -n 120 "${stderr_path}" >&2 || true
  return 1
}

capture_run_context() {
  jq -n \
    --arg run_id "${RUN_ID}" \
    --arg engine "${ENGINE}" \
    --arg query_selector "${QUERY_SELECTOR}" \
    --arg broker_addr "${BROKER_ADDR}" \
    --arg broker_addr_from_container "${BROKER_ADDR_FROM_CONTAINER}" \
    --arg redpanda_image "${REDPANDA_IMAGE}" \
    --arg materialize_image "${MATERIALIZE_IMAGE}" \
    --arg risingwave_image "${RISINGWAVE_IMAGE}" \
    --arg feldera_image "${FELDERA_IMAGE}" \
    --argjson bid_rows "${BID_ROWS}" \
    --argjson auction_rows "${AUCTION_ROWS}" \
    --argjson person_rows "${PERSON_ROWS}" \
    --arg git_commit "$(git rev-parse HEAD)" \
    --arg git_branch "$(git branch --show-current 2>/dev/null || true)" \
    --arg rustc_version "$(rustc -V)" \
    '{
      run_id: $run_id,
      engine: $engine,
      query_selector: $query_selector,
      dataset_rows: {
        bid: $bid_rows,
        auction: $auction_rows,
        person: $person_rows
      },
      kafka: {
        broker_addr: $broker_addr,
        broker_addr_from_container: $broker_addr_from_container
      },
      images: {
        redpanda: $redpanda_image,
        materialize: $materialize_image,
        risingwave: $risingwave_image,
        feldera: $feldera_image
      },
      floe: {
        git_commit: $git_commit,
        git_branch: $git_branch,
        rustc_version: $rustc_version,
        binary: "target/release/floe-node"
      }
    }' > "${RUN_DIR}/run_context.json"
}

append_summary_row() {
  local engine="$1"
  local query_id="$2"
  local status="$3"
  local total_ms="$4"
  local produce_ms="$5"
  local post_ms="$6"
  local rows_per_sec="$7"
  local input_rows="$8"
  local result_rows="$9"
  local notes="${10}"

  local total_s="n/a"
  local produce_s="n/a"
  local post_s="n/a"
  local total_ms_json=0
  local produce_ms_json=0
  local post_ms_json=0
  local rows_per_sec_json=0
  local input_rows_json=0
  local result_rows_json=0

  if [[ "${total_ms}" =~ ^[0-9]+$ ]]; then
    total_s="$(awk "BEGIN { print ${total_ms}/1000 }")"
    total_ms_json="${total_ms}"
  fi
  if [[ "${produce_ms}" =~ ^[0-9]+$ ]]; then
    produce_s="$(awk "BEGIN { print ${produce_ms}/1000 }")"
    produce_ms_json="${produce_ms}"
  fi
  if [[ "${post_ms}" =~ ^[0-9]+$ ]]; then
    post_s="$(awk "BEGIN { print ${post_ms}/1000 }")"
    post_ms_json="${post_ms}"
  fi
  if [[ "${rows_per_sec}" =~ ^[0-9]+$ ]]; then
    rows_per_sec_json="${rows_per_sec}"
  fi
  if [[ "${input_rows}" =~ ^[0-9]+$ ]]; then
    input_rows_json="${input_rows}"
  fi
  if [[ "${result_rows}" =~ ^[0-9]+$ ]]; then
    result_rows_json="${result_rows}"
  fi

  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
    "${engine}" "${query_id}" "${status}" "${total_s}" "${produce_s}" "${post_s}" \
    "${rows_per_sec}" "${input_rows}" "${result_rows}" "${notes}" >> "${RESULTS_FILE}"

  jq -n \
    --arg engine "${engine}" \
    --arg query_id "${query_id}" \
    --arg status "${status}" \
    --argjson total_ms "${total_ms_json}" \
    --argjson produce_ms "${produce_ms_json}" \
    --argjson post_produce_wait_ms "${post_ms_json}" \
    --argjson input_rows_per_sec "${rows_per_sec_json}" \
    --argjson input_rows "${input_rows_json}" \
    --argjson result_rows "${result_rows_json}" \
    --arg notes "${notes}" \
    '{
      engine: $engine,
      query_id: $query_id,
      status: $status,
      timing: {
        total_ms: $total_ms,
        produce_ms: $produce_ms,
        post_produce_wait_ms: $post_produce_wait_ms
      },
      throughput: {
        input_rows_per_sec: $input_rows_per_sec
      },
      rows: {
        input_rows: $input_rows,
        result_rows: $result_rows
      },
      notes: $notes
    }' >> "${RESULTS_JSONL}"
}

record_failure() {
  local engine="$1"
  local query_id="$2"
  local notes="$3"
  local input_rows="$4"
  append_summary_row "${engine}" "${query_id}" "failed" "" "" "" "n/a" "${input_rows}" "n/a" "${notes}"
}

producer_topics_for_query() {
  local engine="$1"
  local query_id="$2"
  local bid_topic="${engine}_${query_id}_${RUN_ID}_bids"
  local auction_topic="${engine}_${query_id}_${RUN_ID}_auctions"
  local person_topic="${engine}_${query_id}_${RUN_ID}_persons"
  printf '%s|%s|%s\n' "${bid_topic}" "${auction_topic}" "${person_topic}"
}

produce_for_query_sources() {
  local sources="$1"
  local bid_topic="$2"
  local auction_topic="$3"
  local person_topic="$4"
  PRODUCE_MS=0

  if has_source "${sources}" auction; then
    produce_topic "${auction_topic}" auction "${AUCTION_ROWS}"
  fi
  if has_source "${sources}" person; then
    produce_topic "${person_topic}" person "${PERSON_ROWS}"
  fi
  if has_source "${sources}" bid; then
    produce_topic "${bid_topic}" bid "${BID_ROWS}"
  fi
}

fetch_pg_scalar() {
  local port="$1"
  local user="$2"
  local db="$3"
  local sql="$4"
  PGPASSWORD="" timeout "${PG_QUERY_TIMEOUT_SECONDS}"s psql -h 127.0.0.1 -p "${port}" -U "${user}" -d "${db}" -Atqc "${sql}" 2>/dev/null | tr -d '[:space:]' || true
}

# Materialize
start_materialize() {
  docker rm -f "${MATERIALIZE_CONTAINER}" >/dev/null 2>&1 || true
  docker pull "${MATERIALIZE_IMAGE}" >/dev/null
  docker run -d \
    --name "${MATERIALIZE_CONTAINER}" \
    --network "${NETWORK_NAME}" \
    -p "${MATERIALIZE_SQL_PORT}:6875" \
    "${MATERIALIZE_IMAGE}" >/dev/null

  if ! wait_for_pg "${MATERIALIZE_SQL_PORT}" materialize materialize "Materialize"; then
    return 1
  fi

  if ! PGPASSWORD="" psql -h 127.0.0.1 -p "${MATERIALIZE_SQL_PORT}" -U materialize -d materialize -v ON_ERROR_STOP=1 -Atqc "DROP CLUSTER IF EXISTS bench CASCADE" >/dev/null 2>&1; then
    return 1
  fi

  if ! PGPASSWORD="" psql -h 127.0.0.1 -p "${MATERIALIZE_SQL_PORT}" -U materialize -d materialize -v ON_ERROR_STOP=1 -Atqc "CREATE CLUSTER bench SIZE '${MATERIALIZE_CLUSTER_SIZE}'" >/dev/null 2>&1; then
    return 1
  fi

  return 0
}

stop_materialize() {
  docker rm -f "${MATERIALIZE_CONTAINER}" >/dev/null 2>&1 || true
}

write_materialize_setup_sql() {
  local path="$1"
  local query_id="$2"
  local sources="$3"
  local bid_topic="$4"
  local auction_topic="$5"
  local person_topic="$6"
  local query_text
  query_text="$(query_sql_for_engine materialize "${query_id}")"
  local use_indexed_views=0
  if env_enabled "${MATERIALIZE_BEST_EFFORT_IN_MEMORY}"; then
    use_indexed_views=1
  fi

  cat > "${path}" <<SQL
SET cluster = bench;
DROP INDEX IF EXISTS benchmark_ingest_bid_primary_idx CASCADE;
DROP INDEX IF EXISTS benchmark_ingest_auction_primary_idx CASCADE;
DROP INDEX IF EXISTS benchmark_ingest_person_primary_idx CASCADE;
DROP INDEX IF EXISTS benchmark_result_primary_idx CASCADE;
DROP VIEW IF EXISTS bid CASCADE;
DROP VIEW IF EXISTS auction CASCADE;
DROP VIEW IF EXISTS person CASCADE;
DROP SOURCE IF EXISTS bids_source CASCADE;
DROP SOURCE IF EXISTS auctions_source CASCADE;
DROP SOURCE IF EXISTS persons_source CASCADE;
DROP CONNECTION IF EXISTS kafka_conn CASCADE;
CREATE CONNECTION kafka_conn TO KAFKA (
  BROKER '${BROKER_ADDR_FROM_CONTAINER}',
  SECURITY PROTOCOL PLAINTEXT
);
SQL

  if [[ "${use_indexed_views}" == "1" ]]; then
    cat >> "${path}" <<SQL
DROP VIEW IF EXISTS benchmark_ingest_bid CASCADE;
DROP VIEW IF EXISTS benchmark_ingest_auction CASCADE;
DROP VIEW IF EXISTS benchmark_ingest_person CASCADE;
DROP VIEW IF EXISTS benchmark_result CASCADE;
SQL
  else
    cat >> "${path}" <<SQL
DROP MATERIALIZED VIEW IF EXISTS benchmark_ingest_bid CASCADE;
DROP MATERIALIZED VIEW IF EXISTS benchmark_ingest_auction CASCADE;
DROP MATERIALIZED VIEW IF EXISTS benchmark_ingest_person CASCADE;
DROP MATERIALIZED VIEW IF EXISTS benchmark_result CASCADE;
SQL
  fi

  if has_source "${sources}" bid; then
    cat >> "${path}" <<SQL
CREATE SOURCE bids_source
FROM KAFKA CONNECTION kafka_conn (TOPIC '${bid_topic}')
FORMAT JSON ENVELOPE NONE;
CREATE VIEW bid AS
SELECT
  (data->>'auction')::bigint AS auction,
  (data->>'bidder')::bigint AS bidder,
  (data->>'price')::bigint AS price,
  (data->>'channel')::text AS channel,
  (data->>'url')::text AS url,
  (data->>'date_time')::bigint AS "dateTime",
  (data->>'extra')::text AS extra
FROM bids_source;
SQL

  if [[ "${use_indexed_views}" == "1" ]]; then
    cat >> "${path}" <<SQL
CREATE VIEW benchmark_ingest_bid AS
SELECT COUNT(*)::bigint AS row_count FROM bids_source;
CREATE DEFAULT INDEX ON benchmark_ingest_bid;
SQL
  else
    cat >> "${path}" <<SQL
CREATE MATERIALIZED VIEW benchmark_ingest_bid AS
SELECT COUNT(*)::bigint AS row_count FROM bids_source;
SQL
  fi
  fi

  if has_source "${sources}" auction; then
    cat >> "${path}" <<SQL
CREATE SOURCE auctions_source
FROM KAFKA CONNECTION kafka_conn (TOPIC '${auction_topic}')
FORMAT JSON ENVELOPE NONE;
CREATE VIEW auction AS
SELECT
  (data->>'id')::bigint AS id,
  (data->>'item_name')::text AS "itemName",
  (data->>'description')::text AS description,
  (data->>'initial_bid')::bigint AS "initialBid",
  (data->>'reserve')::bigint AS reserve,
  (data->>'date_time')::bigint AS "dateTime",
  (data->>'expires')::bigint AS expires,
  (data->>'seller')::bigint AS seller,
  (data->>'category')::bigint AS category,
  (data->>'extra')::text AS extra
FROM auctions_source;
SQL

  if [[ "${use_indexed_views}" == "1" ]]; then
    cat >> "${path}" <<SQL
CREATE VIEW benchmark_ingest_auction AS
SELECT COUNT(*)::bigint AS row_count FROM auctions_source;
CREATE DEFAULT INDEX ON benchmark_ingest_auction;
SQL
  else
    cat >> "${path}" <<SQL
CREATE MATERIALIZED VIEW benchmark_ingest_auction AS
SELECT COUNT(*)::bigint AS row_count FROM auctions_source;
SQL
  fi
  fi

  if has_source "${sources}" person; then
    cat >> "${path}" <<SQL
CREATE SOURCE persons_source
FROM KAFKA CONNECTION kafka_conn (TOPIC '${person_topic}')
FORMAT JSON ENVELOPE NONE;
CREATE VIEW person AS
SELECT
  (data->>'id')::bigint AS id,
  (data->>'name')::text AS name,
  (data->>'city')::text AS city,
  (data->>'state')::text AS state,
  (data->>'date_time')::bigint AS "dateTime",
  (data->>'extra')::text AS extra
FROM persons_source;
SQL

  if [[ "${use_indexed_views}" == "1" ]]; then
    cat >> "${path}" <<SQL
CREATE VIEW benchmark_ingest_person AS
SELECT COUNT(*)::bigint AS row_count FROM persons_source;
CREATE DEFAULT INDEX ON benchmark_ingest_person;
SQL
  else
    cat >> "${path}" <<SQL
CREATE MATERIALIZED VIEW benchmark_ingest_person AS
SELECT COUNT(*)::bigint AS row_count FROM persons_source;
SQL
  fi
  fi

  if [[ "${use_indexed_views}" == "1" ]]; then
    cat >> "${path}" <<SQL
CREATE VIEW benchmark_result AS
${query_text};
CREATE DEFAULT INDEX ON benchmark_result;
SQL
  else
    cat >> "${path}" <<SQL
CREATE MATERIALIZED VIEW benchmark_result AS
${query_text};
SQL
  fi
}

run_materialize_query() {
  local query_id="$1"
  local artifact_dir="$2"
  local sources="$3"
  local bid_topic="$4"
  local auction_topic="$5"
  local person_topic="$6"

  mkdir -p "${artifact_dir}"
  write_materialize_setup_sql "${artifact_dir}/setup.sql" "${query_id}" "${sources}" "${bid_topic}" "${auction_topic}" "${person_topic}"

  if ! PGPASSWORD="" psql -h 127.0.0.1 -p "${MATERIALIZE_SQL_PORT}" -U materialize -d materialize -v ON_ERROR_STOP=1 -f "${artifact_dir}/setup.sql" >"${artifact_dir}/setup.stdout.log" 2>"${artifact_dir}/setup.stderr.log"; then
    return 1
  fi

  local specs=()
  while IFS= read -r spec; do
    [[ -n "${spec}" ]] && specs+=("${spec}")
  done < <(relation_specs_for_sources "${sources}" benchmark_ingest)

  local input_rows
  input_rows="$(input_rows_total_for_sources "${sources}")"
  local notes="count_views_pgwire"
  if env_enabled "${MATERIALIZE_BEST_EFFORT_IN_MEMORY}"; then
    notes="count_views_pgwire_indexed_views"
  fi

  local start_ms end_ms total_ms rows_per_sec result_rows
  start_ms="$(date +%s%3N)"
  produce_for_query_sources "${sources}" "${bid_topic}" "${auction_topic}" "${person_topic}"
  if ! poll_pg_source_counts "${MATERIALIZE_SQL_PORT}" materialize materialize "Materialize ${query_id}" "${specs[@]}"; then
    return 1
  fi
  end_ms="$(date +%s%3N)"
  total_ms=$((end_ms - start_ms))
  if (( total_ms > 0 )); then
    rows_per_sec=$((input_rows * 1000 / total_ms))
  else
    rows_per_sec=0
  fi

  result_rows="$(fetch_pg_scalar "${MATERIALIZE_SQL_PORT}" materialize materialize "SELECT COUNT(*)::BIGINT FROM benchmark_result")"
  [[ -z "${result_rows}" ]] && result_rows="n/a"

  append_summary_row materialize "${query_id}" ok "${total_ms}" "${PRODUCE_MS}" "${POST_PRODUCE_WAIT_MS}" "${rows_per_sec}" "${input_rows}" "${result_rows}" "${notes}"
  return 0
}

# RisingWave
start_risingwave() {
  docker rm -f "${RISINGWAVE_CONTAINER}" >/dev/null 2>&1 || true
  docker pull "${RISINGWAVE_IMAGE}" >/dev/null

  if env_enabled "${RISINGWAVE_IN_MEMORY}"; then
    docker run -d \
      --name "${RISINGWAVE_CONTAINER}" \
      --network "${NETWORK_NAME}" \
      -p "${RISINGWAVE_SQL_PORT}:4566" \
      "${RISINGWAVE_IMAGE}" \
      single_node --in-memory >/dev/null
  else
    docker run -d \
      --name "${RISINGWAVE_CONTAINER}" \
      --network "${NETWORK_NAME}" \
      -p "${RISINGWAVE_SQL_PORT}:4566" \
      "${RISINGWAVE_IMAGE}" \
      single_node >/dev/null
  fi

  wait_for_pg "${RISINGWAVE_SQL_PORT}" root dev "RisingWave"
}

stop_risingwave() {
  docker rm -f "${RISINGWAVE_CONTAINER}" >/dev/null 2>&1 || true
}

write_risingwave_setup_sql() {
  local path="$1"
  local query_id="$2"
  local sources="$3"
  local bid_topic="$4"
  local auction_topic="$5"
  local person_topic="$6"
  local query_text
  query_text="$(query_sql_for_engine risingwave "${query_id}")"
  local rw_fetch_opts=""
  if env_enabled "${KAFKA_LATENCY_FETCH_PROFILE}"; then
    rw_fetch_opts="
  ,properties.fetch.wait.max.ms = '${KAFKA_FETCH_WAIT_MAX_MS}'
  ,properties.fetch.queue.backoff.ms = '${KAFKA_FETCH_QUEUE_BACKOFF_MS}'
  ,properties.fetch.min.bytes = '${KAFKA_FETCH_MIN_BYTES}'"
  fi

  cat > "${path}" <<SQL
DROP MATERIALIZED VIEW IF EXISTS benchmark_ingest_bid;
DROP MATERIALIZED VIEW IF EXISTS benchmark_ingest_auction;
DROP MATERIALIZED VIEW IF EXISTS benchmark_ingest_person;
DROP MATERIALIZED VIEW IF EXISTS benchmark_result;
DROP MATERIALIZED VIEW IF EXISTS bid;
DROP MATERIALIZED VIEW IF EXISTS auction;
DROP MATERIALIZED VIEW IF EXISTS person;
DROP SOURCE IF EXISTS bids_source;
DROP SOURCE IF EXISTS auctions_source;
DROP SOURCE IF EXISTS persons_source;
SQL

  if has_source "${sources}" bid; then
    cat >> "${path}" <<SQL
CREATE SOURCE bids_source (
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  channel VARCHAR,
  url VARCHAR,
  date_time BIGINT,
  extra VARCHAR
)
WITH (
  connector = 'kafka',
  topic = '${bid_topic}',
  properties.bootstrap.server = '${BROKER_ADDR_FROM_CONTAINER}',
  scan.startup.mode = 'earliest'${rw_fetch_opts}
)
FORMAT PLAIN ENCODE JSON;
CREATE MATERIALIZED VIEW bid AS
SELECT auction, bidder, price, channel, url, date_time AS "dateTime", extra
FROM bids_source;
CREATE MATERIALIZED VIEW benchmark_ingest_bid AS
SELECT COUNT(*)::BIGINT AS row_count FROM bids_source;
SQL
  fi

  if has_source "${sources}" auction; then
    cat >> "${path}" <<SQL
CREATE SOURCE auctions_source (
  id BIGINT,
  item_name VARCHAR,
  description VARCHAR,
  initial_bid BIGINT,
  reserve BIGINT,
  seller BIGINT,
  category BIGINT,
  expires BIGINT,
  date_time BIGINT,
  extra VARCHAR
)
WITH (
  connector = 'kafka',
  topic = '${auction_topic}',
  properties.bootstrap.server = '${BROKER_ADDR_FROM_CONTAINER}',
  scan.startup.mode = 'earliest'${rw_fetch_opts}
)
FORMAT PLAIN ENCODE JSON;
CREATE MATERIALIZED VIEW auction AS
SELECT id, item_name AS "itemName", description, initial_bid AS "initialBid", reserve, date_time AS "dateTime", expires, seller, category, extra
FROM auctions_source;
CREATE MATERIALIZED VIEW benchmark_ingest_auction AS
SELECT COUNT(*)::BIGINT AS row_count FROM auctions_source;
SQL
  fi

  if has_source "${sources}" person; then
    cat >> "${path}" <<SQL
CREATE SOURCE persons_source (
  id BIGINT,
  name VARCHAR,
  email_address VARCHAR,
  credit_card VARCHAR,
  city VARCHAR,
  state VARCHAR,
  date_time BIGINT,
  extra VARCHAR
)
WITH (
  connector = 'kafka',
  topic = '${person_topic}',
  properties.bootstrap.server = '${BROKER_ADDR_FROM_CONTAINER}',
  scan.startup.mode = 'earliest'${rw_fetch_opts}
)
FORMAT PLAIN ENCODE JSON;
CREATE MATERIALIZED VIEW person AS
SELECT id, name, city, state, date_time AS "dateTime", extra
FROM persons_source;
CREATE MATERIALIZED VIEW benchmark_ingest_person AS
SELECT COUNT(*)::BIGINT AS row_count FROM persons_source;
SQL
  fi

  cat >> "${path}" <<SQL
CREATE MATERIALIZED VIEW benchmark_result AS
${query_text};
SQL
}

run_risingwave_query() {
  local query_id="$1"
  local artifact_dir="$2"
  local sources="$3"
  local bid_topic="$4"
  local auction_topic="$5"
  local person_topic="$6"

  mkdir -p "${artifact_dir}"
  write_risingwave_setup_sql "${artifact_dir}/setup.sql" "${query_id}" "${sources}" "${bid_topic}" "${auction_topic}" "${person_topic}"

  if ! PGPASSWORD="" psql -h 127.0.0.1 -p "${RISINGWAVE_SQL_PORT}" -U root -d dev -v ON_ERROR_STOP=1 -f "${artifact_dir}/setup.sql" >"${artifact_dir}/setup.stdout.log" 2>"${artifact_dir}/setup.stderr.log"; then
    return 1
  fi

  local specs=()
  while IFS= read -r spec; do
    [[ -n "${spec}" ]] && specs+=("${spec}")
  done < <(relation_specs_for_sources "${sources}" benchmark_ingest)

  local input_rows
  input_rows="$(input_rows_total_for_sources "${sources}")"

  local start_ms end_ms total_ms rows_per_sec result_rows
  start_ms="$(date +%s%3N)"
  produce_for_query_sources "${sources}" "${bid_topic}" "${auction_topic}" "${person_topic}"
  if ! poll_pg_source_counts "${RISINGWAVE_SQL_PORT}" root dev "RisingWave ${query_id}" "${specs[@]}"; then
    return 1
  fi
  end_ms="$(date +%s%3N)"
  total_ms=$((end_ms - start_ms))
  if (( total_ms > 0 )); then
    rows_per_sec=$((input_rows * 1000 / total_ms))
  else
    rows_per_sec=0
  fi

  result_rows="$(fetch_pg_scalar "${RISINGWAVE_SQL_PORT}" root dev "SELECT COUNT(*)::BIGINT FROM benchmark_result")"
  [[ -z "${result_rows}" ]] && result_rows="n/a"

  append_summary_row risingwave "${query_id}" ok "${total_ms}" "${PRODUCE_MS}" "${POST_PRODUCE_WAIT_MS}" "${rows_per_sec}" "${input_rows}" "${result_rows}" "count_views_pgwire"
  return 0
}

# Feldera
start_feldera() {
  docker rm -f "${FELDERA_CONTAINER}" >/dev/null 2>&1 || true
  docker pull "${FELDERA_IMAGE}" >/dev/null
  docker run -d \
    --name "${FELDERA_CONTAINER}" \
    --network "${NETWORK_NAME}" \
    -p "${FELDERA_HTTP_PORT}:8080" \
    "${FELDERA_IMAGE}" >/dev/null

  wait_for_http_ok "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines"
}

stop_feldera() {
  docker rm -f "${FELDERA_CONTAINER}" >/dev/null 2>&1 || true
}

poll_feldera_program_success() {
  local pipeline="$1"
  local _
  for _ in $(seq 1 240); do
    local status
    status="$(curl -fsS "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}" | jq -r '.program_status')"
    case "${status}" in
      Success) return 0 ;;
      SqlError|RustError|SystemError) return 1 ;;
    esac
    sleep 2
  done
  return 1
}

poll_feldera_running() {
  local pipeline="$1"
  local _
  for _ in $(seq 1 120); do
    local status
    status="$(curl -fsS "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}" | jq -r '.deployment_status')"
    if [[ "${status}" == "Running" ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

write_feldera_program_sql() {
  local path="$1"
  local query_id="$2"
  local sources="$3"
  local bid_topic="$4"
  local auction_topic="$5"
  local person_topic="$6"
  local query_text
  query_text="$(query_sql_for_engine feldera "${query_id}")"
  local feldera_fetch_json=""
  if env_enabled "${KAFKA_LATENCY_FETCH_PROFILE}"; then
    feldera_fetch_json=",
          \"fetch.wait.max.ms\": \"${KAFKA_FETCH_WAIT_MAX_MS}\",
          \"fetch.queue.backoff.ms\": \"${KAFKA_FETCH_QUEUE_BACKOFF_MS}\",
          \"fetch.min.bytes\": \"${KAFKA_FETCH_MIN_BYTES}\""
  fi

  : > "${path}"

  if has_source "${sources}" bid; then
    cat >> "${path}" <<SQL
CREATE TABLE bids_source (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    channel VARCHAR,
    url VARCHAR,
    date_time BIGINT,
    extra VARCHAR
) WITH (
    'connectors' = '[{
      "name": "bids_in",
      "transport": {
        "name": "kafka_input",
        "config": {
          "topic": "${bid_topic}",
          "start_from": "earliest",
          "bootstrap.servers": "${BROKER_ADDR_FROM_CONTAINER}"${feldera_fetch_json}
        }
      },
      "format": {
        "name": "json",
        "config": {
          "update_format": "raw",
          "array": false
        }
      }
    }]'
);

CREATE MATERIALIZED VIEW bid AS
SELECT auction, bidder, price, channel, url, date_time AS "dateTime", extra
FROM bids_source;

CREATE MATERIALIZED VIEW benchmark_ingest_bid AS
SELECT COUNT(*) AS row_count FROM bids_source;

SQL
  fi

  if has_source "${sources}" auction; then
    cat >> "${path}" <<SQL
CREATE TABLE auctions_source (
    id BIGINT,
    item_name VARCHAR,
    description VARCHAR,
    initial_bid BIGINT,
    reserve BIGINT,
    seller BIGINT,
    category BIGINT,
    expires BIGINT,
    date_time BIGINT,
    extra VARCHAR
) WITH (
    'connectors' = '[{
      "name": "auctions_in",
      "transport": {
        "name": "kafka_input",
        "config": {
          "topic": "${auction_topic}",
          "start_from": "earliest",
          "bootstrap.servers": "${BROKER_ADDR_FROM_CONTAINER}"${feldera_fetch_json}
        }
      },
      "format": {
        "name": "json",
        "config": {
          "update_format": "raw",
          "array": false
        }
      }
    }]'
);

CREATE MATERIALIZED VIEW auction AS
SELECT id, item_name AS "itemName", description, initial_bid AS "initialBid", reserve, date_time AS "dateTime", expires, seller, category, extra
FROM auctions_source;

CREATE MATERIALIZED VIEW benchmark_ingest_auction AS
SELECT COUNT(*) AS row_count FROM auctions_source;

SQL
  fi

  if has_source "${sources}" person; then
    cat >> "${path}" <<SQL
CREATE TABLE persons_source (
    id BIGINT,
    name VARCHAR,
    email_address VARCHAR,
    credit_card VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time BIGINT,
    extra VARCHAR
) WITH (
    'connectors' = '[{
      "name": "persons_in",
      "transport": {
        "name": "kafka_input",
        "config": {
          "topic": "${person_topic}",
          "start_from": "earliest",
          "bootstrap.servers": "${BROKER_ADDR_FROM_CONTAINER}"${feldera_fetch_json}
        }
      },
      "format": {
        "name": "json",
        "config": {
          "update_format": "raw",
          "array": false
        }
      }
    }]'
);

CREATE MATERIALIZED VIEW person AS
SELECT id, name, city, state, date_time AS "dateTime", extra
FROM persons_source;

CREATE MATERIALIZED VIEW benchmark_ingest_person AS
SELECT COUNT(*) AS row_count FROM persons_source;

SQL
  fi

  cat >> "${path}" <<SQL
CREATE MATERIALIZED VIEW benchmark_result AS
${query_text};
SQL
}

run_feldera_query() {
  local query_id="$1"
  local artifact_dir="$2"
  local sources="$3"
  local bid_topic="$4"
  local auction_topic="$5"
  local person_topic="$6"
  local pipeline="nexmark_${query_id}_${RUN_ID}"

  mkdir -p "${artifact_dir}"
  write_feldera_program_sql "${artifact_dir}/program.sql" "${query_id}" "${sources}" "${bid_topic}" "${auction_topic}" "${person_topic}"

  curl -fsS -X DELETE "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}" >/dev/null 2>&1 || true

  if ! curl -fsS -X PUT "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}" \
      -H 'Content-Type: application/json' \
      -d "$(
        if env_enabled "${FELDERA_BEST_EFFORT_IN_MEMORY}"; then
          jq -Rsn \
            --rawfile code "${artifact_dir}/program.sql" \
            --arg name "${pipeline}" \
            --argjson workers "${FELDERA_WORKERS}" \
            --argjson min_storage_bytes "${FELDERA_MIN_STORAGE_BYTES}" \
            --argjson min_step_storage_bytes "${FELDERA_MIN_STEP_STORAGE_BYTES}" \
            '{name: $name, description: "Nexmark cross-engine benchmark", runtime_config: {workers: $workers, storage: {min_storage_bytes: $min_storage_bytes, min_step_storage_bytes: $min_step_storage_bytes}}, program_config: {}, program_code: $code}'
        else
          jq -Rsn \
            --rawfile code "${artifact_dir}/program.sql" \
            --arg name "${pipeline}" \
            --argjson workers "${FELDERA_WORKERS}" \
            '{name: $name, description: "Nexmark cross-engine benchmark", runtime_config: {workers: $workers}, program_config: {}, program_code: $code}'
        fi
      )" \
      >"${artifact_dir}/pipeline_create.json" 2>"${artifact_dir}/pipeline_create.stderr.log"; then
    return 1
  fi

  if ! poll_feldera_program_success "${pipeline}"; then
    curl -fsS "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}" >"${artifact_dir}/pipeline_status.json" 2>/dev/null || true
    return 1
  fi

  if ! curl -fsS -X POST "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}/start" >/dev/null; then
    return 1
  fi

  if ! poll_feldera_running "${pipeline}"; then
    return 1
  fi

  local specs=()
  while IFS= read -r spec; do
    [[ -n "${spec}" ]] && specs+=("${spec}")
  done < <(relation_specs_for_sources "${sources}" benchmark_ingest)

  local input_rows
  input_rows="$(input_rows_total_for_sources "${sources}")"

  local start_ms end_ms total_ms rows_per_sec result_rows
  start_ms="$(date +%s%3N)"
  produce_for_query_sources "${sources}" "${bid_topic}" "${auction_topic}" "${person_topic}"
  if ! poll_feldera_source_counts "${pipeline}" "${specs[@]}"; then
    return 1
  fi
  end_ms="$(date +%s%3N)"
  total_ms=$((end_ms - start_ms))
  if (( total_ms > 0 )); then
    rows_per_sec=$((input_rows * 1000 / total_ms))
  else
    rows_per_sec=0
  fi

  local response
  response="$(curl -fsS --get \
    "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}/query" \
    --data-urlencode "sql=SELECT COUNT(*) AS row_count FROM benchmark_result" \
    --data-urlencode "format=json" 2>/dev/null || true)"
  result_rows="$(printf '%s' "${response}" | jq -sr 'if length > 0 then (.[0].ROW_COUNT // .[0].row_count // empty) else empty end' 2>/dev/null | tr -d '[:space:]' || true)"
  [[ -z "${result_rows}" ]] && result_rows="n/a"

  local notes="count_views_adhoc_query"
  if env_enabled "${FELDERA_BEST_EFFORT_IN_MEMORY}"; then
    notes="count_views_adhoc_query_best_effort_in_memory"
  fi
  append_summary_row feldera "${query_id}" ok "${total_ms}" "${PRODUCE_MS}" "${POST_PRODUCE_WAIT_MS}" "${rows_per_sec}" "${input_rows}" "${result_rows}" "${notes}"

  curl -fsS -X POST "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}/shutdown" >/dev/null 2>&1 || true
  curl -fsS -X DELETE "http://127.0.0.1:${FELDERA_HTTP_PORT}/v0/pipelines/${pipeline}" >/dev/null 2>&1 || true

  return 0
}

# Floe
write_floe_config() {
  local path="$1"
  local sources="$2"
  local bid_topic="$3"
  local auction_topic="$4"
  local person_topic="$5"
  local bid_group_id="$6"
  local auction_group_id="$7"
  local person_group_id="$8"

  local need_bid=0
  local need_auction=0
  local need_person=0
  has_source "${sources}" bid && need_bid=1
  has_source "${sources}" auction && need_auction=1
  has_source "${sources}" person && need_person=1

  jq -n \
    --arg brokers "${BROKER_ADDR}" \
    --arg bid_topic "${bid_topic}" \
    --arg auction_topic "${auction_topic}" \
    --arg person_topic "${person_topic}" \
    --arg bid_group_id "${bid_group_id}" \
    --arg auction_group_id "${auction_group_id}" \
    --arg person_group_id "${person_group_id}" \
    --argjson need_bid "${need_bid}" \
    --argjson need_auction "${need_auction}" \
    --argjson need_person "${need_person}" \
    --argjson kafka_poll_ms "${FLOE_KAFKA_POLL_MS}" \
    --argjson kafka_max_messages_per_tick "${FLOE_KAFKA_MAX_MESSAGES_PER_TICK}" \
    --argjson ingest_queue_capacity "${FLOE_INGEST_QUEUE_CAPACITY}" \
    --argjson ingest_batch_size "${FLOE_INGEST_BATCH_SIZE}" \
    --argjson ingest_batch_per_source "${FLOE_INGEST_BATCH_PER_SOURCE}" \
    --argjson ingest_batch_per_connector "${FLOE_INGEST_BATCH_PER_CONNECTOR}" \
    --argjson mv_retain_last "${FLOE_MV_RETAIN_LAST}" \
    '{
      connectors: (
        []
        + (if $need_bid == 1 then [{
            type: "kafka",
            brokers: $brokers,
            topics: [$bid_topic],
            group_id: $bid_group_id,
            default_source: "nexmark_bid",
            poll_ms: $kafka_poll_ms,
            max_messages_per_tick: $kafka_max_messages_per_tick
          }] else [] end)
        + (if $need_auction == 1 then [{
            type: "kafka",
            brokers: $brokers,
            topics: [$auction_topic],
            group_id: $auction_group_id,
            default_source: "nexmark_auction",
            poll_ms: $kafka_poll_ms,
            max_messages_per_tick: $kafka_max_messages_per_tick
          }] else [] end)
        + (if $need_person == 1 then [{
            type: "kafka",
            brokers: $brokers,
            topics: [$person_topic],
            group_id: $person_group_id,
            default_source: "nexmark_person",
            poll_ms: $kafka_poll_ms,
            max_messages_per_tick: $kafka_max_messages_per_tick
          }] else [] end)
      ),
      runtime: {
        ingest_queue_capacity: $ingest_queue_capacity,
        ingest_batch_size: $ingest_batch_size,
        ingest_batch_per_source: $ingest_batch_per_source,
        ingest_batch_per_connector: $ingest_batch_per_connector,
        mv_retain_last: $mv_retain_last
      },
      storage: {
        await_durable: false
      }
    }' > "${path}"
}

write_floe_program_sql() {
  local path="$1"
  local query_id="$2"
  local sources="$3"
  local base_query
  base_query="$(query_sql "${query_id}")"
  local ctes=()

  : > "${path}"

  if has_source "${sources}" bid; then
    ctes+=('bid AS (SELECT auction, bidder, price, channel, url, date_time AS "dateTime", extra FROM nexmark_bid)')
    cat >> "${path}" <<'SQL'
CREATE MATERIALIZED VIEW benchmark_ingest_bid AS SELECT COUNT(*)::BIGINT AS row_count FROM nexmark_bid;
SQL
  fi

  if has_source "${sources}" auction; then
    ctes+=('auction AS (SELECT id, item_name AS "itemName", description, initial_bid AS "initialBid", reserve, date_time AS "dateTime", expires, seller, category, extra FROM nexmark_auction)')
    cat >> "${path}" <<'SQL'
CREATE MATERIALIZED VIEW benchmark_ingest_auction AS SELECT COUNT(*)::BIGINT AS row_count FROM nexmark_auction;
SQL
  fi

  if has_source "${sources}" person; then
    ctes+=('person AS (SELECT id, name, city, state, date_time AS "dateTime", extra FROM nexmark_person)')
    cat >> "${path}" <<'SQL'
CREATE MATERIALIZED VIEW benchmark_ingest_person AS SELECT COUNT(*)::BIGINT AS row_count FROM nexmark_person;
SQL
  fi

  local query_text="${base_query}"
  if (( ${#ctes[@]} > 0 )); then
    query_text="WITH ${ctes[0]}"
    local idx
    for ((idx = 1; idx < ${#ctes[@]}; idx++)); do
      query_text+=", ${ctes[idx]}"
    done
    query_text+=" ${base_query}"
  fi

  cat >> "${path}" <<SQL
CREATE MATERIALIZED VIEW benchmark_result AS
${query_text};
SQL
}

run_floe_query() {
  local query_id="$1"
  local artifact_dir="$2"
  local sources="$3"
  local bid_topic="$4"
  local auction_topic="$5"
  local person_topic="$6"

  mkdir -p "${artifact_dir}"

  local bid_group_id="${FLOE_KAFKA_GROUP_ID_PREFIX}_${RUN_ID}_${query_id}_bid"
  local auction_group_id="${FLOE_KAFKA_GROUP_ID_PREFIX}_${RUN_ID}_${query_id}_auction"
  local person_group_id="${FLOE_KAFKA_GROUP_ID_PREFIX}_${RUN_ID}_${query_id}_person"

  local config_path="${artifact_dir}/floe_config.json"
  local program_path="${artifact_dir}/program.sql"
  write_floe_config "${config_path}" "${sources}" "${bid_topic}" "${auction_topic}" "${person_topic}" "${bid_group_id}" "${auction_group_id}" "${person_group_id}"
  write_floe_program_sql "${program_path}" "${query_id}" "${sources}"

  local program_sql
  program_sql="$(tr '\n' ' ' < "${program_path}")"

  stop_floe_process
  # Prevent stale external floe-node runners from holding the shared pgwire port.
  pkill -f "/target/release/floe-node run" >/dev/null 2>&1 || true
  FLOE_PG_ADDR="127.0.0.1:${FLOE_PG_PORT}" \
    FLOE_ADMIN_PORT=0 \
    "${REPO_ROOT}/target/release/floe-node" run \
    --slatedb-await-durable false \
    --slatedb-l0-sst-bytes "${FLOE_L0_SST_BYTES}" \
    --slatedb-max-unflushed-bytes "${FLOE_MAX_UNFLUSHED_BYTES}" \
    --config "${config_path}" \
    --mv-query "${program_sql}" \
    > "${artifact_dir}/floe-node.stdout.log" \
    2> "${artifact_dir}/floe-node.stderr.log" &
  FLOE_NODE_PID=$!

  if ! wait_for_floe_pg "${artifact_dir}"; then
    stop_floe_process
    return 1
  fi

  local specs=()
  while IFS= read -r spec; do
    [[ -n "${spec}" ]] && specs+=("${spec}")
  done < <(relation_specs_for_sources "${sources}" benchmark_ingest)

  local input_rows
  input_rows="$(input_rows_total_for_sources "${sources}")"

  local start_ms end_ms total_ms rows_per_sec result_rows notes
  start_ms="$(date +%s%3N)"
  produce_for_query_sources "${sources}" "${bid_topic}" "${auction_topic}" "${person_topic}"
  if ! poll_floe_query_completion "${query_id}" "${sources}" "${bid_group_id}" "${auction_group_id}" "${person_group_id}" "${bid_topic}" "${auction_topic}" "${person_topic}"; then
    stop_floe_process
    return 1
  fi
  end_ms="$(date +%s%3N)"
  total_ms=$((end_ms - start_ms))
  if (( total_ms > 0 )); then
    rows_per_sec=$((input_rows * 1000 / total_ms))
  else
    rows_per_sec=0
  fi

  result_rows="$(fetch_pg_scalar "${FLOE_PG_PORT}" postgres postgres "SELECT COUNT(*)::BIGINT FROM benchmark_result")"
  [[ -z "${result_rows}" ]] && result_rows="n/a"
  notes="source_catchup_kafka_group_offsets"
  if [[ -n "$(floe_result_row_target_for_query "${query_id}")" ]]; then
    notes="source_catchup_kafka_group_offsets_and_result_visibility"
  fi

  stop_floe_process
  append_summary_row floe "${query_id}" ok "${total_ms}" "${PRODUCE_MS}" "${POST_PRODUCE_WAIT_MS}" "${rows_per_sec}" "${input_rows}" "${result_rows}" "${notes}"
  return 0
}

run_engine_suite() {
  local engine="$1"
  local queries_file="$2"

  local engine_run_dir="${RUN_DIR}/${engine}"
  mkdir -p "${engine_run_dir}"

  case "${engine}" in
    materialize)
      log "starting Materialize container"
      if ! start_materialize; then
        while IFS= read -r query_id; do
          [[ -z "${query_id}" ]] && continue
          local sources
          sources="$(required_sources_for_query "${query_id}")"
          local input_rows
          input_rows="$(input_rows_total_for_sources "${sources}")"
          record_failure materialize "${query_id}" "engine_start_failed" "${input_rows}"
        done < "${queries_file}"
        return
      fi
      ;;
    risingwave)
      log "starting RisingWave container"
      if ! start_risingwave; then
        while IFS= read -r query_id; do
          [[ -z "${query_id}" ]] && continue
          local sources
          sources="$(required_sources_for_query "${query_id}")"
          local input_rows
          input_rows="$(input_rows_total_for_sources "${sources}")"
          record_failure risingwave "${query_id}" "engine_start_failed" "${input_rows}"
        done < "${queries_file}"
        return
      fi
      ;;
    feldera)
      log "starting Feldera container"
      if ! start_feldera; then
        while IFS= read -r query_id; do
          [[ -z "${query_id}" ]] && continue
          local sources
          sources="$(required_sources_for_query "${query_id}")"
          local input_rows
          input_rows="$(input_rows_total_for_sources "${sources}")"
          record_failure feldera "${query_id}" "engine_start_failed" "${input_rows}"
        done < "${queries_file}"
        return
      fi
      ;;
  esac

  while IFS= read -r query_id; do
    [[ -z "${query_id}" ]] && continue
    local query_artifact_dir="${engine_run_dir}/${query_id}"
    local sources
    sources="$(required_sources_for_query "${query_id}")"

    IFS='|' read -r bid_topic auction_topic person_topic <<< "$(producer_topics_for_query "${engine}" "${query_id}")"

    if has_source "${sources}" bid; then
      reset_topic "${bid_topic}"
    fi
    if has_source "${sources}" auction; then
      reset_topic "${auction_topic}"
    fi
    if has_source "${sources}" person; then
      reset_topic "${person_topic}"
    fi

    local input_rows
    input_rows="$(input_rows_total_for_sources "${sources}")"

    log "running ${engine} ${query_id} (sources: ${sources}, input_rows: ${input_rows})"

    case "${engine}" in
      floe)
        if ! run_floe_query "${query_id}" "${query_artifact_dir}" "${sources}" "${bid_topic}" "${auction_topic}" "${person_topic}"; then
          record_failure floe "${query_id}" "setup_or_completion_failed (see ${query_artifact_dir})" "${input_rows}"
        fi
        ;;
      materialize)
        if ! run_materialize_query "${query_id}" "${query_artifact_dir}" "${sources}" "${bid_topic}" "${auction_topic}" "${person_topic}"; then
          record_failure materialize "${query_id}" "setup_or_completion_failed (see ${query_artifact_dir})" "${input_rows}"
        fi
        ;;
      risingwave)
        if ! run_risingwave_query "${query_id}" "${query_artifact_dir}" "${sources}" "${bid_topic}" "${auction_topic}" "${person_topic}"; then
          record_failure risingwave "${query_id}" "setup_or_completion_failed (see ${query_artifact_dir})" "${input_rows}"
        fi
        ;;
      feldera)
        if ! run_feldera_query "${query_id}" "${query_artifact_dir}" "${sources}" "${bid_topic}" "${auction_topic}" "${person_topic}"; then
          record_failure feldera "${query_id}" "setup_or_completion_failed (see ${query_artifact_dir})" "${input_rows}"
        fi
        ;;
      *)
        die "unknown engine '${engine}'"
        ;;
    esac
  done < "${queries_file}"

  case "${engine}" in
    materialize) stop_materialize ;;
    risingwave) stop_risingwave ;;
    feldera) stop_feldera ;;
  esac
}

main() {
  command -v jq >/dev/null 2>&1 || die "jq is required"
  command -v psql >/dev/null 2>&1 || die "psql is required"
  command -v docker >/dev/null 2>&1 || die "docker is required"
  command -v curl >/dev/null 2>&1 || die "curl is required"

  local queries_file="${RUN_DIR}/queries.txt"
  selected_queries "${QUERY_SELECTOR}" > "${queries_file}"

  cat > "${RESULTS_FILE}" <<EOF2
# Nexmark Cross-Engine Benchmark Summary

Run: \`${RUN_ID}\`
Engine selector: \`${ENGINE}\`
Query selector: \`${QUERY_SELECTOR}\`
Dataset rows: bid=\`${BID_ROWS}\`, auction=\`${AUCTION_ROWS}\`, person=\`${PERSON_ROWS}\`

| Engine | Query | Status | Ingest Complete (s) | Produce (s) | Post-Produce Wait (s) | Input Rows/s | Input Rows | Result Rows | Notes |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
EOF2

  : > "${RESULTS_JSONL}"

  ensure_redpanda
  build_producer

  if [[ "${ENGINE}" == "floe" || "${ENGINE}" == "all" ]]; then
    build_floe_node
  fi

  capture_run_context

  case "${ENGINE}" in
    floe)
      run_engine_suite floe "${queries_file}"
      ;;
    materialize)
      run_engine_suite materialize "${queries_file}"
      ;;
    risingwave)
      run_engine_suite risingwave "${queries_file}"
      ;;
    feldera)
      run_engine_suite feldera "${queries_file}"
      ;;
    all)
      run_engine_suite floe "${queries_file}"
      run_engine_suite materialize "${queries_file}"
      run_engine_suite risingwave "${queries_file}"
      run_engine_suite feldera "${queries_file}"
      ;;
    *)
      die "unknown engine '${ENGINE}' (expected floe|materialize|risingwave|feldera|all)"
      ;;
  esac

  log "results written to ${RESULTS_FILE}"
  cat "${RESULTS_FILE}"
}

main "$@"
