#!/usr/bin/env bash
# scale.sh — Scale up/down Kafka, Flink, and producers in the big-data pipeline.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE="$PROJECT_DIR/docker-compose.yml"
KAFKA_C="big-data-project-kafka-1"
FLINK_JM="big-data-project-flink-jobmanager-1"

# ── colors ────────────────────────────────────────────────────────────────────
R='\033[0;31m'; G='\033[0;32m'; Y='\033[1;33m'
B='\033[0;34m'; C='\033[0;36m'; W='\033[1;37m'; N='\033[0m'
info()   { echo -e "${B}▶${N} $*"; }
ok()     { echo -e "${G}✓${N} $*"; }
warn()   { echo -e "${Y}⚠${N} $*"; }
err()    { echo -e "${R}✗${N} $*" >&2; }
banner() { echo -e "\n${W}$*${N}"; }

# ── read current config ───────────────────────────────────────────────────────
cfg_parallelism() {
    grep 'parallelism\.default:' "$COMPOSE" | head -1 | awk '{print $NF}'
}

cfg_producer_count() {
    grep 'PRODUCER_COUNT:' "$COMPOSE" | head -1 | tr -d '"' | awk -F': ' '{print $2}' | xargs
}

cfg_producer_interval() {
    # Only read from anchor block (not weather producer)
    python3 - "$COMPOSE" <<'PY'
import re, sys
txt = open(sys.argv[1]).read()
anchor = txt.split('services:')[0]
m = re.search(r'POLL_INTERVAL_MS:\s*"(\d+)"', anchor)
print(m.group(1) if m else '?')
PY
}

cfg_kafka_partitions() {
    docker exec "$KAFKA_C" \
        kafka-topics --bootstrap-server localhost:9092 --describe --topic trafic.raw_sensor 2>/dev/null \
        | grep -o 'PartitionCount: [0-9]*' | awk '{print $2; exit}' || echo "?"
}

# ── write config ──────────────────────────────────────────────────────────────
write_parallelism() {
    local n=$1
    sed -i "s/parallelism\.default: [0-9]*/parallelism.default: $n/g" "$COMPOSE"
    ok "docker-compose.yml: parallelism.default → $n"
}

write_producer_count() {
    local n=$1
    sed -i "s/PRODUCER_COUNT: \"[0-9]*\"/PRODUCER_COUNT: \"$n\"/" "$COMPOSE"
    ok "docker-compose.yml: PRODUCER_COUNT → $n"
}

write_producer_interval() {
    local ms=$1
    # Only replace inside the anchor block (before 'services:')
    python3 - <<PY "$COMPOSE" "$ms"
import re, sys
path, ms = sys.argv[1], sys.argv[2]
txt = open(path).read()
anchor, rest = txt.split('services:', 1)
anchor = re.sub(r'(POLL_INTERVAL_MS:\s*")(\d+)(")', lambda m: m.group(1) + ms + m.group(3), anchor)
open(path, 'w').write(anchor + 'services:' + rest)
PY
    ok "docker-compose.yml: POLL_INTERVAL_MS → ${ms}ms (sensor producers)"
}

write_kafka_partitions_in_compose() {
    local n=$1
    # Update --partitions N inside kafka-topic-init command
    sed -i "s/--partitions [0-9]*/--partitions $n/g" "$COMPOSE"
    ok "docker-compose.yml: kafka-topic-init partitions → $n"
}

# ── kafka ─────────────────────────────────────────────────────────────────────
do_kafka_partitions() {
    local n=$1
    local current
    current=$(cfg_kafka_partitions)

    if [[ "$current" == "?" ]]; then
        err "Kafka không reachable — kiểm tra container $KAFKA_C"; return 1
    fi

    if (( current >= n )); then
        warn "Topics đang có $current partitions ≥ $n. Kafka không thể giảm partitions."
        warn "Muốn giảm cần xóa & tạo lại topic (mất data). Dùng: $0 kafka recreate <n>"
        return 0
    fi

    info "Tăng partitions từ $current → $n cho tất cả topics..."
    local topics=(
        trafic.raw_sensor trafic.raw_weather
        trafic.enriched_events trafic.predictions
        trafic.congestion_alerts trafic.accident_anomaly_alerts
    )
    for topic in "${topics[@]}"; do
        docker exec "$KAFKA_C" \
            kafka-topics --bootstrap-server localhost:9092 \
            --alter --topic "$topic" --partitions "$n" 2>&1 | grep -v '^$' || true
        ok "  $topic → $n partitions"
    done
    write_kafka_partitions_in_compose "$n"
}

do_kafka_recreate() {
    local n=$1
    warn "Recreate topics sẽ XÓA toàn bộ data hiện tại!"
    read -r -p "Xác nhận? [y/N] " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || { info "Hủy."; return 0; }

    local topics=(
        trafic.raw_sensor trafic.raw_weather
        trafic.enriched_events trafic.predictions
        trafic.congestion_alerts trafic.accident_anomaly_alerts
    )
    for topic in "${topics[@]}"; do
        docker exec "$KAFKA_C" \
            kafka-topics --bootstrap-server localhost:9092 --delete --topic "$topic" 2>/dev/null || true
    done
    sleep 2
    for topic in "${topics[@]}"; do
        docker exec "$KAFKA_C" \
            kafka-topics --bootstrap-server localhost:9092 \
            --create --topic "$topic" --partitions "$n" --replication-factor 1 2>&1 | grep -v '^$' || true
        ok "  $topic: $n partitions (fresh)"
    done
    write_kafka_partitions_in_compose "$n"
    # Restart Flink jobs to pick up new topics
    _submit_flink_jobs
}

# ── flink ─────────────────────────────────────────────────────────────────────
_wait_flink() {
    info "Đợi Flink REST API..."
    for i in $(seq 1 25); do
        docker exec "$FLINK_JM" flink list -m localhost:8081 >/dev/null 2>&1 \
            && ok "Flink ready (attempt $i)" && return 0
        sleep 2
    done
    err "Flink không ready sau 50s"; return 1
}

_submit_flink_jobs() {
    info "Submitting 4 Flink jobs..."
    local jobs=(
        flink-enrich-job-submitter
        flink-predict-job-submitter
        flink-congestion-cep-job-submitter
        flink-accident-anomaly-job-submitter
    )
    for job in "${jobs[@]}"; do
        docker compose -f "$COMPOSE" run --rm "$job" 2>&1 \
            | grep -E "submitted with JobID|already running" &
    done
    wait
    ok "Tất cả Flink jobs đã submit."
}

do_flink_parallelism() {
    local n=$1
    write_parallelism "$n"
    info "Restart Flink jobmanager + taskmanager..."
    docker compose -f "$COMPOSE" up -d --force-recreate flink-jobmanager flink-taskmanager \
        2>&1 | grep -E "Started|Running|Recreat" || true
    _wait_flink
    _submit_flink_jobs
}

# ── producers ─────────────────────────────────────────────────────────────────
do_producers() {
    local count=$1 interval=$2

    [[ "$count" =~ ^[1-3]$ ]] || { err "count phải là 1, 2 hoặc 3"; return 1; }
    (( interval >= 50 && interval <= 10000 )) || { err "interval phải từ 50–10000 ms"; return 1; }

    write_producer_count "$count"
    write_producer_interval "$interval"

    info "Restart sensor producers (${count} active, ${interval}ms interval)..."
    for i in 0 1 2; do
        if (( i < count )); then
            docker compose -f "$COMPOSE" up -d --force-recreate "traffic-sensor-producer-$i" \
                2>&1 | grep -E "Started|Running" || true
            ok "  producer-$i started"
        else
            docker compose -f "$COMPOSE" stop "traffic-sensor-producer-$i" 2>/dev/null \
                && warn "  producer-$i stopped" || true
        fi
    done
}

# ── throughput sample ─────────────────────────────────────────────────────────
do_throughput() {
    local secs=${1:-5}
    local topics=(trafic.raw_sensor trafic.enriched_events trafic.predictions)
    declare -A o1 o2

    for t in "${topics[@]}"; do
        o1[$t]=$(docker exec "$KAFKA_C" \
            bash -c "kafka-get-offsets --bootstrap-server localhost:9092 --topic $t --time -1 2>/dev/null" \
            | awk -F: '{s+=$3} END{print s+0}')
    done

    printf "  Sampling %ds" "$secs"
    for _ in $(seq 1 "$secs"); do printf "."; sleep 1; done
    echo ""

    for t in "${topics[@]}"; do
        o2[$t]=$(docker exec "$KAFKA_C" \
            bash -c "kafka-get-offsets --bootstrap-server localhost:9092 --topic $t --time -1 2>/dev/null" \
            | awk -F: '{s+=$3} END{print s+0}')
        local rate=$(( (${o2[$t]} - ${o1[$t]}) / secs ))
        printf "  %-32s ${G}%d rec/s${N}\n" "$t" "$rate"
    done
}

# ── status ────────────────────────────────────────────────────────────────────
do_status() {
    banner "═══ Big Data Pipeline ─ Scale Status ═══"

    echo -e "\n${C}Config (docker-compose.yml):${N}"
    printf "  Flink parallelism   : %s\n" "$(cfg_parallelism)"
    printf "  Sensor producers    : %s active  (POLL_INTERVAL=%sms)\n" \
        "$(cfg_producer_count)" "$(cfg_producer_interval)"
    printf "  Kafka partitions    : %s (per topic)\n" "$(cfg_kafka_partitions)"

    echo -e "\n${C}Containers:${N}"
    local services=(
        kafka flink-jobmanager flink-taskmanager
        traffic-sensor-producer-0 traffic-sensor-producer-1 traffic-sensor-producer-2
        traffic-weather-producer airflow clickhouse
    )
    for svc in "${services[@]}"; do
        local cname="big-data-project-${svc}-1"
        local state
        state=$(docker inspect "$cname" --format "{{.State.Status}}" 2>/dev/null || echo "missing")
        if [[ "$state" == "running" ]]; then
            printf "  ${G}✓${N} %-38s %s\n" "$svc" "$state"
        else
            printf "  ${R}✗${N} %-38s %s\n" "$svc" "$state"
        fi
    done

    echo -e "\n${C}Flink jobs:${N}"
    if docker exec "$FLINK_JM" flink list -r -m localhost:8081 >/dev/null 2>&1; then
        docker exec "$FLINK_JM" flink list -r -m localhost:8081 2>/dev/null \
            | grep -E 'RUNNING|FAILED|CANCELED' \
            | while read -r line; do
                # format: "date time : jobid : jobname (STATE)"
                local name; name=$(echo "$line" | awk '{print $(NF-1)}')
                if echo "$line" | grep -q RUNNING; then
                    echo -e "  ${G}✓${N} $name"
                else
                    echo -e "  ${R}✗${N} $name"
                fi
            done
    else
        warn "  Flink không reachable"
    fi

    echo -e "\n${C}Throughput (5s sample):${N}"
    do_throughput 5
}

# ── presets ───────────────────────────────────────────────────────────────────
do_preset() {
    local preset=$1
    case "$preset" in
        light)
            banner "Preset: light  (tiết kiệm CPU/RAM, ~15 rec/s)"
            do_producers 1 1000
            do_flink_parallelism 1
            ;;
        demo)
            banner "Preset: demo  (balanced, ~100 rec/s)"
            do_kafka_partitions 6
            do_producers 3 500
            do_flink_parallelism 2
            ;;
        heavy)
            banner "Preset: heavy  (max throughput, ~200+ rec/s)"
            do_kafka_partitions 6
            do_producers 3 200
            do_flink_parallelism 3
            ;;
        *)
            err "Unknown preset: '$preset'. Dùng: light | demo | heavy"
            exit 1
            ;;
    esac
    echo ""
    ok "Preset '$preset' applied."
    echo -e "  Chạy ${C}$0 status${N} để verify."
}

# ── usage ─────────────────────────────────────────────────────────────────────
usage() {
    echo -e "${W}Usage:${N} $(basename "$0") <command> [args]\n"
    echo -e "${C}Presets (one-shot scale):${N}"
    echo "  preset light             1 producer, Flink p=1, interval=1000ms  → ~15 rec/s  (low CPU)"
    echo "  preset demo              3 producers, Flink p=2, interval=500ms   → ~100 rec/s"
    echo "  preset heavy             3 producers, Flink p=3, interval=200ms   → ~200+ rec/s"
    echo ""
    echo -e "${C}Components:${N}"
    echo "  flink parallelism <1-4>  Restart Flink với parallelism mới"
    echo "  kafka partitions  <N>    Tăng partitions (chỉ tăng được, không giảm)"
    echo "  kafka recreate    <N>    Xóa + tạo lại topics với N partitions (mất data)"
    echo "  producers count   <1-3>  Số sensor producer instances đang chạy"
    echo "  producers interval <ms>  Batch interval ms (thấp hơn = nhanh hơn)"
    echo ""
    echo -e "${C}Other:${N}"
    echo "  status                   Show config + container status + live throughput"
    echo "  throughput [seconds]     Đo throughput nhanh (default 5s)"
}

# ── dispatch ──────────────────────────────────────────────────────────────────
case "${1:-}" in
    status)    do_status ;;
    throughput) do_throughput "${2:-5}" ;;
    preset)    do_preset "${2:?$(usage; exit 1)}" ;;

    flink)
        case "${2:-}" in
            parallelism) do_flink_parallelism "${3:?Usage: $0 flink parallelism <N>}" ;;
            *) err "Unknown: flink ${2:-}"; usage; exit 1 ;;
        esac ;;

    kafka)
        case "${2:-}" in
            partitions) do_kafka_partitions "${3:?Usage: $0 kafka partitions <N>}" ;;
            recreate)   do_kafka_recreate   "${3:?Usage: $0 kafka recreate <N>}" ;;
            *) err "Unknown: kafka ${2:-}"; usage; exit 1 ;;
        esac ;;

    producers)
        case "${2:-}" in
            count)    do_producers "${3:?Usage: $0 producers count <N>}" "$(cfg_producer_interval)" ;;
            interval) do_producers "$(cfg_producer_count)" "${3:?Usage: $0 producers interval <ms>}" ;;
            *) err "Unknown: producers ${2:-}"; usage; exit 1 ;;
        esac ;;

    *) usage ;;
esac
