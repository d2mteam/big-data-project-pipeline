#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
FLINK_JM="big-data-project-flink-jobmanager-1"
REST="http://localhost:8081"

G='\033[0;32m'; R='\033[0;31m'; B='\033[0;34m'; W='\033[1;37m'; N='\033[0m'
ok()   { echo -e "${G}✓${N} $*"; }
err()  { echo -e "${R}✗${N} $*" >&2; }
info() { echo -e "${B}▶${N} $*"; }

JOBS=(
    flink-enrich-job-submitter
    flink-predict-job-submitter
    flink-congestion-cep-job-submitter
    flink-accident-anomaly-job-submitter
)

JOB_NAMES=(
    "raw-to-enriched-state-window"
    "enriched-to-kmeans-state-prediction-30m"
    "congestion-cep-alerts"
    "accident-anomaly-alerts"
)

rest() { docker exec "$FLINK_JM" bash -c "curl -s $*" 2>/dev/null; }

list_running() {
    rest "http://localhost:8081/jobs/overview" \
        | python3 -c "
import json,sys
jobs=[j for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING']
for j in jobs: print(j['jid']+'|'+j['name']+'|'+str(j['tasks']['total']))
"
}

cmd_list() {
    echo -e "\n${W}Flink jobs:${N}"
    rest "http://localhost:8081/jobs/overview" | python3 -c "
import json,sys
jobs=json.load(sys.stdin)['jobs']
# Chỉ giữ job mới nhất của mỗi tên (theo start-time)
latest={}
for j in jobs:
    name=j['name']
    if name not in latest or j['start-time']>latest[name]['start-time']:
        latest[name]=j
for j in sorted(latest.values(), key=lambda x: x['start-time']):
    icon = '\033[0;32m✓\033[0m' if j['state']=='RUNNING' else '\033[0;31m✗\033[0m'
    print(f\"  {icon} {j['state']:10s} {j['name'][:45]:45s} tasks={j['tasks']['total']}\")
" 2>/dev/null || err "Flink không reachable"
}

cmd_submit() {
    local target=${1:-all}
    info "Submitting jobs (target=$target)..."
    for svc in "${JOBS[@]}"; do
        if [[ "$target" == "all" ]] || [[ "$svc" == *"$target"* ]]; then
            docker compose -f "$PROJECT_DIR/docker-compose.yml" run --rm "$svc" \
                2>&1 | grep -E "submitted with JobID|already running" &
        fi
    done
    wait
    ok "Done."
}

cmd_cancel() {
    local target=${1:-all}
    info "Cancelling jobs (target=$target)..."
    while IFS='|' read -r jid name tasks; do
        if [[ "$target" == "all" ]] || [[ "$name" == *"$target"* ]]; then
            rest "'http://localhost:8081/jobs/${jid}?mode=cancel' -X PATCH" > /dev/null
            ok "  cancelled: $name"
        fi
    done < <(list_running)
}

cmd_restart() {
    local target=${1:-all}
    cmd_cancel "$target"
    sleep 4
    cmd_submit "$target"
}

usage() {
    echo -e "${W}Usage:${N} $(basename "$0") <command> [job]\n"
    echo -e "${B}Commands:${N}"
    echo "  list                    Xem tất cả jobs + trạng thái"
    echo "  submit  [job|all]       Nộp job(s)"
    echo "  cancel  [job|all]       Xóa job(s) đang chạy"
    echo "  restart [job|all]       Cancel rồi submit lại"
    echo ""
    echo -e "${B}Job names (có thể dùng substring):${N}"
    for n in "${JOB_NAMES[@]}"; do echo "  $n"; done
}

case "${1:-}" in
    list)    cmd_list ;;
    submit)  cmd_submit  "${2:-all}" ;;
    cancel)  cmd_cancel  "${2:-all}" ;;
    restart) cmd_restart "${2:-all}" ;;
    *)       usage ;;
esac
