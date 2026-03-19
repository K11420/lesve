#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-shironeko-server}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:4103/api/health}"

print_section() {
  printf '\n[%s]\n' "$1"
}

print_kv() {
  printf '%-36s %s\n' "$1" "$2"
}

warn() {
  printf 'WARN: %s\n' "$1"
}

print_section "Host Disk"
df -h /
usage_pct="$(df --output=pcent / | awk 'NR==2{gsub(/%/,"",$1); print $1}')"
if [[ "${usage_pct:-0}" -ge 90 ]]; then
  warn "root filesystem usage is ${usage_pct}% (>= 90%)"
elif [[ "${usage_pct:-0}" -ge 85 ]]; then
  warn "root filesystem usage is ${usage_pct}% (>= 85%)"
else
  print_kv "disk_threshold" "OK (${usage_pct}%)"
fi

print_section "Containers"
/usr/bin/docker ps --format '{{.Names}}\t{{.Status}}' | sed 's/\t/  /g'
if ! /usr/bin/docker ps --format '{{.Names}}' | rg -q "^${SERVICE_NAME}$"; then
  warn "${SERVICE_NAME} container is not running"
fi

print_section "Health Endpoint"
if health_json="$(curl -fsS "${HEALTH_URL}")"; then
  print_kv "health_url" "${HEALTH_URL}"
  print_kv "health_json" "${health_json}"
else
  warn "health endpoint failed: ${HEALTH_URL}"
fi

print_section "Runtime Env (container)"
/usr/bin/docker exec "${SERVICE_NAME}" sh -lc '
for k in \
  STRIPE_SECRET_KEY \
  STRIPE_WEBHOOK_SECRET \
  DISCORD_OPS_WEBHOOK_URL \
  DISCORD_SUPPORT_WEBHOOK_URL \
  DISCORD_COMPLIANCE_WEBHOOK_URL \
  AUTO_BACKUP_ENABLED \
  AUTO_BACKUP_INTERVAL_MINUTES \
  SYSTEM_MONITOR_ENABLED \
  SYSTEM_MONITOR_DISK_WARN_PERCENT \
  RATE_LIMIT_ENABLED; do
  eval v=\$$k
  if [ -n "$v" ]; then
    printf "%-36s set\n" "$k"
  else
    printf "%-36s empty\n" "$k"
  fi
done
'

print_section "Backups"
/usr/bin/docker exec "${SERVICE_NAME}" sh -lc '
if [ -d /app/data/backups ]; then
  ls -1t /app/data/backups | head -n 10
else
  echo "/app/data/backups not found"
fi
'

print_section "Stripe Webhook Event Count (best-effort)"
/usr/bin/docker exec "${SERVICE_NAME}" sh -lc '
if command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 /app/data/shironeko.db "SELECT event_type,status,COUNT(*) FROM stripe_webhook_events GROUP BY event_type,status ORDER BY COUNT(*) DESC;"
else
  echo "sqlite3 missing in container (skip)"
fi
'

print_section "Done"
echo "preflight completed"
