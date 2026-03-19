#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Create a full pre-migration backup for lenovo-server.

Usage:
  backup-lenovo-before-vps.sh [options]

Options:
  --project-dir <dir>   lenovo-server project root (default: script parent)
  --output-dir <dir>    backup destination (default: <project>/backups)
  --help                show help

Example:
  ./scripts/backup-lenovo-before-vps.sh
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project-dir)
      PROJECT_DIR="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$OUTPUT_DIR" ]]; then
  OUTPUT_DIR="$PROJECT_DIR/backups"
fi

if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync command not found" >&2
  exit 1
fi

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP_ROOT="${OUTPUT_DIR%/}/lenovo-pre-vps-${TIMESTAMP}"
mkdir -p "$BACKUP_ROOT" "$BACKUP_ROOT/meta" "$BACKUP_ROOT/project" "$BACKUP_ROOT/volumes"

echo "[1/6] Backing up project files..."
rsync -a \
  --exclude 'backend/node_modules' \
  --exclude 'frontend/node_modules' \
  --exclude 'backups' \
  "$PROJECT_DIR/" "$BACKUP_ROOT/project/"

echo "[2/6] Capturing core config snapshots..."
for f in \
  "$PROJECT_DIR/.env" \
  "$PROJECT_DIR/.env.example" \
  "$PROJECT_DIR/docker-compose.yml" \
  "$PROJECT_DIR/docker-compose.simple.yml" \
  "$PROJECT_DIR/PRODUCTION.md"
  do
  [[ -f "$f" ]] && cp "$f" "$BACKUP_ROOT/meta/"
done

echo "[3/6] Capturing SQLite DB and data directories..."
DB_PATH="$PROJECT_DIR/data/lenovo.db"
[[ -f "$DB_PATH" ]] || DB_PATH="$PROJECT_DIR/data/shironeko.db"
[[ -f "$DB_PATH" ]] && cp "$DB_PATH" "$BACKUP_ROOT/meta/$(basename "$DB_PATH")"
[[ -d "$PROJECT_DIR/data/containers" ]] && tar czf "$BACKUP_ROOT/meta/containers-dir.tar.gz" -C "$PROJECT_DIR/data" containers
[[ -d "$PROJECT_DIR/data/backups" ]] && tar czf "$BACKUP_ROOT/meta/backups-dir.tar.gz" -C "$PROJECT_DIR/data" backups

echo "[4/6] Capturing Docker runtime metadata..."
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker ps -a --no-trunc > "$BACKUP_ROOT/meta/docker-ps-a.txt"
  docker images --digests > "$BACKUP_ROOT/meta/docker-images.txt"
  docker volume ls > "$BACKUP_ROOT/meta/docker-volume-ls.txt"

  for volume in lenovo-data lenovo-containers lenovo-backups; do
    if docker volume inspect "$volume" >/dev/null 2>&1; then
      echo "  - backing up volume $volume"
      docker run --rm \
        -v "$volume:/source:ro" \
        -v "$BACKUP_ROOT/volumes:/backup" \
        busybox \
        sh -c "tar czf /backup/${volume}.tar.gz -C /source ."
    fi
  done
else
  echo "  - Docker daemon unavailable, skipped live metadata/volume backup"
fi

echo "[5/6] Generating restore helper..."
cat > "$BACKUP_ROOT/meta/RESTORE_HINTS.txt" <<HINT
Restore priority:
1) Restore lenovo.db
2) Restore containers-dir.tar.gz to data/containers
3) Restore backups-dir.tar.gz to data/backups
4) (If needed) restore docker volumes from volumes/*.tar.gz

Example:
  tar xzf containers-dir.tar.gz -C /path/to/lenovo-server/data
HINT

echo "[6/6] Writing checksums..."
(
  cd "$BACKUP_ROOT"
  find . -type f -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS
)

echo "Backup completed: $BACKUP_ROOT"
