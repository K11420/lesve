#!/bin/bash
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
    echo "backendctl must run as root" >&2
    exit 1
fi

if [ $# -lt 1 ]; then
    echo "Usage: backendctl <action> [args...]" >&2
    exit 2
fi

ACTION="$1"
shift

SSH_USER="${SUDO_USER:-}"
if [ -z "$SSH_USER" ]; then
    echo "SUDO_USER is required" >&2
    exit 1
fi

if ! printf '%s' "$SSH_USER" | grep -Eq '^srv-[a-z0-9]+$'; then
    echo "Invalid ssh user context" >&2
    exit 1
fi

USER_HOME="/home/${SSH_USER}"
BACKEND_TYPE_FILE="${USER_HOME}/.backend_type"
BACKEND_TARGET_FILE="${USER_HOME}/.backend_target"

if [ ! -f "$BACKEND_TYPE_FILE" ] || [ ! -f "$BACKEND_TARGET_FILE" ]; then
    echo "Backend mapping not found for ${SSH_USER}" >&2
    exit 1
fi

BACKEND_TYPE="$(cat "$BACKEND_TYPE_FILE")"
BACKEND_TARGET="$(cat "$BACKEND_TARGET_FILE")"

validate_uri() {
    local uri="$1"
    if [ "$uri" != "qemu:///system" ]; then
        echo "Unsupported libvirt URI: ${uri}" >&2
        exit 1
    fi
}

validate_target() {
    local requested="$1"
    if [ "$requested" != "$BACKEND_TARGET" ]; then
        echo "Access denied for backend target: ${requested}" >&2
        exit 1
    fi
}

ensure_backend_type() {
    local expected="$1"
    if [ "$BACKEND_TYPE" != "$expected" ]; then
        echo "Backend type mismatch (expected ${expected}, actual ${BACKEND_TYPE})" >&2
        exit 1
    fi
}

case "$ACTION" in
    docker-ps-names)
        ensure_backend_type docker
        exec /usr/bin/docker ps --format '{{.Names}}'
        ;;
    docker-attach)
        ensure_backend_type docker
        TARGET="${1:-$BACKEND_TARGET}"
        validate_target "$TARGET"
        exec /usr/bin/docker exec -it "$TARGET" /bin/bash -l
        ;;
    vm-dominfo)
        ensure_backend_type vm
        URI="${1:-}"
        TARGET="${2:-}"
        [ -n "$URI" ] && [ -n "$TARGET" ] || { echo "vm-dominfo requires uri and target" >&2; exit 2; }
        validate_uri "$URI"
        validate_target "$TARGET"
        exec /usr/bin/virsh -c "$URI" dominfo "$TARGET"
        ;;
    vm-domstate)
        ensure_backend_type vm
        URI="${1:-}"
        TARGET="${2:-}"
        [ -n "$URI" ] && [ -n "$TARGET" ] || { echo "vm-domstate requires uri and target" >&2; exit 2; }
        validate_uri "$URI"
        validate_target "$TARGET"
        exec /usr/bin/virsh -c "$URI" domstate "$TARGET"
        ;;
    vm-start)
        ensure_backend_type vm
        URI="${1:-}"
        TARGET="${2:-}"
        [ -n "$URI" ] && [ -n "$TARGET" ] || { echo "vm-start requires uri and target" >&2; exit 2; }
        validate_uri "$URI"
        validate_target "$TARGET"
        exec /usr/bin/virsh -c "$URI" start "$TARGET"
        ;;
    vm-agent-command)
        ensure_backend_type vm
        URI="${1:-}"
        TARGET="${2:-}"
        PAYLOAD="${3:-}"
        [ -n "$URI" ] && [ -n "$TARGET" ] && [ -n "$PAYLOAD" ] || { echo "vm-agent-command requires uri target payload" >&2; exit 2; }
        validate_uri "$URI"
        validate_target "$TARGET"
        exec /usr/bin/virsh -c "$URI" qemu-agent-command "$TARGET" "$PAYLOAD"
        ;;
    vm-console)
        ensure_backend_type vm
        URI="${1:-}"
        TARGET="${2:-}"
        [ -n "$URI" ] && [ -n "$TARGET" ] || { echo "vm-console requires uri and target" >&2; exit 2; }
        validate_uri "$URI"
        validate_target "$TARGET"
        exec /usr/bin/virsh -c "$URI" -q console "$TARGET"
        ;;
    *)
        echo "Unsupported action: ${ACTION}" >&2
        exit 2
        ;;
esac
