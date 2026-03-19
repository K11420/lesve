#!/bin/bash
# Shell script that runs when SSH user logs in
# Connects to Docker container or VM backend

BACKEND_TYPE_FILE="$HOME/.backend_type"
BACKEND_TARGET_FILE="$HOME/.backend_target"

if [ ! -f "$BACKEND_TYPE_FILE" ] || [ ! -f "$BACKEND_TARGET_FILE" ]; then
    echo "Error: Backend configuration not found"
    exit 1
fi

BACKEND_TYPE=$(cat "$BACKEND_TYPE_FILE")
BACKEND_TARGET=$(cat "$BACKEND_TARGET_FILE")

if [ -z "$BACKEND_TYPE" ] || [ -z "$BACKEND_TARGET" ]; then
    echo "Error: Backend settings are empty"
    exit 1
fi

if [ "$BACKEND_TYPE" = "docker" ]; then
    # Check if container is running
    if ! docker ps --format '{{.Names}}' | grep -q "^${BACKEND_TARGET}$"; then
        echo "Error: Container '$BACKEND_TARGET' is not running"
        echo ""
        echo "Please start your server from the dashboard first."
        exit 1
    fi

    echo "Connecting to your Docker server..."
    echo "==========================================="

    exec docker exec -it "$BACKEND_TARGET" /bin/bash
fi

if [ "$BACKEND_TYPE" = "vm" ]; then
    if ! command -v virsh >/dev/null 2>&1; then
        echo "Error: virsh command not available in SSH gateway"
        exit 1
    fi

    VIRSH_URI="${LIBVIRT_DEFAULT_URI:-qemu:///system}"
    VIRSH_CMD="virsh -c $VIRSH_URI"

    if ! $VIRSH_CMD dominfo "$BACKEND_TARGET" >/dev/null 2>&1; then
        echo "Error: VM '$BACKEND_TARGET' not found"
        exit 1
    fi

    # Start VM automatically if it is not running
    VM_STATE=$($VIRSH_CMD domstate "$BACKEND_TARGET" 2>/dev/null | tr -d '\r')
    if ! echo "$VM_STATE" | grep -qi "running"; then
        echo "Starting VM '$BACKEND_TARGET'..."
        $VIRSH_CMD start "$BACKEND_TARGET" >/dev/null 2>&1 || true
        sleep 2
    fi

    # Ensure serial console autologins as current SSH user (best effort)
    if command -v jq >/dev/null 2>&1; then
        LOGIN_USER=$(id -un 2>/dev/null || true)
        if [ -n "$LOGIN_USER" ]; then
            AUTOLOGIN_CMD="if id -u ${LOGIN_USER} >/dev/null 2>&1; then mkdir -p /etc/systemd/system/serial-getty@ttyS0.service.d; printf '%s\n' '[Service]' 'ExecStart=' 'ExecStart=-/sbin/agetty --noissue --autologin ${LOGIN_USER} --keep-baud 115200,38400,9600 %I xterm-256color' 'Type=idle' > /etc/systemd/system/serial-getty@ttyS0.service.d/autologin.conf; systemctl daemon-reload; systemctl reset-failed serial-getty@ttyS0 || true; systemctl restart serial-getty@ttyS0 || true; fi"
            PAYLOAD=$(jq -nc --arg c "$AUTOLOGIN_CMD" '{execute:"guest-exec",arguments:{path:"/bin/bash",arg:["-lc",$c],"capture-output":true}}')
            $VIRSH_CMD qemu-agent-command "$BACKEND_TARGET" "$PAYLOAD" >/dev/null 2>&1 || true
        fi
    fi

    export TERM=xterm-256color
    exec virsh -c "$VIRSH_URI" -q console "$BACKEND_TARGET"
fi

echo "Error: Unsupported backend type '$BACKEND_TYPE'"
exit 1
