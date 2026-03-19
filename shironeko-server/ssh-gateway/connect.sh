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

run_backendctl() {
    if command -v sudo >/dev/null 2>&1; then
        sudo -n /usr/local/sbin/shironeko-backendctl "$@"
        return $?
    fi

    return 127
}

if [ "$BACKEND_TYPE" = "docker" ]; then
    # Check if container is running
    if ! run_backendctl docker-ps-names | grep -q "^${BACKEND_TARGET}$"; then
        echo "Error: Container '$BACKEND_TARGET' is not running"
        echo ""
        echo "Please start your server from the dashboard first."
        exit 1
    fi

    echo "Connecting to your Docker server..."
    echo "==========================================="

    export TERM=xterm-256color
    exec sudo -n /usr/local/sbin/shironeko-backendctl docker-attach "$BACKEND_TARGET"
fi

if [ "$BACKEND_TYPE" = "vm" ]; then
    if ! command -v virsh >/dev/null 2>&1; then
        echo "Error: virsh command not available in SSH gateway"
        exit 1
    fi

    VIRSH_URI="${LIBVIRT_DEFAULT_URI:-qemu:///system}"
    DOMINFO_OUTPUT="$(run_backendctl vm-dominfo "$VIRSH_URI" "$BACKEND_TARGET" 2>&1)"
    if [ $? -ne 0 ]; then
        echo "Error: VM '$BACKEND_TARGET' not found or inaccessible"
        [ -n "$DOMINFO_OUTPUT" ] && echo "Detail: $DOMINFO_OUTPUT"
        exit 1
    fi

    # Start VM automatically if it is not running
    VM_STATE="$(run_backendctl vm-domstate "$VIRSH_URI" "$BACKEND_TARGET" 2>/dev/null | tr -d '\r')"
    if ! echo "$VM_STATE" | grep -qi "running"; then
        echo "Starting VM '$BACKEND_TARGET'..."
        run_backendctl vm-start "$VIRSH_URI" "$BACKEND_TARGET" >/dev/null 2>&1 || true
        sleep 2
    fi

    # Ensure serial console autologins as current SSH user (best effort)
    if command -v jq >/dev/null 2>&1; then
        LOGIN_USER=$(id -un 2>/dev/null || true)
        if [ -n "$LOGIN_USER" ] && printf '%s' "$LOGIN_USER" | grep -Eq '^[a-z_][a-z0-9_-]{0,31}$'; then
            AUTOLOGIN_CMD=$(cat <<EOF
set -e
if id -u ${LOGIN_USER} >/dev/null 2>&1; then
  USER_HOME=/home/${LOGIN_USER}
  mkdir -p /etc/systemd/system/serial-getty@ttyS0.service.d
  printf '%s\n' '[Service]' 'ExecStart=' 'ExecStart=-/sbin/agetty --noissue --autologin ${LOGIN_USER} --keep-baud 115200,38400,9600 %I xterm-256color' 'Type=idle' >/etc/systemd/system/serial-getty@ttyS0.service.d/autologin.conf

  if [ ! -d "\${USER_HOME}" ]; then
    mkdir -p "\${USER_HOME}"
  fi

  if [ ! -f "\${USER_HOME}/.bashrc" ]; then
    if [ -f /etc/skel/.bashrc ]; then
      cp /etc/skel/.bashrc "\${USER_HOME}/.bashrc"
    else
      cat >"\${USER_HOME}/.bashrc" <<'EOBASHRC'
# ~/.bashrc: executed by bash(1) for non-login shells.
case "$TERM" in
  xterm-color|*-256color) color_prompt=yes;;
esac
force_color_prompt=yes
if [ "$color_prompt" = yes ]; then
  PS1='\\[\\033[01;32m\\]\\u@\\h\\[\\033[00m\\]:\\[\\033[01;34m\\]\\w\\[\\033[00m\\]\\$ '
else
  PS1='\\u@\\h:\\w\\$ '
fi
EOBASHRC
    fi
  fi

  if [ ! -f "\${USER_HOME}/.profile" ]; then
    if [ -f /etc/skel/.profile ]; then
      cp /etc/skel/.profile "\${USER_HOME}/.profile"
    else
      cat >"\${USER_HOME}/.profile" <<'EOPROFILE'
# ~/.profile: executed by the command interpreter for login shells.
if [ -n "$BASH_VERSION" ]; then
  if [ -f "$HOME/.bashrc" ]; then
    . "$HOME/.bashrc"
  fi
fi
EOPROFILE
    fi
  fi

  if grep -q '^#force_color_prompt=yes' "\${USER_HOME}/.bashrc"; then
    sed -i 's/^#force_color_prompt=yes/force_color_prompt=yes/' "\${USER_HOME}/.bashrc"
  elif ! grep -q '^force_color_prompt=' "\${USER_HOME}/.bashrc"; then
    printf '\nforce_color_prompt=yes\n' >>"\${USER_HOME}/.bashrc"
  fi

  if ! grep -q '^# SHIRONEKO_COLOR_PROMPT$' "\${USER_HOME}/.bashrc"; then
    cat >>"\${USER_HOME}/.bashrc" <<'EOFORCECOLOR'
# SHIRONEKO_COLOR_PROMPT
if [ -n "$PS1" ] && [ -x /usr/bin/tput ] && tput setaf 2 >/dev/null 2>&1; then
  PS1='\\[\\033[01;32m\\]\\u@\\h\\[\\033[00m\\]:\\[\\033[01;34m\\]\\w\\[\\033[00m\\]\\$ '
fi
EOFORCECOLOR
  fi

  chown ${LOGIN_USER}:${LOGIN_USER} "\${USER_HOME}" "\${USER_HOME}/.bashrc" "\${USER_HOME}/.profile" || true
  chmod 0755 "\${USER_HOME}" || true
  chmod 0644 "\${USER_HOME}/.bashrc" "\${USER_HOME}/.profile" || true

  if getent group sudo >/dev/null 2>&1; then
    usermod -aG sudo ${LOGIN_USER} || true
  elif getent group wheel >/dev/null 2>&1; then
    usermod -aG wheel ${LOGIN_USER} || true
  fi

  rm -f /etc/sudoers.d/90-${LOGIN_USER} || true

  systemctl daemon-reload
  systemctl reset-failed serial-getty@ttyS0 || true
  systemctl restart serial-getty@ttyS0 || true
fi
EOF
)
            PAYLOAD=$(jq -nc --arg c "$AUTOLOGIN_CMD" '{execute:"guest-exec",arguments:{path:"/bin/bash",arg:["-lc",$c],"capture-output":true}}')
            run_backendctl vm-agent-command "$VIRSH_URI" "$BACKEND_TARGET" "$PAYLOAD" >/dev/null 2>&1 || true
        fi
    fi

    export TERM=xterm-256color
    exec sudo -n /usr/local/sbin/shironeko-backendctl vm-console "$VIRSH_URI" "$BACKEND_TARGET"
fi

echo "Error: Unsupported backend type '$BACKEND_TYPE'"
exit 1
