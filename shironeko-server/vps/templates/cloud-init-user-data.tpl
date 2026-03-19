#cloud-config
hostname: __HOSTNAME__
manage_etc_hosts: true

users:
  - default
  - name: __ADMIN_USER__
    groups: [sudo]
    shell: /bin/bash
    sudo: ["ALL=(ALL) ALL"]
    lock_passwd: true
    ssh_authorized_keys:
      - __SSH_PUBLIC_KEY__

package_update: true
packages:
  - qemu-guest-agent
  - ufw
  - fail2ban
  - curl
  - wget
  - vim
  - ca-certificates

write_files:
  - path: /etc/lenovo-tenant-id
    owner: root:root
    permissions: '0644'
    content: |
      __TENANT_ID__
  - path: /home/__ADMIN_USER__/.bashrc
    owner: __ADMIN_USER__:__ADMIN_USER__
    permissions: '0644'
    content: |
      # ~/.bashrc: executed by bash(1) for non-login shells.
      case "$TERM" in
        xterm-color|*-256color) color_prompt=yes;;
      esac
      force_color_prompt=yes

      if [ "$color_prompt" = yes ]; then
        PS1='\[\033[01;32m\]\u@\h\[\033[00m\]:\[\033[01;34m\]\w\[\033[00m\]\$ '
      else
        PS1='\u@\h:\w\$ '
      fi
  - path: /home/__ADMIN_USER__/.profile
    owner: __ADMIN_USER__:__ADMIN_USER__
    permissions: '0644'
    content: |
      # ~/.profile: executed by the command interpreter for login shells.
      if [ -n "$BASH_VERSION" ]; then
        if [ -f "$HOME/.bashrc" ]; then
          . "$HOME/.bashrc"
        fi
      fi
  - path: /usr/local/bin/install-ttyd-cloudflared.sh
    owner: root:root
    permissions: '0755'
    content: |
      #!/usr/bin/env bash
      set -euo pipefail
      export DEBIAN_FRONTEND=noninteractive

      retry() {
        local attempts=5
        local delay=3
        local n=1
        while true; do
          if "$@"; then
            return 0
          fi
          if [ "$n" -ge "$attempts" ]; then
            return 1
          fi
          n=$((n + 1))
          sleep "$delay"
        done
      }

      retry apt-get update -y
      retry apt-get install -y ttyd curl ca-certificates

      if ! command -v cloudflared >/dev/null 2>&1; then
        if apt-cache policy cloudflared 2>/dev/null | grep -q "Candidate:" && ! apt-cache policy cloudflared 2>/dev/null | grep -q "Candidate: (none)"; then
          retry apt-get install -y cloudflared || true
        fi
      fi

      if ! command -v cloudflared >/dev/null 2>&1; then
        ARCH="$(dpkg --print-architecture)"
        case "$ARCH" in
          amd64) URL="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64" ;;
          arm64) URL="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64" ;;
          armhf) URL="https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm" ;;
          *)
            echo "Unsupported architecture: $ARCH" >&2
            exit 1
            ;;
        esac
        curl -fsSL "$URL" -o /usr/local/bin/cloudflared
        chmod +x /usr/local/bin/cloudflared
      fi

  - path: /usr/local/bin/start-ttyd-trycloudflare.sh
    owner: root:root
    permissions: '0755'
    content: |
      #!/usr/bin/env bash
      set -euo pipefail

      PORT="${TTYD_PORT:-7681}"
      AUTH_USER="${TTYD_USER:-__ADMIN_USER__}"
      AUTH_PASS="${TTYD_PASS:-}"

      if [ -z "$AUTH_PASS" ]; then
        echo "TTYD_PASS is required" >&2
        exit 1
      fi

      mkdir -p /var/log/lenovo

      port_in_use() {
        local p="$1"
        if command -v ss >/dev/null 2>&1; then
          ss -ltn "( sport = :${p} )" 2>/dev/null | awk 'NR>1 {print; exit}' | grep -q .
          return $?
        fi
        return 1
      }

      pick_available_port() {
        local base="$1"
        local max_tries=30
        local p
        for p in $(seq "$base" $((base + max_tries))); do
          if ! port_in_use "$p"; then
            echo "$p"
            return 0
          fi
        done
        return 1
      }

      if pgrep -f "ttyd .*--port ${PORT}" >/dev/null 2>&1; then
        pkill -f "ttyd .*--port ${PORT}" >/dev/null 2>&1 || true
        sleep 1
      fi

      ACTUAL_PORT="${PORT}"
      if port_in_use "${ACTUAL_PORT}"; then
        ALT_PORT="$(pick_available_port "${PORT}" || true)"
        if [ -z "${ALT_PORT}" ]; then
          echo "no free port found near ${PORT}" >&2
          exit 1
        fi
        ACTUAL_PORT="${ALT_PORT}"
      fi

      nohup ttyd --port "${ACTUAL_PORT}" --interface 127.0.0.1 --writable -c "${AUTH_USER}:${AUTH_PASS}" /bin/login -f "${AUTH_USER}" >/var/log/lenovo/ttyd.log 2>&1 &
      sleep 1

      if ! pgrep -f "ttyd .*--port ${ACTUAL_PORT}" >/dev/null 2>&1; then
        echo "failed to start ttyd" >&2
        tail -n 60 /var/log/lenovo/ttyd.log >&2 || true
        exit 1
      fi

      pkill -f "cloudflared tunnel --url http://127.0.0.1:${ACTUAL_PORT}" >/dev/null 2>&1 || true
      : > /var/log/lenovo/cloudflared-ttyd.log
      nohup cloudflared tunnel --url "http://127.0.0.1:${ACTUAL_PORT}" --no-autoupdate >/var/log/lenovo/cloudflared-ttyd.log 2>&1 &

      for i in $(seq 1 30); do
        URL="$(grep -Eo 'https://[-a-zA-Z0-9]+\.trycloudflare\.com' /var/log/lenovo/cloudflared-ttyd.log | grep -Ev '^https://api\.trycloudflare\.com$' | tail -n 1 || true)"
        if [ -n "$URL" ]; then
          echo "TTYD_PORT=${ACTUAL_PORT}"
          echo "TRYCLOUDFLARE_URL=$URL"
          exit 0
        fi
        sleep 1
      done

      echo "failed to create trycloudflare URL" >&2
      tail -n 80 /var/log/lenovo/cloudflared-ttyd.log >&2 || true
      exit 1

runcmd:
  - systemctl enable --now qemu-guest-agent
  - sed -i 's/^#\?PermitRootLogin.*/PermitRootLogin no/' /etc/ssh/sshd_config
  - sed -i 's/^#\?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
  - systemctl restart ssh
  - /usr/local/bin/install-ttyd-cloudflared.sh
  - ufw allow OpenSSH
  - ufw --force enable
