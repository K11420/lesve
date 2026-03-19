import express from 'express';
import cors from 'cors';
import { WebSocketServer } from 'ws';
import http from 'http';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import { v4 as uuidv4 } from 'uuid';
import Docker from 'dockerode';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import multer from 'multer';
import Database from 'better-sqlite3';
import { spawn } from 'child_process';
import crypto from 'crypto';
import {
    generateRegistrationOptions,
    verifyRegistrationResponse,
    generateAuthenticationOptions,
    verifyAuthenticationResponse
} from '@simplewebauthn/server';
import { authenticator } from 'otplib';
import QRCode from 'qrcode';
import Stripe from 'stripe';
import { registerPasskeyLoginRoutes } from './passkey-login.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ============ 設定 ============
const CONFIG = {
    JWT_SECRET: process.env.JWT_SECRET || 'shironeko-secret-key-2024-production',
    PORT: process.env.PORT || 4103,
    HOST: process.env.HOST || '0.0.0.0',
    PUBLIC_HOST: process.env.PUBLIC_HOST || 'lenovo.schale41.jp',
    SSH_HOST: process.env.SSH_HOST || '106.73.68.66',
    ADMIN_HOST: process.env.ADMIN_HOST || 'manage.schale41.jp',
    DATA_DIR: process.env.DATA_DIR || path.join(__dirname, '../data'),
    CONTAINERS_DIR: process.env.CONTAINERS_DIR || path.join(__dirname, '../data/containers'),
    BACKUPS_DIR: process.env.BACKUPS_DIR || path.join(__dirname, '../data/backups'),
    DB_PATH: process.env.DB_PATH || path.join(__dirname, '../data/shironeko.db'),
    MAX_FILE_SIZE: 100 * 1024 * 1024, // 100MB
    
    // Discord OAuth設定
    DISCORD_CLIENT_ID: process.env.DISCORD_CLIENT_ID || '',
    DISCORD_CLIENT_SECRET: process.env.DISCORD_CLIENT_SECRET || '',
    DISCORD_REDIRECT_URI: process.env.DISCORD_REDIRECT_URI || 'https://lenovo.schale41.jp/api/auth/discord/callback',
    
    // Cloudflare Turnstile設定
    TURNSTILE_SITE_KEY: process.env.TURNSTILE_SITE_KEY || '',
    TURNSTILE_SECRET_KEY: process.env.TURNSTILE_SECRET_KEY || '',
    STRIPE_SECRET_KEY: process.env.STRIPE_SECRET_KEY || '',
    STRIPE_PRICE_STANDARD: process.env.STRIPE_PRICE_STANDARD || '',
    STRIPE_PRICE_PRO: process.env.STRIPE_PRICE_PRO || '',
    STRIPE_STANDARD_COUPON_ID: process.env.STRIPE_STANDARD_COUPON_ID || '',
    
    // WebAuthn設定
    RP_NAME: 'LenovoServer',
    RP_ID: process.env.RP_ID || 'lenovo.schale41.jp',
    RP_ORIGIN: process.env.RP_ORIGIN || 'https://lenovo.schale41.jp',
    
    // SSH設定
    SSH_PORT: parseInt(process.env.SSH_PORT) || 4104,  // 直接IPアドレス経由
    VM_TTYD_PORT: parseInt(process.env.VM_TTYD_PORT || '7681', 10),
    
    // Ubuntuベースイメージ
    BASE_IMAGE: 'ubuntu:22.04',
    
    // プラン設定
    PLANS: {
        free: { cpu: 0.5, memory: 512, storage: 1024, containers: 1, price: 0 },
        standard: { cpu: 1, memory: 2048, storage: 10240, containers: 5, price: 500 },
        pro: { cpu: 2, memory: 4096, storage: 51200, containers: -1, price: 1500 }
    },
    
    // サーバータイプ（全てUbuntuベース）
    // 全サーバータイプに共通でインストールされる基本パッケージ
    BASE_PACKAGES: ['openssh-client', 'curl', 'wget', 'git', 'vim', 'nano', 'sudo'],
    SERVER_TYPES: {
        ubuntu: {
            name: 'Ubuntu 22.04',
            description: '純粋なUbuntu環境。自由にカスタマイズ可能',
            packages: []
        },
        'ubuntu-nodejs': {
            name: 'Ubuntu + Node.js',
            description: 'Node.js 20 LTSがプリインストール',
            packages: ['nodejs', 'npm']
        },
        'ubuntu-python': {
            name: 'Ubuntu + Python',
            description: 'Python 3.11がプリインストール',
            packages: ['python3', 'python3-pip', 'python3-venv']
        },
        'ubuntu-java': {
            name: 'Ubuntu + Java',
            description: 'OpenJDK 17がプリインストール',
            packages: ['openjdk-17-jdk']
        },
        'ubuntu-go': {
            name: 'Ubuntu + Go',
            description: 'Go 1.21がプリインストール',
            packages: ['golang-go']
        },
        'ubuntu-full': {
            name: 'Ubuntu Full Stack',
            description: 'Node.js, Python, Java, Go全てインストール',
            packages: ['nodejs', 'npm', 'python3', 'python3-pip', 'openjdk-17-jdk', 'golang-go']
        }
    }
};

const isVmBackendRef = (ref) => typeof ref === 'string' && ref.startsWith('vm:');
const isDockerBackendRef = (ref) => !!ref && !isVmBackendRef(ref);
const DEFAULT_VM_DISK_TARGETS = ['vda', 'sda', 'vdb'];
const vmCpuSnapshots = new Map();
const stripe = CONFIG.STRIPE_SECRET_KEY ? new Stripe(CONFIG.STRIPE_SECRET_KEY) : null;

const runCommand = (command, args = [], timeoutMs = 2500) => new Promise((resolve) => {
    let stdout = '';
    let stderr = '';
    let settled = false;
    let timer = null;

    const finish = (result) => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        resolve({
            ...result,
            stdout: (result.stdout || '').trim(),
            stderr: (result.stderr || '').trim()
        });
    };

    let child;
    try {
        child = spawn(command, args, {
            env: {
                ...process.env,
                LIBVIRT_DEFAULT_URI: process.env.LIBVIRT_DEFAULT_URI || 'qemu:///system'
            }
        });
    } catch (error) {
        finish({ ok: false, code: null, stdout: '', stderr: error.message || String(error) });
        return;
    }

    timer = setTimeout(() => {
        child.kill('SIGKILL');
        finish({ ok: false, code: null, stdout, stderr: stderr || 'command timeout' });
    }, timeoutMs);

    child.stdout.on('data', (chunk) => { stdout += chunk.toString(); });
    child.stderr.on('data', (chunk) => { stderr += chunk.toString(); });
    child.on('error', (error) => finish({ ok: false, code: null, stdout, stderr: error.message || String(error) }));
    child.on('close', (code) => finish({ ok: code === 0, code, stdout, stderr }));
});

const parseVmPowerState = (stateRaw = '') => {
    const normalized = String(stateRaw).toLowerCase();
    if (normalized.includes('running') || normalized.includes('idle') || normalized.includes('paused')) {
        return 'running';
    }
    return 'stopped';
};

const clampPercent = (value) => {
    if (!Number.isFinite(value)) return null;
    if (value < 0) return 0;
    return Math.min(value, 100);
};

const parseVmIface = (domifListRaw = '') => {
    const lines = domifListRaw.split('\n')
        .map((line) => line.trim())
        .filter((line) => line && !line.startsWith('Interface') && !line.startsWith('-'));
    if (lines.length === 0) return null;
    const cols = lines[0].split(/\s+/);
    return cols[0] || null;
};

const parseCpuTimeSeconds = (cpuStatsRaw = '') => {
    const match = cpuStatsRaw.match(/cpu_time\s+([0-9.]+)\s+seconds/i);
    if (!match) return null;
    const value = parseFloat(match[1]);
    return Number.isFinite(value) ? value : null;
};

const getVmRuntimeStats = async (vmName, vmCpuLimit, storageLimitMb) => {
    const stats = {
        cpuUsage: null,
        memoryUsage: null,
        networkInBytes: null,
        networkOutBytes: null,
        diskUsageBytes: null,
        diskUsagePercent: null
    };

    const memStat = await runCommand('virsh', ['dommemstat', vmName], 3000);
    if (memStat.ok) {
        const available = parseInt((memStat.stdout.match(/^\s*available\s+(\d+)/mi) || [])[1], 10);
        const unused = parseInt((memStat.stdout.match(/^\s*unused\s+(\d+)/mi) || [])[1], 10);
        if (Number.isFinite(available) && available > 0 && Number.isFinite(unused) && unused >= 0) {
            const used = Math.max(available - unused, 0);
            stats.memoryUsage = clampPercent((used / available) * 100);
        }
    }

    const domifList = await runCommand('virsh', ['domiflist', vmName], 3000);
    const iface = domifList.ok ? parseVmIface(domifList.stdout) : null;
    if (iface) {
        const ifStat = await runCommand('virsh', ['domifstat', vmName, iface], 3000);
        if (ifStat.ok) {
            const rx = parseInt((ifStat.stdout.match(/\brx_bytes\s+(\d+)/i) || [])[1], 10);
            const tx = parseInt((ifStat.stdout.match(/\btx_bytes\s+(\d+)/i) || [])[1], 10);
            if (Number.isFinite(rx)) stats.networkInBytes = rx;
            if (Number.isFinite(tx)) stats.networkOutBytes = tx;
        }
    }

    for (const target of DEFAULT_VM_DISK_TARGETS) {
        const blkInfo = await runCommand('virsh', ['domblkinfo', vmName, target], 3000);
        if (!blkInfo.ok) continue;
        const allocation = parseInt((blkInfo.stdout.match(/^\s*Allocation:\s+(\d+)/mi) || [])[1], 10);
        const capacity = parseInt((blkInfo.stdout.match(/^\s*Capacity:\s+(\d+)/mi) || [])[1], 10);
        if (!Number.isFinite(allocation) || !Number.isFinite(capacity) || capacity <= 0) continue;
        stats.diskUsageBytes = allocation;
        stats.diskUsagePercent = clampPercent((allocation / capacity) * 100);
        break;
    }

    const cpuStats = await runCommand('virsh', ['cpu-stats', vmName, '--total'], 3000);
    if (cpuStats.ok) {
        const cpuTimeSec = parseCpuTimeSeconds(cpuStats.stdout);
        if (Number.isFinite(cpuTimeSec)) {
            const nowMs = Date.now();
            const prev = vmCpuSnapshots.get(vmName);
            vmCpuSnapshots.set(vmName, { cpuTimeSec, atMs: nowMs });
            if (prev && prev.atMs < nowMs) {
                const deltaCpu = cpuTimeSec - prev.cpuTimeSec;
                const deltaSec = (nowMs - prev.atMs) / 1000;
                const cpuCount = Math.max(1, Number(vmCpuLimit) || 1);
                if (deltaCpu >= 0 && deltaSec > 0) {
                    stats.cpuUsage = clampPercent((deltaCpu / (deltaSec * cpuCount)) * 100);
                }
            }
        }
    }

    if (stats.diskUsageBytes === null && Number.isFinite(storageLimitMb) && storageLimitMb > 0) {
        stats.diskUsageBytes = 0;
        stats.diskUsagePercent = 0;
    }

    return stats;
};

const getVmBackendInfo = async (vmName) => {
    if (!vmName) return null;

    const domInfo = await runCommand('virsh', ['dominfo', vmName]);
    if (!domInfo.ok) {
        return null;
    }

    const cpuMatch = domInfo.stdout.match(/^\s*CPU\(s\):\s+(\d+)/mi);
    const memoryMatch = domInfo.stdout.match(/^\s*Max memory:\s+(\d+)\s+KiB/mi);
    const stateMatch = domInfo.stdout.match(/^\s*State:\s+(.+)$/mi);

    let storageLimitMb = null;
    for (const target of DEFAULT_VM_DISK_TARGETS) {
        const blkInfo = await runCommand('virsh', ['domblkinfo', vmName, target]);
        if (!blkInfo.ok) continue;
        const capacityMatch = blkInfo.stdout.match(/^\s*Capacity:\s+(\d+)/mi);
        if (!capacityMatch) continue;
        const capacityBytes = parseInt(capacityMatch[1], 10);
        if (Number.isFinite(capacityBytes) && capacityBytes > 0) {
            storageLimitMb = Math.floor(capacityBytes / (1024 * 1024));
            break;
        }
    }

    const cpuLimit = cpuMatch ? parseInt(cpuMatch[1], 10) : null;
    const memoryKiB = memoryMatch ? parseInt(memoryMatch[1], 10) : null;
    const parsedCpuLimit = Number.isFinite(cpuLimit) && cpuLimit > 0 ? cpuLimit : null;
    const parsedMemoryLimit = Number.isFinite(memoryKiB) && memoryKiB > 0 ? Math.floor(memoryKiB / 1024) : null;
    const parsedStatus = parseVmPowerState(stateMatch ? stateMatch[1] : '');
    const runtimeStats = parsedStatus === 'running'
        ? await getVmRuntimeStats(vmName, parsedCpuLimit, storageLimitMb)
        : null;

    return {
        available: true,
        status: parsedStatus,
        cpu_limit: parsedCpuLimit,
        memory_limit: parsedMemoryLimit,
        storage_limit: storageLimitMb,
        stats: runtimeStats
    };
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const normalizeAppPath = (value, fallback = '/app') => {
    const raw = String(value || fallback).replace(/\\/g, '/').trim();
    if (!raw) return fallback;
    const base = raw.startsWith('/') ? raw : `/${raw}`;
    const normalized = path.posix.normalize(base);
    return normalized || fallback;
};

const isPathWithinApp = (targetPath) => targetPath === '/app' || targetPath.startsWith('/app/');

const parseLsOutput = (stdout) => {
    const lines = String(stdout || '')
        .split('\n')
        .filter((line) => line.trim() && !line.startsWith('total'));
    const items = [];

    for (const line of lines) {
        const cleanLine = line.replace(/[\x00-\x1F\x7F]/g, '').trim();
        if (!cleanLine) continue;
        const parts = cleanLine.split(/\s+/);
        if (parts.length < 7) continue;

        const permissions = parts[0];
        const size = parseInt(parts[4], 10) || 0;
        const modified = parts[5] || '';
        const name = parts.slice(6).join(' ');
        if (name === '.' || name === '..') continue;

        items.push({
            name,
            type: permissions.startsWith('d') ? 'directory' : 'file',
            size,
            modified
        });
    }

    return items;
};

const decodeQgaBase64 = (value) => {
    if (!value) return '';
    try {
        return Buffer.from(String(value), 'base64').toString('utf8');
    } catch {
        return '';
    }
};

const qemuAgentCommand = async (vmName, payload, timeoutMs = 5000) => {
    const command = await runCommand('virsh', ['qemu-agent-command', vmName, JSON.stringify(payload)], timeoutMs);
    if (!command.ok) {
        return { ok: false, error: command.stderr || command.stdout || 'qemu-agent command failed' };
    }

    let parsed;
    try {
        parsed = JSON.parse(command.stdout || '{}');
    } catch (error) {
        return { ok: false, error: `qemu-agent response parse error: ${error.message}` };
    }

    if (parsed?.error) {
        return { ok: false, error: parsed.error.desc || parsed.error.class || 'qemu-agent error' };
    }

    return { ok: true, result: parsed?.return || {} };
};

const vmGuestExec = async (vmName, {
    execPath,
    args = [],
    input = null,
    timeoutMs = 10000
}) => {
    const guestExecArgs = {
        path: execPath,
        arg: args,
        'capture-output': true
    };

    if (input !== null && input !== undefined) {
        const buffer = Buffer.isBuffer(input) ? input : Buffer.from(String(input), 'utf8');
        guestExecArgs['input-data'] = buffer.toString('base64');
    }

    const startResult = await qemuAgentCommand(vmName, {
        execute: 'guest-exec',
        arguments: guestExecArgs
    }, 6000);

    if (!startResult.ok) {
        return { ok: false, exitCode: null, stdout: '', stderr: startResult.error, timedOut: false };
    }

    const pid = startResult.result?.pid;
    if (!Number.isFinite(pid)) {
        return { ok: false, exitCode: null, stdout: '', stderr: 'guest-exec pid missing', timedOut: false };
    }

    const startedAt = Date.now();
    while (Date.now() - startedAt < timeoutMs) {
        await sleep(200);
        const statusResult = await qemuAgentCommand(vmName, {
            execute: 'guest-exec-status',
            arguments: { pid }
        }, 6000);

        if (!statusResult.ok) {
            return { ok: false, exitCode: null, stdout: '', stderr: statusResult.error, timedOut: false };
        }

        const status = statusResult.result || {};
        if (!status.exited) continue;

        const stdout = decodeQgaBase64(status['out-data']);
        const stderr = decodeQgaBase64(status['err-data']);
        const exitCode = Number.isFinite(status.exitcode) ? status.exitcode : 1;
        return {
            ok: exitCode === 0,
            exitCode,
            stdout,
            stderr,
            timedOut: false
        };
    }

    return { ok: false, exitCode: null, stdout: '', stderr: 'guest-exec timeout', timedOut: true };
};

const shellSingleQuote = (value) => `'${String(value).replace(/'/g, `'\\''`)}'`;

const vmExecBash = async (vmName, script, options = {}) => vmGuestExec(vmName, {
    execPath: '/bin/bash',
    args: ['-lc', script],
    input: options.input ?? null,
    timeoutMs: options.timeoutMs ?? 10000
});

const LINUX_USER_PATTERN = /^[a-z_][a-z0-9_-]{0,31}$/i;
const isValidLinuxUserName = (value) => LINUX_USER_PATTERN.test(String(value || '').trim());

const buildVmUserScript = (userName, script) => {
    const normalizedUser = String(userName || '').trim();
    const baseScript = String(script || '');
    if (!normalizedUser || normalizedUser === 'root') {
        return baseScript;
    }

    const quotedScript = shellSingleQuote(baseScript);
    return `
if command -v runuser >/dev/null 2>&1; then
    runuser -u ${shellSingleQuote(normalizedUser)} -- /bin/bash -lc ${quotedScript}
elif command -v su >/dev/null 2>&1; then
    su -s /bin/bash ${shellSingleQuote(normalizedUser)} -c ${quotedScript}
else
    echo "runuser/su が利用できません" >&2
    exit 127
fi
`.trim();
};

const vmExecBashAsUser = async (vmName, userName, script, options = {}) => vmExecBash(
    vmName,
    buildVmUserScript(userName, script),
    options
);

const vmLinuxUserExists = async (vmName, userName) => {
    const normalizedUser = String(userName || '').trim();
    if (!isValidLinuxUserName(normalizedUser)) return false;
    const result = await vmExecBash(vmName, `getent passwd ${shellSingleQuote(normalizedUser)} >/dev/null 2>&1`);
    return result.ok;
};

const resolveVmUserHomeDirectory = async (vmName, userName = 'root') => {
    const normalizedUser = String(userName || 'root').trim() || 'root';
    const fallbackHome = normalizedUser === 'root' ? '/root' : `/home/${normalizedUser}`;
    if (normalizedUser !== 'root' && !isValidLinuxUserName(normalizedUser)) {
        return fallbackHome;
    }

    const result = await vmExecBash(
        vmName,
        `getent passwd ${shellSingleQuote(normalizedUser)} | cut -d: -f6`,
        { timeoutMs: 10000 }
    );
    if (!result.ok) return fallbackHome;

    const homeDir = (result.stdout || '').trim().split('\n').filter(Boolean).pop() || '';
    if (!homeDir.startsWith('/')) return fallbackHome;
    return homeDir;
};

const normalizeVmTtydPort = (value, fallback = 7681) => {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed)) return fallback;
    if (parsed < 1024) return 1024;
    if (parsed > 65535) return 65535;
    return parsed;
};

const sanitizeTtydAuthUser = (value, fallback = 'admin') => {
    const cleaned = String(value || '').trim().replace(/[:\s]/g, '');
    return cleaned || fallback;
};

const generateTunnelPassword = () => crypto.randomBytes(10).toString('hex');

const ensureVmTtydTryCloudflareTunnel = async (vmName, { port, authUser, authPass }) => {
    const safePort = normalizeVmTtydPort(port, normalizeVmTtydPort(CONFIG.VM_TTYD_PORT, 7681));
    const safeUser = sanitizeTtydAuthUser(authUser, 'admin');
    const safePass = String(authPass || '').trim() || generateTunnelPassword();

    const script = `
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
PORT=${safePort}
TTYD_USER=${shellSingleQuote(safeUser)}
TTYD_PASS=${shellSingleQuote(safePass)}

cat >/usr/local/bin/install-ttyd-cloudflared.sh <<'EOF'
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
EOF
chmod +x /usr/local/bin/install-ttyd-cloudflared.sh

cat >/usr/local/bin/start-ttyd-trycloudflare.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

PORT="\${TTYD_PORT:-7681}"
AUTH_USER="\${TTYD_USER:-admin}"
AUTH_PASS="\${TTYD_PASS:-}"

if [ -z "$AUTH_PASS" ]; then
  echo "TTYD_PASS is required" >&2
  exit 1
fi

mkdir -p /var/log/lenovo

port_in_use() {
  local p="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltn "( sport = :\${p} )" 2>/dev/null | awk 'NR>1 {print; exit}' | grep -q .
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

if pgrep -f "ttyd .*--port \${PORT}" >/dev/null 2>&1; then
  pkill -f "ttyd .*--port \${PORT}" >/dev/null 2>&1 || true
  sleep 1
fi

ACTUAL_PORT="\${PORT}"
if port_in_use "\${ACTUAL_PORT}"; then
  ALT_PORT="$(pick_available_port "\${PORT}" || true)"
  if [ -z "\${ALT_PORT}" ]; then
    echo "no free port found near \${PORT}" >&2
    exit 1
  fi
  ACTUAL_PORT="\${ALT_PORT}"
fi

nohup ttyd --port "\${ACTUAL_PORT}" --interface 127.0.0.1 --writable -c "\${AUTH_USER}:\${AUTH_PASS}" /bin/login -f "\${AUTH_USER}" >/var/log/lenovo/ttyd.log 2>&1 &
sleep 1

if ! pgrep -f "ttyd .*--port \${ACTUAL_PORT}" >/dev/null 2>&1; then
  echo "failed to start ttyd" >&2
  tail -n 60 /var/log/lenovo/ttyd.log >&2 || true
  exit 1
fi

pkill -f "cloudflared tunnel --url http://127.0.0.1:\${ACTUAL_PORT}" >/dev/null 2>&1 || true
: > /var/log/lenovo/cloudflared-ttyd.log
nohup cloudflared tunnel --url "http://127.0.0.1:\${ACTUAL_PORT}" --no-autoupdate >/var/log/lenovo/cloudflared-ttyd.log 2>&1 &

for i in $(seq 1 30); do
  URL="$(grep -Eo 'https://[-a-zA-Z0-9]+\\.trycloudflare\\.com' /var/log/lenovo/cloudflared-ttyd.log | grep -Ev '^https://api\\.trycloudflare\\.com$' | tail -n 1 || true)"
  if [ -n "$URL" ]; then
    echo "TTYD_PORT=\${ACTUAL_PORT}"
    echo "TRYCLOUDFLARE_URL=$URL"
    exit 0
  fi
  sleep 1
done

echo "failed to create trycloudflare URL" >&2
tail -n 80 /var/log/lenovo/cloudflared-ttyd.log >&2 || true
exit 1
EOF
chmod +x /usr/local/bin/start-ttyd-trycloudflare.sh

/usr/local/bin/install-ttyd-cloudflared.sh
OUT="$(TTYD_PORT="$PORT" TTYD_USER="$TTYD_USER" TTYD_PASS="$TTYD_PASS" /usr/local/bin/start-ttyd-trycloudflare.sh)"
echo "$OUT"
`.trim();

    const result = await vmExecBash(vmName, script, { timeoutMs: 240000 });
    const combined = `${result.stdout || ''}\n${result.stderr || ''}`;
    const urlLineMatch = combined.match(/TRYCLOUDFLARE_URL=(https:\/\/[-a-zA-Z0-9]+\.trycloudflare\.com)/);
    const rawUrlMatches = Array.from(
        combined.matchAll(/https:\/\/[-a-zA-Z0-9]+\.trycloudflare\.com/g),
        (match) => match[0]
    ).filter((url) => !/^https:\/\/api\.trycloudflare\.com$/i.test(url));
    const portLineMatch = combined.match(/TTYD_PORT=(\d{2,5})/);
    const detectedUrl = urlLineMatch ? urlLineMatch[1] : (rawUrlMatches.length ? rawUrlMatches[rawUrlMatches.length - 1] : null);
    const detectedPort = portLineMatch ? normalizeVmTtydPort(portLineMatch[1], safePort) : safePort;
    if (!result.ok || !detectedUrl) {
        return {
            ok: false,
            error: (result.stderr || result.stdout || 'ttyd tunnel setup failed').trim()
        };
    }

    return {
        ok: true,
        url: detectedUrl,
        port: detectedPort,
        username: safeUser,
        password: safePass
    };
};

const resolvePlanLimits = (planId) => {
    const plan = CONFIG.PLANS[planId] || CONFIG.PLANS.free;
    return {
        planId: CONFIG.PLANS[planId] ? planId : 'free',
        maxContainers: plan.containers,
        maxCpu: plan.cpu,
        maxMemory: plan.memory,
        maxStorage: plan.storage
    };
};

const getEffectiveUserLimits = (user) => {
    const planLimits = resolvePlanLimits(user?.plan || 'free');
    return {
        maxContainers: Number.isFinite(user?.max_containers) ? user.max_containers : planLimits.maxContainers,
        maxCpu: Number.isFinite(user?.max_cpu) ? user.max_cpu : planLimits.maxCpu,
        maxMemory: Number.isFinite(user?.max_memory) ? user.max_memory : planLimits.maxMemory,
        maxStorage: Number.isFinite(user?.max_storage) ? user.max_storage : planLimits.maxStorage
    };
};

const syncVmDomainLimits = async (vmName, limits) => {
    if (!vmName) {
        return { ok: false, errors: ['vm name missing'] };
    }

    const targetCpu = Math.max(1, Math.round(Number(limits?.maxCpu) || 1));
    const targetMemoryMb = Math.max(256, Math.round(Number(limits?.maxMemory) || 512));
    const targetMemoryKiB = targetMemoryMb * 1024;
    const errors = [];

    const pushError = (label, result) => {
        errors.push(`${label}: ${result.stderr || result.stdout || 'unknown error'}`);
    };

    const domInfo = await runCommand('virsh', ['dominfo', vmName], 4000);
    const stateMatch = domInfo.ok ? domInfo.stdout.match(/^\s*State:\s+(.+)$/mi) : null;
    const currentCpuMatch = domInfo.ok ? domInfo.stdout.match(/^\s*CPU\(s\):\s+(\d+)/mi) : null;
    const currentCpu = currentCpuMatch ? parseInt(currentCpuMatch[1], 10) : null;
    const isRunning = domInfo.ok
        ? parseVmPowerState(stateMatch ? stateMatch[1] : '') === 'running'
        : false;

    const setVcpuConfig = await runCommand('virsh', ['setvcpus', vmName, String(targetCpu), '--config'], 8000);
    if (!setVcpuConfig.ok) pushError('setvcpus --config', setVcpuConfig);
    if (isRunning) {
        const reduceWhileRunning = Number.isFinite(currentCpu) && targetCpu < currentCpu;
        if (!reduceWhileRunning) {
            const setVcpuLive = await runCommand('virsh', ['setvcpus', vmName, String(targetCpu), '--live'], 8000);
            if (!setVcpuLive.ok) pushError('setvcpus --live', setVcpuLive);
        }
    }

    const setMaxMemConfig = await runCommand('virsh', ['setmaxmem', vmName, `${targetMemoryKiB}K`, '--config'], 8000);
    if (!setMaxMemConfig.ok) pushError('setmaxmem --config', setMaxMemConfig);
    const setMemConfig = await runCommand('virsh', ['setmem', vmName, `${targetMemoryKiB}K`, '--config'], 8000);
    if (!setMemConfig.ok) pushError('setmem --config', setMemConfig);
    if (isRunning) {
        const setMemLive = await runCommand('virsh', ['setmem', vmName, `${targetMemoryKiB}K`, '--live'], 8000);
        if (!setMemLive.ok) pushError('setmem --live', setMemLive);
    }

    return {
        ok: errors.length === 0,
        errors,
        applied: {
            cpu: targetCpu,
            memory: targetMemoryMb
        }
    };
};

const syncContainersToPlanLimits = async (userId, limits) => {
    const containers = db.prepare('SELECT * FROM containers WHERE user_id = ?').all(userId);
    for (const container of containers) {
        db.prepare(`
            UPDATE containers
            SET cpu_limit = ?, memory_limit = ?, storage_limit = ?, plan = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        `).run(limits.maxCpu, limits.maxMemory, limits.maxStorage, limits.planId, container.id);

        if (isVmBackendRef(container.docker_id)) {
            const vmName = container.docker_id.replace(/^vm:/, '');
            const vmSync = await syncVmDomainLimits(vmName, limits);
            if (!vmSync.ok) {
                console.error(`VM limit update error (${container.id}): ${vmSync.errors.join(' | ')}`);
            }
            continue;
        }

        if (!dockerAvailable || !isDockerBackendRef(container.docker_id)) continue;
        try {
            const dockerContainer = docker.getContainer(container.docker_id);
            await dockerContainer.update({
                NanoCpus: Math.round(limits.maxCpu * 1e9),
                Memory: Math.round(limits.maxMemory * 1024 * 1024),
                MemorySwap: Math.round(limits.maxMemory * 1024 * 1024 * 2)
            });
        } catch (error) {
            console.error(`Container limit update error (${container.id}):`, error.message);
        }
    }
};

const applyUserPlanAndLimits = async ({ userId, planId, actorUserId = null, ip = null, activity = 'plan_update' }) => {
    const limits = resolvePlanLimits(planId);
    db.prepare(`
        UPDATE users
        SET plan = ?, max_containers = ?, max_cpu = ?, max_memory = ?, max_storage = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    `).run(limits.planId, limits.maxContainers, limits.maxCpu, limits.maxMemory, limits.maxStorage, userId);

    await syncContainersToPlanLimits(userId, limits);

    if (actorUserId) {
        logActivity(actorUserId, null, activity, { target_user: userId, plan: limits.planId, limits }, ip);
    }

    return limits;
};

const syncAllContainersToUserLimits = async () => {
    const users = db.prepare(`
        SELECT id, plan, max_containers, max_cpu, max_memory, max_storage
        FROM users
    `).all();

    for (const user of users) {
        const limits = getEffectiveUserLimits(user);
        await syncContainersToPlanLimits(user.id, {
            planId: user.plan || 'free',
            ...limits
        });
    }
};

const formatVmPrompt = (sshUser, cwd) => `${sshUser || 'ubuntu'}@vm:${cwd}$ `;

const resolveVmCwd = async (vmName, currentCwd, targetRaw, userName = 'root', homeDir = '/root') => {
    const target = String(targetRaw || '').trim();
    if (!target || target === '~') return homeDir;
    const script = `cd ${shellSingleQuote(currentCwd)} 2>/dev/null || cd ${shellSingleQuote(homeDir)} 2>/dev/null || cd /; cd ${shellSingleQuote(target)} 2>/dev/null && pwd`;
    const result = await vmExecBashAsUser(vmName, userName, script, { timeoutMs: 10000 });
    if (!result.ok) return { ok: false, cwd: currentCwd, error: result.stderr || 'cd failed' };
    const cwd = (result.stdout || '').trim().split('\n').filter(Boolean).pop() || currentCwd;
    return { ok: true, cwd };
};

const normalizeAbsolutePosixPath = (value, fallback = '/app') => {
    const raw = String(value ?? '').replace(/\\/g, '/').trim();
    if (!raw) return fallback;
    const base = raw.startsWith('/') ? raw : `/${raw}`;
    return path.posix.normalize(base) || fallback;
};

const resolveVmFileContext = async (container) => {
    if (!isVmBackendRef(container?.docker_id)) {
        return { ok: false, status: 400, error: 'VMバックエンドではありません' };
    }

    const vmName = container.docker_id.replace(/^vm:/, '');
    const vmInfo = await getVmBackendInfo(vmName);
    if (!vmInfo?.available) {
        return { ok: false, status: 500, error: 'VM状態の取得に失敗しました' };
    }
    if (vmInfo.status !== 'running') {
        return { ok: false, status: 400, error: 'VMが停止中です。先に起動してください。' };
    }

    const preferredUser = String(container.ssh_user || '').trim();
    let vmExecUser = preferredUser;
    if (!isValidLinuxUserName(vmExecUser) || !(await vmLinuxUserExists(vmName, vmExecUser))) {
        vmExecUser = 'root';
    }

    const vmHomeDir = await resolveVmUserHomeDirectory(vmName, vmExecUser);
    return { ok: true, vmName, vmExecUser, vmHomeDir };
};

const resolveVmFilePath = (requestedPath, vmHomeDir, { allowHome = true } = {}) => {
    const normalized = normalizeAbsolutePosixPath(requestedPath || '/app', '/app');
    let resolved = normalized;

    // Web UIの/app基準をVMではホーム配下にマップ
    if (normalized === '/' || normalized === '/app') {
        resolved = vmHomeDir;
    } else if (normalized.startsWith('/app/')) {
        resolved = path.posix.join(vmHomeDir, normalized.slice('/app/'.length));
    }

    const withinHome = resolved === vmHomeDir || resolved.startsWith(`${vmHomeDir}/`);
    if (!withinHome) {
        return { ok: false, error: 'アクセスが拒否されました (ホームディレクトリ以下のみ)' };
    }
    if (!allowHome && resolved === vmHomeDir) {
        return { ok: false, error: 'アクセスが拒否されました (ホーム直下は対象外)' };
    }
    return { ok: true, path: resolved };
};

// ディレクトリ作成
[CONFIG.DATA_DIR, CONFIG.CONTAINERS_DIR, CONFIG.BACKUPS_DIR].forEach(dir => {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

// ============ データベース初期化 ============
const db = new Database(CONFIG.DB_PATH);
db.pragma('journal_mode = WAL');

db.exec(`
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        name TEXT NOT NULL,
        plan TEXT DEFAULT 'free',
        is_admin INTEGER DEFAULT 0,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS containers (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        name TEXT NOT NULL,
        type TEXT NOT NULL,
        docker_id TEXT,
        status TEXT DEFAULT 'stopped',
        plan TEXT DEFAULT 'free',
        cpu_limit REAL,
        memory_limit INTEGER,
        storage_limit INTEGER,
        auto_restart INTEGER DEFAULT 0,
        env_vars TEXT DEFAULT '{}',
        ssh_user TEXT,
        ssh_password TEXT,
        startup_command TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS container_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        container_id TEXT NOT NULL,
        cpu_usage REAL,
        memory_usage REAL,
        network_in INTEGER,
        network_out INTEGER,
        recorded_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (container_id) REFERENCES containers(id)
    );

    CREATE TABLE IF NOT EXISTS activity_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT,
        container_id TEXT,
        action TEXT NOT NULL,
        details TEXT,
        ip_address TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_containers_user ON containers(user_id);
    CREATE INDEX IF NOT EXISTS idx_stats_container ON container_stats(container_id);
    CREATE INDEX IF NOT EXISTS idx_logs_user ON activity_logs(user_id);
`);

// カラム追加（既存DBの場合）
try {
    db.exec(`ALTER TABLE containers ADD COLUMN ssh_password TEXT`);
} catch (e) {}
try {
    db.exec(`ALTER TABLE containers ADD COLUMN startup_command TEXT`);
} catch (e) {}

// Discord認証用カラム追加
try {
    db.exec(`ALTER TABLE users ADD COLUMN discord_id TEXT`);
} catch (e) {}
try {
    db.exec(`ALTER TABLE users ADD COLUMN avatar TEXT`);
} catch (e) {}
try {
    db.exec(`ALTER TABLE users ADD COLUMN max_containers INTEGER DEFAULT 1`);
} catch (e) {}
try {
    db.exec(`ALTER TABLE users ADD COLUMN max_cpu REAL DEFAULT 0.5`);
} catch (e) {}
try {
    db.exec(`ALTER TABLE users ADD COLUMN max_memory INTEGER DEFAULT 512`);
} catch (e) {}
try {
    db.exec(`ALTER TABLE users ADD COLUMN max_storage INTEGER DEFAULT 1024`);
} catch (e) {}
try {
    db.exec(`ALTER TABLE users ADD COLUMN stripe_customer_id TEXT`);
} catch (e) {}
try {
    db.exec(`ALTER TABLE users ADD COLUMN stripe_subscription_id TEXT`);
} catch (e) {}

// インデックス追加
try {
    db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_users_discord ON users(discord_id)`);
} catch (e) {}

// SSH公開鍵テーブル
try {
    db.exec(`
        CREATE TABLE IF NOT EXISTS ssh_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            name TEXT NOT NULL,
            public_key TEXT NOT NULL,
            fingerprint TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id),
            UNIQUE(user_id, fingerprint)
        )
    `);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_ssh_keys_user ON ssh_keys(user_id)`);
} catch (e) {}

// パスキー（WebAuthn）テーブル
try {
    db.exec(`
        CREATE TABLE IF NOT EXISTS passkeys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            credential_id TEXT NOT NULL UNIQUE,
            public_key TEXT NOT NULL,
            counter INTEGER DEFAULT 0,
            name TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_used TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    `);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_passkeys_user ON passkeys(user_id)`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_passkeys_credential ON passkeys(credential_id)`);
} catch (e) {}

// TOTP二段階認証テーブル
try {
    db.exec(`
        CREATE TABLE IF NOT EXISTS totp_secrets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL UNIQUE,
            secret TEXT NOT NULL,
            enabled INTEGER DEFAULT 0,
            backup_codes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    `);
} catch (e) {}

// 2FA有効フラグをusersテーブルに追加
try {
    db.exec(`ALTER TABLE users ADD COLUMN two_factor_enabled INTEGER DEFAULT 0`);
} catch (e) {}
try {
    db.exec(`ALTER TABLE users ADD COLUMN passkey_enabled INTEGER DEFAULT 0`);
} catch (e) {}

// ============ Express & Docker 初期化 ============
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

let docker;
let dockerAvailable = false;
try {
    docker = new Docker({ socketPath: '/var/run/docker.sock' });
    await docker.ping();
    dockerAvailable = true;
    console.log('✅ Docker connected');
    
    // Ubuntuイメージをプル
    console.log('📦 Pulling Ubuntu base image...');
    try {
        await new Promise((resolve, reject) => {
            docker.pull(CONFIG.BASE_IMAGE, (err, stream) => {
                if (err) return reject(err);
                docker.modem.followProgress(stream, (err) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
        });
        console.log('✅ Ubuntu image ready');
    } catch (e) {
        console.log('⚠️ Could not pull Ubuntu image:', e.message);
    }
} catch (e) {
    console.log('⚠️ Docker not available, running in simulation mode');
}

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../frontend/public')));

// 管理UIの互換パス
app.get(['/manage', '/manage/'], (req, res) => {
    res.sendFile(path.join(__dirname, '../frontend/public/admin.html'));
});

// ファイルアップロード設定
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        const containerDir = path.join(CONFIG.CONTAINERS_DIR, req.params.id);
        if (!fs.existsSync(containerDir)) fs.mkdirSync(containerDir, { recursive: true });
        
        // サブディレクトリ対応
        const uploadPath = req.query.path ? path.join(containerDir, req.query.path) : containerDir;
        if (!fs.existsSync(uploadPath)) fs.mkdirSync(uploadPath, { recursive: true });
        
        cb(null, uploadPath);
    },
    filename: (req, file, cb) => {
        cb(null, file.originalname);
    }
});
const upload = multer({ storage, limits: { fileSize: CONFIG.MAX_FILE_SIZE } });

// ============ ヘルパー関数 ============
const logActivity = (userId, containerId, action, details, ip) => {
    db.prepare(`
        INSERT INTO activity_logs (user_id, container_id, action, details, ip_address)
        VALUES (?, ?, ?, ?, ?)
    `).run(userId, containerId, action, JSON.stringify(details), ip);
};

const generatePassword = () => {
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let password = '';
    for (let i = 0; i < 16; i++) {
        password += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return password;
};

// SSHユーザー名を生成（コンテナIDからsrv-プレフィックス付き）
const generateSSHUser = (containerId) => {
    // コンテナID: server-abc12345-xyz → srv-abc12345
    const shortId = containerId.replace('server-', '').split('-')[0];
    return `srv-${shortId}`;
};

const getContainerStats = async (dockerId, containerId = null) => {
    if (!dockerAvailable || !dockerId) {
        return {
            cpuUsage: Math.random() * 30 + 5,
            memoryUsage: Math.random() * 40 + 10,
            networkIn: Math.floor(Math.random() * 1000000000),
            networkOut: Math.floor(Math.random() * 500000000),
            diskUsage: 0,
            diskUsageBytes: 0
        };
    }
    
    try {
        const container = docker.getContainer(dockerId);
        const stats = await container.stats({ stream: false });
        
        const cpuDelta = stats.cpu_stats.cpu_usage.total_usage - stats.precpu_stats.cpu_usage.total_usage;
        const systemDelta = stats.cpu_stats.system_cpu_usage - stats.precpu_stats.system_cpu_usage;
        const cpuUsage = systemDelta > 0 ? (cpuDelta / systemDelta) * 100 : 0;
        
        const memoryUsage = stats.memory_stats.limit > 0 
            ? (stats.memory_stats.usage / stats.memory_stats.limit) * 100 
            : 0;
        
        // ディスク使用量を取得（/app ディレクトリ）
        let diskUsageBytes = 0;
        try {
            const exec = await container.exec({
                Cmd: ['du', '-sb', '/app'],
                AttachStdout: true,
                AttachStderr: true
            });
            const stream = await exec.start();
            const output = await new Promise((resolve) => {
                let data = '';
                stream.on('data', (chunk) => { data += chunk.toString(); });
                stream.on('end', () => resolve(data));
            });
            // "12345\t/app" 形式からバイト数を抽出
            const match = output.replace(/[\x00-\x1F]/g, '').match(/^(\d+)/);
            if (match) {
                diskUsageBytes = parseInt(match[1]) || 0;
            }
        } catch (e) {
            // du コマンドが失敗しても続行
        }
        
        return {
            cpuUsage: cpuUsage.toFixed(2),
            memoryUsage: memoryUsage.toFixed(2),
            networkIn: stats.networks?.eth0?.rx_bytes || 0,
            networkOut: stats.networks?.eth0?.tx_bytes || 0,
            diskUsageBytes: diskUsageBytes
        };
    } catch (e) {
        return { cpuUsage: 0, memoryUsage: 0, networkIn: 0, networkOut: 0, diskUsageBytes: 0 };
    }
};

const formatBytes = (bytes) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

// Dockerコンテナ内でコマンド実行
const execInContainer = async (dockerId, command) => {
    if (!dockerAvailable || !dockerId) return null;
    
    try {
        const container = docker.getContainer(dockerId);
        const exec = await container.exec({
            Cmd: ['bash', '-c', command],
            AttachStdout: true,
            AttachStderr: true
        });
        
        const stream = await exec.start();
        return new Promise((resolve) => {
            let output = '';
            stream.on('data', (data) => { output += data.toString(); });
            stream.on('end', () => { resolve(output); });
        });
    } catch (e) {
        console.error('Exec error:', e);
        return null;
    }
};

// ============ ミドルウェア ============
const authMiddleware = (req, res, next) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return res.status(401).json({ error: '認証が必要です' });
    
    try {
        const decoded = jwt.verify(token, CONFIG.JWT_SECRET);
        const user = db.prepare('SELECT * FROM users WHERE id = ?').get(decoded.userId);
        if (!user) return res.status(401).json({ error: 'ユーザーが見つかりません' });
        req.user = user;
        next();
    } catch (e) {
        return res.status(401).json({ error: 'トークンが無効です' });
    }
};

const adminMiddleware = (req, res, next) => {
    if (!req.user.is_admin) {
        return res.status(403).json({ error: '管理者権限が必要です' });
    }
    next();
};

// ============ Cloudflare Turnstile ============

// Turnstile検証関数
const verifyTurnstile = async (token, ip) => {
    // Turnstileが設定されていない場合はスキップ
    if (!CONFIG.TURNSTILE_SECRET_KEY) {
        return { success: true, skipped: true };
    }
    
    try {
        const response = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
                secret: CONFIG.TURNSTILE_SECRET_KEY,
                response: token,
                remoteip: ip
            })
        });
        
        const data = await response.json();
        return data;
    } catch (e) {
        console.error('Turnstile verification error:', e);
        return { success: false, error: 'verification_failed' };
    }
};

// Turnstile設定を取得
app.get('/api/turnstile/config', (req, res) => {
    res.json({
        enabled: !!CONFIG.TURNSTILE_SITE_KEY,
        siteKey: CONFIG.TURNSTILE_SITE_KEY || null
    });
});

// Turnstile検証API（認証開始前に呼び出される）
app.post('/api/turnstile/verify', async (req, res) => {
    const { token } = req.body;
    
    if (!CONFIG.TURNSTILE_SECRET_KEY) {
        // Turnstileが設定されていない場合は成功として処理
        return res.json({ success: true, skipped: true });
    }
    
    if (!token) {
        return res.status(400).json({ success: false, error: 'トークンが必要です' });
    }
    
    const result = await verifyTurnstile(token, req.ip);
    
    if (result.success) {
        // 検証成功時、一時的なセッションIDを発行
        const sessionId = uuidv4();
        // セッションを5分間有効に（メモリに保存）
        turnstileSessions.set(sessionId, {
            verified: true,
            ip: req.ip,
            expires: Date.now() + 5 * 60 * 1000
        });
        
        res.json({ success: true, sessionId });
    } else {
        res.status(400).json({ success: false, error: '認証に失敗しました' });
    }
});

// Turnstileセッション保存用（メモリ）
const turnstileSessions = new Map();

// 期限切れセッションの定期クリーンアップ
setInterval(() => {
    const now = Date.now();
    for (const [key, value] of turnstileSessions) {
        if (value.expires < now) {
            turnstileSessions.delete(key);
        }
    }
}, 60000); // 1分ごと

// ============ Discord OAuth認証API ============

// Discord認証開始
app.get('/api/auth/discord', (req, res) => {
    if (!CONFIG.DISCORD_CLIENT_ID) {
        return res.status(500).json({ error: 'Discord認証が設定されていません' });
    }
    
    // Turnstileセッション検証（設定されている場合）
    const sessionId = req.query.session;
    if (CONFIG.TURNSTILE_SECRET_KEY) {
        if (!sessionId) {
            return res.redirect('/auth.html?error=turnstile_required');
        }
        
        const session = turnstileSessions.get(sessionId);
        if (!session || session.expires < Date.now()) {
            turnstileSessions.delete(sessionId);
            return res.redirect('/auth.html?error=turnstile_expired');
        }
        
        // セッションを使用済みとしてマーク（1回限り）
        turnstileSessions.delete(sessionId);
    }
    
    // リダイレクト先をstateパラメータにエンコード
    const redirectUrl = req.query.redirect || '';
    const stateData = Buffer.from(JSON.stringify({ redirect: redirectUrl })).toString('base64');
    
    const params = new URLSearchParams({
        client_id: CONFIG.DISCORD_CLIENT_ID,
        redirect_uri: CONFIG.DISCORD_REDIRECT_URI,
        response_type: 'code',
        scope: 'identify email',
        state: stateData
    });
    
    res.redirect(`https://discord.com/api/oauth2/authorize?${params}`);
});

// Discord認証コールバック
app.get('/api/auth/discord/callback', async (req, res) => {
    try {
        const { code, state } = req.query;
        
        // stateからリダイレクト先を復元
        let redirectUrl = '';
        if (state) {
            try {
                const stateData = JSON.parse(Buffer.from(state, 'base64').toString());
                redirectUrl = stateData.redirect || '';
            } catch (e) {
                console.log('State parse error:', e);
            }
        }
        
        if (!code) {
            return res.redirect('/auth.html?error=no_code');
        }
        
        // アクセストークン取得
        const tokenResponse = await fetch('https://discord.com/api/oauth2/token', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
                client_id: CONFIG.DISCORD_CLIENT_ID,
                client_secret: CONFIG.DISCORD_CLIENT_SECRET,
                grant_type: 'authorization_code',
                code,
                redirect_uri: CONFIG.DISCORD_REDIRECT_URI
            })
        });
        
        const tokenData = await tokenResponse.json();
        
        if (!tokenData.access_token) {
            console.error('Discord token error:', tokenData);
            return res.redirect('/auth.html?error=token_failed');
        }
        
        // ユーザー情報取得
        const userResponse = await fetch('https://discord.com/api/users/@me', {
            headers: { Authorization: `Bearer ${tokenData.access_token}` }
        });
        
        const discordUser = await userResponse.json();
        
        if (!discordUser.id) {
            return res.redirect('/auth.html?error=user_failed');
        }
        
        // 既存ユーザー確認または新規作成
        let user = db.prepare('SELECT * FROM users WHERE discord_id = ?').get(discordUser.id);
        
        if (!user) {
            // 新規ユーザー作成
            const userId = `user-${uuidv4()}`;
            const email = discordUser.email || `${discordUser.id}@discord.user`;
            const name = discordUser.global_name || discordUser.username;
            const avatar = discordUser.avatar 
                ? `https://cdn.discordapp.com/avatars/${discordUser.id}/${discordUser.avatar}.png`
                : null;
            
            db.prepare(`
                INSERT INTO users (id, discord_id, email, password, name, avatar, plan, max_containers, max_cpu, max_memory, max_storage)
                VALUES (?, ?, ?, ?, ?, ?, 'free', 1, 0.5, 512, 1024)
            `).run(userId, discordUser.id, email, '', name, avatar);
            
            user = db.prepare('SELECT * FROM users WHERE id = ?').get(userId);
            
            logActivity(userId, null, 'register', { discord_id: discordUser.id }, req.ip);
        } else {
            // 既存ユーザー: アバター更新
            const avatar = discordUser.avatar 
                ? `https://cdn.discordapp.com/avatars/${discordUser.id}/${discordUser.avatar}.png`
                : null;
            db.prepare('UPDATE users SET avatar = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?')
                .run(avatar, user.id);
        }
        
        // 2FA/パスキーが有効かチェック
        if (user.two_factor_enabled || user.passkey_enabled) {
            // 2FA認証が必要 - 一時トークンを発行
            const pendingToken = jwt.sign(
                { userId: user.id, discordId: discordUser.id, pending2fa: true },
                CONFIG.JWT_SECRET,
                { expiresIn: '5m' }
            );
            
            // 2FA認証ページへリダイレクト
            const authType = user.passkey_enabled ? 'passkey' : 'totp';
            res.redirect(`/auth.html?require2fa=${authType}&pending=${pendingToken}&redirect=${encodeURIComponent(redirectUrl || '')}`);
            return;
        }
        
        // JWTトークン発行（90日間有効）
        const token = jwt.sign({ userId: user.id, discordId: discordUser.id }, CONFIG.JWT_SECRET, { expiresIn: '90d' });
        
        logActivity(user.id, null, 'login', { discord_id: discordUser.id }, req.ip);
        
        // リダイレクト先を決定
        if (redirectUrl && redirectUrl.startsWith('https://')) {
            // 外部リダイレクト（manage.schale41.jpなど）
            const separator = redirectUrl.includes('?') ? '&' : '?';
            res.redirect(`${redirectUrl}${separator}token=${token}`);
        } else {
            // デフォルトはダッシュボード
            res.redirect(`/dashboard.html?token=${token}`);
        }
    } catch (e) {
        console.error('Discord callback error:', e);
        res.redirect('/auth.html?error=server_error');
    }
});

// ユーザー情報取得
app.get('/api/auth/me', authMiddleware, (req, res) => {
    res.json({
        id: req.user.id,
        discord_id: req.user.discord_id,
        email: req.user.email,
        name: req.user.name,
        avatar: req.user.avatar,
        plan: req.user.plan,
        is_admin: req.user.is_admin,
        max_containers: req.user.max_containers || 1,
        max_cpu: req.user.max_cpu || 0.5,
        max_memory: req.user.max_memory || 512,
        max_storage: req.user.max_storage || 1024,
        stripe_customer_id: req.user.stripe_customer_id || null,
        stripe_subscription_id: req.user.stripe_subscription_id || null,
        two_factor_enabled: !!req.user.two_factor_enabled,
        passkey_enabled: !!req.user.passkey_enabled,
        created_at: req.user.created_at
    });
});

// ログアウト（クライアント側でトークン削除）
app.post('/api/auth/logout', authMiddleware, (req, res) => {
    logActivity(req.user.id, null, 'logout', {}, req.ip);
    res.json({ success: true });
});

// トークン更新（期限切れ前に新しいトークンを発行）
app.post('/api/auth/refresh', authMiddleware, (req, res) => {
    try {
        // 新しいトークンを発行（90日間有効）
        const newToken = jwt.sign(
            { userId: req.user.id, discordId: req.user.discord_id }, 
            CONFIG.JWT_SECRET, 
            { expiresIn: '90d' }
        );
        res.json({ success: true, token: newToken });
    } catch (e) {
        console.error('Token refresh error:', e);
        res.status(500).json({ error: 'トークンの更新に失敗しました' });
    }
});

// ============ パスキー（WebAuthn）API ============

// WebAuthnチャレンジ保存用（メモリ）
const webauthnChallenges = new Map();

// チャレンジの定期クリーンアップ
setInterval(() => {
    const now = Date.now();
    for (const [key, value] of webauthnChallenges) {
        if (value.expires < now) {
            webauthnChallenges.delete(key);
        }
    }
}, 60000);

// パスキー単独ログインルート登録
registerPasskeyLoginRoutes(app, db, jwt, CONFIG, webauthnChallenges, generateAuthenticationOptions, verifyAuthenticationResponse, uuidv4, logActivity);

// パスキー一覧取得
app.get('/api/passkeys', authMiddleware, (req, res) => {
    try {
        const passkeys = db.prepare('SELECT id, name, created_at, last_used FROM passkeys WHERE user_id = ?')
            .all(req.user.id);
        res.json(passkeys);
    } catch (e) {
        console.error('Get passkeys error:', e);
        res.status(500).json({ error: 'パスキー一覧の取得に失敗しました' });
    }
});

// パスキー登録オプション生成
app.post('/api/passkeys/register/options', authMiddleware, async (req, res) => {
    try {
        const existingPasskeys = db.prepare('SELECT credential_id FROM passkeys WHERE user_id = ?')
            .all(req.user.id);
        
        // SimpleWebAuthn v9+ requires userID as Uint8Array
        const userIdBuffer = new TextEncoder().encode(req.user.id);
        
        const options = await generateRegistrationOptions({
            rpName: CONFIG.RP_NAME,
            rpID: CONFIG.RP_ID,
            userID: userIdBuffer,
            userName: req.user.name || req.user.email,
            userDisplayName: req.user.name || req.user.email,
            attestationType: 'none',
            excludeCredentials: existingPasskeys.map(pk => ({
                id: Buffer.from(pk.credential_id, 'base64url'),
                type: 'public-key'
            })),
            authenticatorSelection: {
                residentKey: 'preferred',
                userVerification: 'preferred'
            }
        });
        
        webauthnChallenges.set(`reg_${req.user.id}`, {
            challenge: options.challenge,
            expires: Date.now() + 5 * 60 * 1000
        });
        
        // SimpleWebAuthn v9+ returns base64url encoded strings already
        // Convert user.id (Uint8Array) to base64url for frontend
        const responseOptions = {
            ...options,
            user: {
                ...options.user,
                id: Buffer.from(options.user.id).toString('base64url')
            },
            excludeCredentials: options.excludeCredentials?.map(cred => ({
                ...cred,
                id: typeof cred.id === 'string' ? cred.id : Buffer.from(cred.id).toString('base64url')
            })) || []
        };
        
        res.json(responseOptions);
    } catch (e) {
        console.error('Passkey register options error:', e);
        res.status(500).json({ error: 'パスキー登録オプションの生成に失敗しました' });
    }
});

// パスキー登録完了
app.post('/api/passkeys/register/verify', authMiddleware, async (req, res) => {
    try {
        const { name, response } = req.body;
        
        if (!name || !response) {
            return res.status(400).json({ error: '名前と認証レスポンスが必要です' });
        }
        
        const challengeData = webauthnChallenges.get(`reg_${req.user.id}`);
        if (!challengeData || challengeData.expires < Date.now()) {
            return res.status(400).json({ error: 'チャレンジが無効または期限切れです' });
        }
        
        const verification = await verifyRegistrationResponse({
            response,
            expectedChallenge: challengeData.challenge,
            expectedOrigin: CONFIG.RP_ORIGIN,
            expectedRPID: CONFIG.RP_ID
        });
        
        if (!verification.verified) {
            return res.status(400).json({ error: '検証に失敗しました' });
        }
        
        const { credentialID, credentialPublicKey, counter } = verification.registrationInfo;
        
        db.prepare(`
            INSERT INTO passkeys (user_id, credential_id, public_key, counter, name)
            VALUES (?, ?, ?, ?, ?)
        `).run(
            req.user.id,
            Buffer.from(credentialID).toString('base64url'),
            Buffer.from(credentialPublicKey).toString('base64'),
            counter,
            name
        );
        
        db.prepare('UPDATE users SET passkey_enabled = 1 WHERE id = ?').run(req.user.id);
        
        webauthnChallenges.delete(`reg_${req.user.id}`);
        
        logActivity(req.user.id, null, 'passkey_register', { name }, req.ip);
        
        res.json({ success: true, message: 'パスキーを登録しました' });
    } catch (e) {
        console.error('Passkey register verify error:', e);
        res.status(500).json({ error: 'パスキーの登録に失敗しました' });
    }
});

// パスキー認証オプション生成（2FA用）
app.post('/api/passkeys/authenticate/options', async (req, res) => {
    try {
        const { pendingToken } = req.body;
        
        if (!pendingToken) {
            return res.status(400).json({ error: 'トークンが必要です' });
        }
        
        let decoded;
        try {
            decoded = jwt.verify(pendingToken, CONFIG.JWT_SECRET);
            if (!decoded.pending2fa) {
                return res.status(400).json({ error: '無効なトークンです' });
            }
        } catch (e) {
            return res.status(400).json({ error: 'トークンが無効または期限切れです' });
        }
        
        const passkeys = db.prepare('SELECT credential_id FROM passkeys WHERE user_id = ?')
            .all(decoded.userId);
        
        if (passkeys.length === 0) {
            return res.status(400).json({ error: 'パスキーが登録されていません' });
        }
        
        const options = await generateAuthenticationOptions({
            rpID: CONFIG.RP_ID,
            allowCredentials: passkeys.map(pk => ({
                id: Buffer.from(pk.credential_id, 'base64url'),
                type: 'public-key'
            })),
            userVerification: 'preferred'
        });
        
        webauthnChallenges.set(`auth_${decoded.userId}`, {
            challenge: options.challenge,
            expires: Date.now() + 5 * 60 * 1000
        });
        
        // Convert allowCredentials ids to base64url strings for frontend
        const responseOptions = {
            ...options,
            allowCredentials: options.allowCredentials?.map(cred => ({
                ...cred,
                id: typeof cred.id === 'string' ? cred.id : Buffer.from(cred.id).toString('base64url')
            })) || []
        };
        
        res.json(responseOptions);
    } catch (e) {
        console.error('Passkey auth options error:', e);
        res.status(500).json({ error: '認証オプションの生成に失敗しました' });
    }
});

// パスキー認証完了（2FA）
app.post('/api/passkeys/authenticate/verify', async (req, res) => {
    try {
        const { pendingToken, response } = req.body;
        
        if (!pendingToken || !response) {
            return res.status(400).json({ error: 'トークンとレスポンスが必要です' });
        }
        
        let decoded;
        try {
            decoded = jwt.verify(pendingToken, CONFIG.JWT_SECRET);
            if (!decoded.pending2fa) {
                return res.status(400).json({ error: '無効なトークンです' });
            }
        } catch (e) {
            return res.status(400).json({ error: 'トークンが無効または期限切れです' });
        }
        
        const challengeData = webauthnChallenges.get(`auth_${decoded.userId}`);
        if (!challengeData || challengeData.expires < Date.now()) {
            return res.status(400).json({ error: 'チャレンジが無効または期限切れです' });
        }
        
        const credentialId = response.id;
        const passkey = db.prepare('SELECT * FROM passkeys WHERE credential_id = ? AND user_id = ?')
            .get(credentialId, decoded.userId);
        
        if (!passkey) {
            return res.status(400).json({ error: 'パスキーが見つかりません' });
        }
        
        const verification = await verifyAuthenticationResponse({
            response,
            expectedChallenge: challengeData.challenge,
            expectedOrigin: CONFIG.RP_ORIGIN,
            expectedRPID: CONFIG.RP_ID,
            authenticator: {
                credentialID: Buffer.from(passkey.credential_id, 'base64url'),
                credentialPublicKey: Buffer.from(passkey.public_key, 'base64'),
                counter: passkey.counter
            }
        });
        
        if (!verification.verified) {
            return res.status(400).json({ error: '認証に失敗しました' });
        }
        
        db.prepare('UPDATE passkeys SET counter = ?, last_used = CURRENT_TIMESTAMP WHERE id = ?')
            .run(verification.authenticationInfo.newCounter, passkey.id);
        
        webauthnChallenges.delete(`auth_${decoded.userId}`);
        
        const user = db.prepare('SELECT * FROM users WHERE id = ?').get(decoded.userId);
        const token = jwt.sign({ userId: user.id, discordId: user.discord_id }, CONFIG.JWT_SECRET, { expiresIn: '90d' });
        
        logActivity(user.id, null, 'login_2fa', { method: 'passkey' }, req.ip);
        
        res.json({ success: true, token });
    } catch (e) {
        console.error('Passkey auth verify error:', e);
        res.status(500).json({ error: '認証に失敗しました' });
    }
});

// パスキー削除
app.delete('/api/passkeys/:id', authMiddleware, (req, res) => {
    try {
        const passkey = db.prepare('SELECT * FROM passkeys WHERE id = ? AND user_id = ?')
            .get(req.params.id, req.user.id);
        
        if (!passkey) {
            return res.status(404).json({ error: 'パスキーが見つかりません' });
        }
        
        db.prepare('DELETE FROM passkeys WHERE id = ?').run(req.params.id);
        
        const remaining = db.prepare('SELECT COUNT(*) as count FROM passkeys WHERE user_id = ?')
            .get(req.user.id);
        if (remaining.count === 0) {
            db.prepare('UPDATE users SET passkey_enabled = 0 WHERE id = ?').run(req.user.id);
        }
        
        logActivity(req.user.id, null, 'passkey_delete', { name: passkey.name }, req.ip);
        
        res.json({ success: true, message: 'パスキーを削除しました' });
    } catch (e) {
        console.error('Delete passkey error:', e);
        res.status(500).json({ error: 'パスキーの削除に失敗しました' });
    }
});

// ============ TOTP二段階認証API ============

// TOTP設定状態取得
app.get('/api/totp/status', authMiddleware, (req, res) => {
    try {
        const totp = db.prepare('SELECT enabled FROM totp_secrets WHERE user_id = ?')
            .get(req.user.id);
        res.json({ enabled: totp?.enabled === 1 });
    } catch (e) {
        console.error('Get TOTP status error:', e);
        res.status(500).json({ error: 'TOTP状態の取得に失敗しました' });
    }
});

// TOTPセットアップ開始
app.post('/api/totp/setup', authMiddleware, async (req, res) => {
    try {
        const existing = db.prepare('SELECT enabled FROM totp_secrets WHERE user_id = ?')
            .get(req.user.id);
        if (existing?.enabled === 1) {
            return res.status(400).json({ error: '二段階認証は既に有効です' });
        }
        
        const secret = authenticator.generateSecret();
        
        const otpauth = authenticator.keyuri(
            req.user.email || req.user.name,
            'LenovoServer',
            secret
        );
        
        const qrCode = await QRCode.toDataURL(otpauth);
        
        const backupCodes = Array.from({ length: 10 }, () => 
            crypto.randomBytes(4).toString('hex').toUpperCase()
        );
        
        db.prepare(`
            INSERT OR REPLACE INTO totp_secrets (user_id, secret, enabled, backup_codes)
            VALUES (?, ?, 0, ?)
        `).run(req.user.id, secret, JSON.stringify(backupCodes));
        
        res.json({ 
            secret, 
            qrCode, 
            backupCodes,
            message: '認証アプリでQRコードをスキャンし、表示されたコードで確認してください'
        });
    } catch (e) {
        console.error('TOTP setup error:', e);
        res.status(500).json({ error: 'TOTPセットアップに失敗しました' });
    }
});

// TOTPセットアップ確認・有効化
app.post('/api/totp/verify-setup', authMiddleware, (req, res) => {
    try {
        const { code } = req.body;
        
        if (!code) {
            return res.status(400).json({ error: '認証コードが必要です' });
        }
        
        const totp = db.prepare('SELECT secret FROM totp_secrets WHERE user_id = ?')
            .get(req.user.id);
        
        if (!totp) {
            return res.status(400).json({ error: 'セットアップが開始されていません' });
        }
        
        const isValid = authenticator.check(code, totp.secret);
        
        if (!isValid) {
            return res.status(400).json({ error: '認証コードが正しくありません' });
        }
        
        db.prepare('UPDATE totp_secrets SET enabled = 1 WHERE user_id = ?').run(req.user.id);
        db.prepare('UPDATE users SET two_factor_enabled = 1 WHERE id = ?').run(req.user.id);
        
        logActivity(req.user.id, null, 'totp_enable', {}, req.ip);
        
        res.json({ success: true, message: '二段階認証を有効化しました' });
    } catch (e) {
        console.error('TOTP verify setup error:', e);
        res.status(500).json({ error: 'TOTP確認に失敗しました' });
    }
});

// TOTP認証（2FA用）
app.post('/api/totp/authenticate', async (req, res) => {
    try {
        const { pendingToken, code } = req.body;
        
        if (!pendingToken || !code) {
            return res.status(400).json({ error: 'トークンとコードが必要です' });
        }
        
        let decoded;
        try {
            decoded = jwt.verify(pendingToken, CONFIG.JWT_SECRET);
            if (!decoded.pending2fa) {
                return res.status(400).json({ error: '無効なトークンです' });
            }
        } catch (e) {
            return res.status(400).json({ error: 'トークンが無効または期限切れです' });
        }
        
        const totp = db.prepare('SELECT secret, backup_codes FROM totp_secrets WHERE user_id = ? AND enabled = 1')
            .get(decoded.userId);
        
        if (!totp) {
            return res.status(400).json({ error: '二段階認証が設定されていません' });
        }
        
        let isValid = authenticator.check(code, totp.secret);
        let usedBackupCode = false;
        
        if (!isValid && totp.backup_codes) {
            const backupCodes = JSON.parse(totp.backup_codes);
            const index = backupCodes.indexOf(code.toUpperCase());
            if (index !== -1) {
                isValid = true;
                usedBackupCode = true;
                backupCodes.splice(index, 1);
                db.prepare('UPDATE totp_secrets SET backup_codes = ? WHERE user_id = ?')
                    .run(JSON.stringify(backupCodes), decoded.userId);
            }
        }
        
        if (!isValid) {
            return res.status(400).json({ error: '認証コードが正しくありません' });
        }
        
        const user = db.prepare('SELECT * FROM users WHERE id = ?').get(decoded.userId);
        const token = jwt.sign({ userId: user.id, discordId: user.discord_id }, CONFIG.JWT_SECRET, { expiresIn: '90d' });
        
        logActivity(user.id, null, 'login_2fa', { method: usedBackupCode ? 'backup_code' : 'totp' }, req.ip);
        
        res.json({ 
            success: true, 
            token,
            warning: usedBackupCode ? 'バックアップコードを使用しました。残りのコードを確認してください。' : null
        });
    } catch (e) {
        console.error('TOTP authenticate error:', e);
        res.status(500).json({ error: '認証に失敗しました' });
    }
});

// TOTP無効化
app.post('/api/totp/disable', authMiddleware, (req, res) => {
    try {
        const { code } = req.body;
        
        if (!code) {
            return res.status(400).json({ error: '認証コードが必要です' });
        }
        
        const totp = db.prepare('SELECT secret FROM totp_secrets WHERE user_id = ? AND enabled = 1')
            .get(req.user.id);
        
        if (!totp) {
            return res.status(400).json({ error: '二段階認証が設定されていません' });
        }
        
        const isValid = authenticator.check(code, totp.secret);
        
        if (!isValid) {
            return res.status(400).json({ error: '認証コードが正しくありません' });
        }
        
        db.prepare('DELETE FROM totp_secrets WHERE user_id = ?').run(req.user.id);
        db.prepare('UPDATE users SET two_factor_enabled = 0 WHERE id = ?').run(req.user.id);
        
        logActivity(req.user.id, null, 'totp_disable', {}, req.ip);
        
        res.json({ success: true, message: '二段階認証を無効化しました' });
    } catch (e) {
        console.error('TOTP disable error:', e);
        res.status(500).json({ error: 'TOTP無効化に失敗しました' });
    }
});

// セキュリティ設定取得
app.get('/api/security/status', authMiddleware, (req, res) => {
    try {
        const passkeys = db.prepare('SELECT id, name, created_at, last_used FROM passkeys WHERE user_id = ?')
            .all(req.user.id);
        const totp = db.prepare('SELECT enabled FROM totp_secrets WHERE user_id = ?')
            .get(req.user.id);
        
        res.json({
            passkeys,
            passkey_enabled: !!req.user.passkey_enabled,
            totp_enabled: totp?.enabled === 1,
            two_factor_enabled: !!req.user.two_factor_enabled
        });
    } catch (e) {
        console.error('Get security status error:', e);
        res.status(500).json({ error: 'セキュリティ状態の取得に失敗しました' });
    }
});

// ============ SSHゲートウェイ用内部API ============
// SSHゲートウェイがユーザー情報を取得するためのAPI（内部ネットワーク用）
app.get('/api/internal/ssh-users', (req, res) => {
    try {
        const containers = db.prepare(`
            SELECT c.id, c.ssh_user, c.ssh_password, c.docker_id, c.user_id
            FROM containers c
            WHERE c.ssh_user IS NOT NULL AND c.docker_id IS NOT NULL AND c.status = 'running'
        `).all();
        
        // 各コンテナのユーザーの公開鍵を取得
        const result = containers.map(c => {
            const keys = db.prepare(`
                SELECT public_key FROM ssh_keys WHERE user_id = ?
            `).all(c.user_id);

            let backendType = 'docker';
            let backendTarget = `shironeko-${c.id}`;

            // docker_id can store VM mapping as "vm:<vm-name>"
            if (c.docker_id && c.docker_id.startsWith('vm:')) {
                backendType = 'vm';
                backendTarget = c.docker_id.replace(/^vm:/, '');
            }
            
            return {
                ssh_user: c.ssh_user,
                ssh_password: c.ssh_password,
                backend_type: backendType,
                backend_target: backendTarget,
                docker_name: `shironeko-${c.id}`,
                authorized_keys: keys.map(k => k.public_key)
            };
        });
        
        res.json(result);
    } catch (e) {
        console.error('SSH users API error:', e);
        res.status(500).json([]);
    }
});

// ============ SSH公開鍵管理API ============
// 公開鍵のフィンガープリントを計算
function calculateFingerprint(publicKey) {
    // 公開鍵からコメント部分を除去して本体だけ取得
    const parts = publicKey.trim().split(' ');
    if (parts.length < 2) return null;
    const keyData = Buffer.from(parts[1], 'base64');
    const hash = crypto.createHash('sha256').update(keyData).digest('base64');
    return 'SHA256:' + hash.replace(/=+$/, '');
}

// 公開鍵一覧取得
app.get('/api/ssh-keys', authMiddleware, (req, res) => {
    try {
        const keys = db.prepare(`
            SELECT id, name, fingerprint, created_at FROM ssh_keys WHERE user_id = ? ORDER BY created_at DESC
        `).all(req.user.id);
        res.json(keys);
    } catch (e) {
        console.error('Get SSH keys error:', e);
        res.status(500).json({ error: 'SSH鍵の取得に失敗しました' });
    }
});

// 公開鍵追加
app.post('/api/ssh-keys', authMiddleware, (req, res) => {
    try {
        const { name, public_key } = req.body;
        
        if (!name || !public_key) {
            return res.status(400).json({ error: '名前と公開鍵は必須です' });
        }
        
        // 公開鍵の形式を検証
        const keyPattern = /^(ssh-rsa|ssh-ed25519|ecdsa-sha2-nistp256|ecdsa-sha2-nistp384|ecdsa-sha2-nistp521)\s+[A-Za-z0-9+\/=]+(\s+.*)?$/;
        if (!keyPattern.test(public_key.trim())) {
            return res.status(400).json({ error: '無効な公開鍵形式です。ssh-rsa, ssh-ed25519, ecdsa形式のみ対応しています' });
        }
        
        const fingerprint = calculateFingerprint(public_key);
        if (!fingerprint) {
            return res.status(400).json({ error: '公開鍵のフィンガープリントを計算できませんでした' });
        }
        
        // 同じフィンガープリントの鍵がないか確認
        const existing = db.prepare(`SELECT id FROM ssh_keys WHERE user_id = ? AND fingerprint = ?`).get(req.user.id, fingerprint);
        if (existing) {
            return res.status(400).json({ error: 'この公開鍵は既に登録されています' });
        }
        
        // 登録数制限（最大10個）
        const count = db.prepare(`SELECT COUNT(*) as count FROM ssh_keys WHERE user_id = ?`).get(req.user.id);
        if (count.count >= 10) {
            return res.status(400).json({ error: '登録できる公開鍵は最大10個までです' });
        }
        
        const result = db.prepare(`
            INSERT INTO ssh_keys (user_id, name, public_key, fingerprint) VALUES (?, ?, ?, ?)
        `).run(req.user.id, name.trim(), public_key.trim(), fingerprint);
        
        logActivity(req.user.id, null, 'ssh_key_add', { name, fingerprint }, req.ip);
        
        res.json({ 
            success: true, 
            message: 'SSH公開鍵を登録しました',
            key: {
                id: result.lastInsertRowid,
                name: name.trim(),
                fingerprint
            }
        });
    } catch (e) {
        console.error('Add SSH key error:', e);
        res.status(500).json({ error: 'SSH鍵の登録に失敗しました' });
    }
});

// 公開鍵削除
app.delete('/api/ssh-keys/:id', authMiddleware, (req, res) => {
    try {
        const key = db.prepare(`SELECT * FROM ssh_keys WHERE id = ? AND user_id = ?`).get(req.params.id, req.user.id);
        if (!key) {
            return res.status(404).json({ error: 'SSH鍵が見つかりません' });
        }
        
        db.prepare(`DELETE FROM ssh_keys WHERE id = ?`).run(req.params.id);
        
        logActivity(req.user.id, null, 'ssh_key_delete', { name: key.name, fingerprint: key.fingerprint }, req.ip);
        
        res.json({ success: true, message: 'SSH公開鍵を削除しました' });
    } catch (e) {
        console.error('Delete SSH key error:', e);
        res.status(500).json({ error: 'SSH鍵の削除に失敗しました' });
    }
});

// ============ サーバータイプAPI ============
app.get('/api/server-types', (req, res) => {
    res.json(Object.entries(CONFIG.SERVER_TYPES).map(([id, type]) => ({
        id,
        ...type
    })));
});

// ============ コンテナ管理API ============
app.get('/api/containers', authMiddleware, async (req, res) => {
    try {
        const containers = db.prepare(`
            SELECT * FROM containers WHERE user_id = ? ORDER BY created_at DESC
        `).all(req.user.id);
        
        const containersWithStats = await Promise.all(containers.map(async (c) => {
            let realStatus = c.status;
            const isVmBackend = isVmBackendRef(c.docker_id);
            const vmName = isVmBackend ? c.docker_id.replace(/^vm:/, '') : null;
            const vmInfo = isVmBackend ? await getVmBackendInfo(vmName) : null;
            
            // Docker状態を確認
            if (dockerAvailable && isDockerBackendRef(c.docker_id)) {
                try {
                    const dockerContainer = docker.getContainer(c.docker_id);
                    const inspect = await dockerContainer.inspect();
                    realStatus = inspect.State.Running ? 'running' : 'stopped';
                    
                    // DB更新
                    if (realStatus !== c.status) {
                        db.prepare('UPDATE containers SET status = ? WHERE id = ?').run(realStatus, c.id);
                    }
                } catch (e) {}
            } else if (vmInfo?.status) {
                realStatus = vmInfo.status;
                if (realStatus !== c.status) {
                    db.prepare('UPDATE containers SET status = ? WHERE id = ?').run(realStatus, c.id);
                }
            }
            
            const stats = realStatus === 'running' && isDockerBackendRef(c.docker_id)
                ? await getContainerStats(c.docker_id, c.id)
                : {
                cpuUsage: 0, memoryUsage: 0, networkIn: 0, networkOut: 0, diskUsageBytes: 0
            };
            const hasDockerStats = realStatus === 'running' && isDockerBackendRef(c.docker_id);
            const hasVmStats = realStatus === 'running' && isVmBackend && !!vmInfo?.stats;
            
            const serverType = CONFIG.SERVER_TYPES[c.type] || CONFIG.SERVER_TYPES.ubuntu;
            
            // VMバックエンドはlibvirt実値を優先し、取得できない場合はDBの設定値を使用
            const configuredCpuLimit = c.cpu_limit || 0.5;
            const configuredMemoryLimit = c.memory_limit || 512;
            const configuredStorageLimit = c.storage_limit || 1024;
            const cpuLimit = vmInfo?.cpu_limit || configuredCpuLimit;
            const memoryLimit = vmInfo?.memory_limit || configuredMemoryLimit;
            const storageLimit = vmInfo?.storage_limit || configuredStorageLimit;
            const storageLimitBytes = storageLimit * 1024 * 1024; // MB to Bytes
            const diskUsagePercent = storageLimitBytes > 0 
                ? ((stats.diskUsageBytes / storageLimitBytes) * 100).toFixed(1) 
                : 0;
            
            return {
                ...c,
                status: realStatus,
                backend_type: isVmBackend ? 'vm' : 'docker',
                backend_target: isVmBackend ? vmName : c.docker_id,
                resource_source: vmInfo?.available ? 'vm' : 'configured',
                type_name: serverType.name,
                env_vars: JSON.parse(c.env_vars || '{}'),
                cpu: `${cpuLimit} vCPU`,
                memory: `${memoryLimit} MB`,
                storage: `${storageLimit} MB`,
                cpu_limit: cpuLimit,
                memory_limit: memoryLimit,
                storage_limit: storageLimit,
                configured_cpu_limit: configuredCpuLimit,
                configured_memory_limit: configuredMemoryLimit,
                configured_storage_limit: configuredStorageLimit,
                ssh_host: CONFIG.SSH_HOST,
                ssh_port: CONFIG.SSH_PORT,
                ssh_command: c.ssh_user ? `ssh ${c.ssh_user}@${CONFIG.SSH_HOST} -p ${CONFIG.SSH_PORT}` : null,
                stats: {
                    available: hasDockerStats || hasVmStats,
                    cpuUsage: hasDockerStats ? stats.cpuUsage : (hasVmStats ? vmInfo.stats.cpuUsage : null),
                    memoryUsage: hasDockerStats ? stats.memoryUsage : (hasVmStats ? vmInfo.stats.memoryUsage : null),
                    networkIn: hasDockerStats
                        ? formatBytes(stats.networkIn)
                        : (hasVmStats && Number.isFinite(vmInfo.stats.networkInBytes) ? formatBytes(vmInfo.stats.networkInBytes) : null),
                    networkOut: hasDockerStats
                        ? formatBytes(stats.networkOut)
                        : (hasVmStats && Number.isFinite(vmInfo.stats.networkOutBytes) ? formatBytes(vmInfo.stats.networkOutBytes) : null),
                    diskUsage: hasDockerStats
                        ? formatBytes(stats.diskUsageBytes)
                        : (hasVmStats && Number.isFinite(vmInfo.stats.diskUsageBytes) ? formatBytes(vmInfo.stats.diskUsageBytes) : null),
                    diskUsageBytes: hasDockerStats
                        ? stats.diskUsageBytes
                        : (hasVmStats && Number.isFinite(vmInfo.stats.diskUsageBytes) ? vmInfo.stats.diskUsageBytes : null),
                    diskUsagePercent: hasDockerStats ? diskUsagePercent : (hasVmStats ? vmInfo.stats.diskUsagePercent : null)
                }
            };
        }));
        
        res.json(containersWithStats);
    } catch (e) {
        console.error('Get containers error:', e);
        res.status(500).json({ error: 'コンテナ一覧の取得に失敗しました' });
    }
});

app.post('/api/containers', authMiddleware, async (req, res) => {
    try {
        const { name, type, env_vars, auto_restart, startup_command } = req.body;
        
        if (!name) {
            return res.status(400).json({ error: 'サーバー名を指定してください' });
        }
        
        const serverType = CONFIG.SERVER_TYPES[type] || CONFIG.SERVER_TYPES.ubuntu;
        
        // ユーザー制限チェック（プラン上限を適用）
        const userLimits = getEffectiveUserLimits(req.user);
        const maxContainers = userLimits.maxContainers;
        const containerCount = db.prepare('SELECT COUNT(*) as count FROM containers WHERE user_id = ?').get(req.user.id).count;
        
        if (maxContainers !== -1 && containerCount >= maxContainers) {
            return res.status(400).json({ 
                error: `サーバー上限(${maxContainers}台)に達しています。管理者にお問い合わせください。` 
            });
        }
        
        // サーバー初期スペックはユーザープラン上限に合わせる
        const planConfig = {
            cpu: userLimits.maxCpu,
            memory: userLimits.maxMemory,
            storage: userLimits.maxStorage
        };
        const containerPlan = req.user.plan || 'free';
        const containerId = `server-${uuidv4().slice(0, 12)}`;
        const sshPassword = generatePassword();
        
        // コンテナディレクトリ作成
        const containerDir = path.join(CONFIG.CONTAINERS_DIR, containerId);
        fs.mkdirSync(containerDir, { recursive: true });
        
        // 初期ファイル作成
        fs.writeFileSync(path.join(containerDir, 'README.md'), `# ${name}

このサーバーは LenovoServer で作成されました。

## サーバー情報
- タイプ: ${serverType.name}
- プラン: ${containerPlan}
- 作成日: ${new Date().toISOString()}

## 使い方
1. コンソールタブでターミナルを使用できます
2. ファイルタブでファイルを管理できます
3. 環境変数タブで環境変数を設定できます

## 起動コマンド
startup_commandに起動コマンドを設定すると、サーバー起動時に自動実行されます。
`);
        
        // SSHユーザー名を生成
        const sshUser = generateSSHUser(containerId);
        
        // Dockerコンテナ作成
        let dockerId = null;
        if (dockerAvailable) {
            try {
                // コンテナ作成（SSHゲートウェイ経由でアクセス）
                const container = await docker.createContainer({
                    Image: CONFIG.BASE_IMAGE,
                    name: `shironeko-${containerId}`,
                    Hostname: name.replace(/[^a-zA-Z0-9-]/g, '-').toLowerCase(),
                    Tty: true,
                    OpenStdin: true,
                    User: 'root',
                    Env: [
                        ...Object.entries(env_vars || {}).map(([k, v]) => `${k}=${v}`),
                        `SERVER_NAME=${name}`,
                        `SERVER_TYPE=${type}`,
                        `SERVER_PLAN=${containerPlan}`,
                        'DEBIAN_FRONTEND=noninteractive'
                    ],
                    HostConfig: {
                        Memory: planConfig.memory * 1024 * 1024,
                        NanoCpus: planConfig.cpu * 1e9,
                        Binds: [`${containerDir}:/app`],
                        RestartPolicy: auto_restart ? { Name: 'unless-stopped' } : { Name: 'no' },
                        Dns: ['8.8.8.8', '8.8.4.4'],
                        NetworkMode: 'shironeko-network'
                    },
                    WorkingDir: '/app',
                    Cmd: ['/bin/bash', '-c', 'while true; do sleep 3600; done']
                });
                
                dockerId = container.id;
                
                // コンテナ起動
                await container.start();
                
                // 基本パッケージ + サーバータイプ固有パッケージをインストール
                const allPackages = [...CONFIG.BASE_PACKAGES, ...serverType.packages];
                const setupCmd = `apt-get update && apt-get install -y ${allPackages.join(' ')} && apt-get clean`;
                const exec = await container.exec({
                    Cmd: ['bash', '-c', setupCmd],
                    AttachStdout: true,
                    AttachStderr: true
                });
                exec.start({ Detach: true }).catch(e => console.log('Package setup started in background'));
                
                console.log(`✅ Container ${containerId} created (SSH user: ${sshUser})`);
                
            } catch (e) {
                console.error('Docker container creation error:', e);
                // エラーでもDB登録は続行
            }
        }
        
        // DB保存
        db.prepare(`
            INSERT INTO containers (id, user_id, name, type, docker_id, status, plan, cpu_limit, memory_limit, storage_limit, auto_restart, env_vars, ssh_user, ssh_password, startup_command)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(
            containerId, 
            req.user.id, 
            name, 
            type || 'ubuntu',
            dockerId,
            dockerId ? 'running' : 'stopped',
            containerPlan,
            planConfig.cpu,
            planConfig.memory,
            planConfig.storage,
            auto_restart ? 1 : 0,
            JSON.stringify(env_vars || {}),
            sshUser,
            sshPassword,
            startup_command || ''
        );
        
        logActivity(req.user.id, containerId, 'container_create', { name, type }, req.ip);
        
        const containerData = db.prepare('SELECT * FROM containers WHERE id = ?').get(containerId);
        res.json({
            ...containerData,
            type_name: serverType.name,
            cpu: `${containerData.cpu_limit} vCPU`,
            memory: `${containerData.memory_limit}MB`,
            storage: `${containerData.storage_limit}MB`,
            cpu_limit: containerData.cpu_limit,
            memory_limit: containerData.memory_limit,
            storage_limit: containerData.storage_limit,
            ssh_host: CONFIG.SSH_HOST,
            ssh_port: CONFIG.SSH_PORT,
            ssh_command: `ssh ${sshUser}@${CONFIG.SSH_HOST} -p ${CONFIG.SSH_PORT}`,
            stats: { cpuUsage: 0, memoryUsage: 0, networkIn: '0 B', networkOut: '0 B' }
        });
    } catch (e) {
        console.error('Create container error:', e);
        res.status(500).json({ error: 'サーバーの作成に失敗しました: ' + e.message });
    }
});

app.get('/api/containers/:id', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        
        const isVmBackend = isVmBackendRef(container.docker_id);
        const vmName = isVmBackend ? container.docker_id.replace(/^vm:/, '') : null;
        const vmInfo = isVmBackend ? await getVmBackendInfo(vmName) : null;
        const realStatus = vmInfo?.status || container.status;

        if (realStatus !== container.status) {
            db.prepare('UPDATE containers SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?')
                .run(realStatus, container.id);
        }

        const stats = realStatus === 'running' && isDockerBackendRef(container.docker_id)
            ? await getContainerStats(container.docker_id, container.id)
            : { cpuUsage: 0, memoryUsage: 0, networkIn: 0, networkOut: 0, diskUsageBytes: 0 };
        const hasDockerStats = realStatus === 'running' && isDockerBackendRef(container.docker_id);
        const hasVmStats = realStatus === 'running' && isVmBackend && !!vmInfo?.stats;
        
        const configuredCpuLimit = container.cpu_limit || 0.5;
        const configuredMemoryLimit = container.memory_limit || 512;
        const configuredStorageLimit = container.storage_limit || 1024;
        const cpuLimit = vmInfo?.cpu_limit || configuredCpuLimit;
        const memoryLimit = vmInfo?.memory_limit || configuredMemoryLimit;
        const storageLimit = vmInfo?.storage_limit || configuredStorageLimit;
        const storageLimitBytes = storageLimit * 1024 * 1024;
        const diskUsagePercent = storageLimitBytes > 0 
            ? ((stats.diskUsageBytes / storageLimitBytes) * 100).toFixed(1) 
            : 0;
        
        const serverType = CONFIG.SERVER_TYPES[container.type] || CONFIG.SERVER_TYPES.ubuntu;
        
        res.json({
            ...container,
            status: realStatus,
            backend_type: isVmBackend ? 'vm' : 'docker',
            backend_target: isVmBackend ? vmName : container.docker_id,
            resource_source: vmInfo?.available ? 'vm' : 'configured',
            type_name: serverType.name,
            env_vars: JSON.parse(container.env_vars || '{}'),
            cpu: `${cpuLimit} vCPU`,
            memory: `${memoryLimit} MB`,
            storage: `${storageLimit} MB`,
            cpu_limit: cpuLimit,
            memory_limit: memoryLimit,
            storage_limit: storageLimit,
            configured_cpu_limit: configuredCpuLimit,
            configured_memory_limit: configuredMemoryLimit,
            configured_storage_limit: configuredStorageLimit,
            ssh_host: CONFIG.SSH_HOST,
            ssh_port: CONFIG.SSH_PORT,
            ssh_command: container.ssh_user ? `ssh ${container.ssh_user}@${CONFIG.SSH_HOST} -p ${CONFIG.SSH_PORT}` : null,
            stats: {
                available: hasDockerStats || hasVmStats,
                cpuUsage: hasDockerStats ? stats.cpuUsage : (hasVmStats ? vmInfo.stats.cpuUsage : null),
                memoryUsage: hasDockerStats ? stats.memoryUsage : (hasVmStats ? vmInfo.stats.memoryUsage : null),
                networkIn: hasDockerStats
                    ? formatBytes(stats.networkIn)
                    : (hasVmStats && Number.isFinite(vmInfo.stats.networkInBytes) ? formatBytes(vmInfo.stats.networkInBytes) : null),
                networkOut: hasDockerStats
                    ? formatBytes(stats.networkOut)
                    : (hasVmStats && Number.isFinite(vmInfo.stats.networkOutBytes) ? formatBytes(vmInfo.stats.networkOutBytes) : null),
                diskUsage: hasDockerStats
                    ? formatBytes(stats.diskUsageBytes)
                    : (hasVmStats && Number.isFinite(vmInfo.stats.diskUsageBytes) ? formatBytes(vmInfo.stats.diskUsageBytes) : null),
                diskUsageBytes: hasDockerStats
                    ? stats.diskUsageBytes
                    : (hasVmStats && Number.isFinite(vmInfo.stats.diskUsageBytes) ? vmInfo.stats.diskUsageBytes : null),
                diskUsagePercent: hasDockerStats ? diskUsagePercent : (hasVmStats ? vmInfo.stats.diskUsagePercent : null)
            }
        });
    } catch (e) {
        console.error('Get container error:', e);
        res.status(500).json({ error: 'サーバーの取得に失敗しました' });
    }
});

app.post('/api/containers/:id/start', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }

        if (isVmBackendRef(container.docker_id)) {
            const vmName = container.docker_id.replace(/^vm:/, '');
            const vmInfo = await getVmBackendInfo(vmName);
            if (!vmInfo) {
                return res.status(500).json({ error: `VM状態の取得に失敗しました: ${vmName}` });
            }

            if (vmInfo.status !== 'running') {
                const startResult = await runCommand('virsh', ['start', vmName], 10000);
                const alreadyRunning = /already active|already running/i.test(startResult.stderr || '');
                if (!startResult.ok && !alreadyRunning) {
                    return res.status(500).json({
                        error: `VMの起動に失敗しました: ${startResult.stderr || 'unknown error'}`
                    });
                }
            }

            db.prepare(`
                UPDATE containers SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = ?
            `).run(req.params.id);

            logActivity(req.user.id, req.params.id, 'container_start', { backend: 'vm', vm: vmName }, req.ip);
            return res.json({ success: true, message: 'サーバーを起動しました' });
        }
        
        let dockerId = container.docker_id;
        
        if (dockerAvailable) {
            // docker_idがある場合、コンテナが存在するか確認
            if (dockerId) {
                try {
                    const dockerContainer = docker.getContainer(dockerId);
                    const inspect = await dockerContainer.inspect();
                    
                    // 既存コンテナを起動
                    if (!inspect.State.Running) {
                        await dockerContainer.start();
                    }
                    
                    // 起動コマンドがあれば実行
                    if (container.startup_command) {
                        const exec = await dockerContainer.exec({
                            Cmd: ['bash', '-c', `cd /app && ${container.startup_command} &`],
                            User: 'root',
                            AttachStdout: true,
                            AttachStderr: true
                        });
                        await exec.start({ Detach: true });
                    }
                } catch (e) {
                    if (e.statusCode === 404) {
                        // コンテナが存在しない場合、再作成
                        console.log(`Container ${container.id} not found, recreating...`);
                        dockerId = null;
                    } else {
                        throw e;
                    }
                }
            }
            
            // docker_idがない、またはコンテナが削除されていた場合、再作成
            if (!dockerId) {
                const serverType = CONFIG.SERVER_TYPES[container.type] || CONFIG.SERVER_TYPES.ubuntu;
                const planConfig = CONFIG.PLANS[container.plan] || CONFIG.PLANS.free;
                const containerDir = path.join(CONFIG.CONTAINERS_DIR, container.id);
                
                // ディレクトリがなければ作成
                if (!fs.existsSync(containerDir)) {
                    fs.mkdirSync(containerDir, { recursive: true });
                }
                
                const envVars = JSON.parse(container.env_vars || '{}');
                
                try {
                    // 新しいDockerコンテナを作成
                    const newContainer = await docker.createContainer({
                        Image: CONFIG.BASE_IMAGE,
                        name: `shironeko-${container.id}`,
                        Hostname: container.name.replace(/[^a-zA-Z0-9-]/g, '-').toLowerCase(),
                        Tty: true,
                        OpenStdin: true,
                        User: 'root',
                        Env: [
                            ...Object.entries(envVars).map(([k, v]) => `${k}=${v}`),
                            `SERVER_NAME=${container.name}`,
                            `SERVER_TYPE=${container.type}`,
                            `SERVER_PLAN=${container.plan}`,
                            'DEBIAN_FRONTEND=noninteractive'
                        ],
                        HostConfig: {
                            Memory: planConfig.memory * 1024 * 1024,
                            NanoCpus: planConfig.cpu * 1e9,
                            Binds: [`${containerDir}:/app`],
                            RestartPolicy: container.auto_restart ? { Name: 'unless-stopped' } : { Name: 'no' },
                            Dns: ['8.8.8.8', '8.8.4.4'],
                            NetworkMode: 'shironeko-network'
                        },
                        WorkingDir: '/app',
                        Cmd: ['/bin/bash', '-c', 'while true; do sleep 3600; done']
                    });
                    
                    dockerId = newContainer.id;
                    await newContainer.start();
                    
                    // 基本パッケージ + サーバータイプ固有パッケージをインストール
                    const allPackages = [...CONFIG.BASE_PACKAGES, ...serverType.packages];
                    const setupCmd = `apt-get update && apt-get install -y ${allPackages.join(' ')} && apt-get clean`;
                    const exec2 = await newContainer.exec({
                        Cmd: ['bash', '-c', setupCmd],
                        AttachStdout: true,
                        AttachStderr: true
                    });
                    exec2.start({ Detach: true }).catch(() => {});
                    
                    // 起動コマンドがあれば実行
                    if (container.startup_command) {
                        const exec = await newContainer.exec({
                            Cmd: ['bash', '-c', `cd /app && ${container.startup_command} &`],
                            User: 'root',
                            AttachStdout: true,
                            AttachStderr: true
                        });
                        await exec.start({ Detach: true });
                    }
                    
                    // DBを更新
                    db.prepare(`
                        UPDATE containers SET docker_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
                    `).run(dockerId, container.id);
                    
                    console.log(`✅ Container ${container.id} recreated with new docker_id: ${dockerId}`);
                } catch (e) {
                    console.error('Docker container recreation error:', e);
                    return res.status(500).json({ error: 'コンテナの再作成に失敗しました: ' + e.message });
                }
            }
        }
        
        db.prepare(`
            UPDATE containers SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = ?
        `).run(req.params.id);
        
        logActivity(req.user.id, req.params.id, 'container_start', {}, req.ip);
        
        res.json({ success: true, message: 'サーバーを起動しました' });
    } catch (e) {
        console.error('Start container error:', e);
        res.status(500).json({ error: 'サーバーの起動に失敗しました' });
    }
});

app.post('/api/containers/:id/stop', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }

        if (isVmBackendRef(container.docker_id)) {
            const vmName = container.docker_id.replace(/^vm:/, '');
            const vmInfo = await getVmBackendInfo(vmName);
            if (vmInfo?.status === 'running') {
                const stopResult = await runCommand('virsh', ['shutdown', vmName], 8000);
                if (!stopResult.ok) {
                    // graceful shutdown失敗時は強制停止
                    await runCommand('virsh', ['destroy', vmName], 8000);
                }
            }

            db.prepare(`
                UPDATE containers SET status = 'stopped', updated_at = CURRENT_TIMESTAMP WHERE id = ?
            `).run(req.params.id);

            logActivity(req.user.id, req.params.id, 'container_stop', { backend: 'vm', vm: vmName }, req.ip);
            return res.json({ success: true, message: 'サーバーを停止しました' });
        }
        
        if (dockerAvailable && isDockerBackendRef(container.docker_id)) {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                await dockerContainer.stop({ t: 10 });
            } catch (e) {
                console.error('Docker stop error:', e);
            }
        }
        
        db.prepare(`
            UPDATE containers SET status = 'stopped', updated_at = CURRENT_TIMESTAMP WHERE id = ?
        `).run(req.params.id);
        
        logActivity(req.user.id, req.params.id, 'container_stop', {}, req.ip);
        
        res.json({ success: true, message: 'サーバーを停止しました' });
    } catch (e) {
        console.error('Stop container error:', e);
        res.status(500).json({ error: 'サーバーの停止に失敗しました' });
    }
});

app.post('/api/containers/:id/restart', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }

        if (isVmBackendRef(container.docker_id)) {
            const vmName = container.docker_id.replace(/^vm:/, '');
            const vmInfo = await getVmBackendInfo(vmName);
            if (!vmInfo) {
                return res.status(500).json({ error: `VM状態の取得に失敗しました: ${vmName}` });
            }

            if (vmInfo.status === 'running') {
                const rebootResult = await runCommand('virsh', ['reboot', vmName], 10000);
                if (!rebootResult.ok) {
                    await runCommand('virsh', ['destroy', vmName], 8000);
                    await runCommand('virsh', ['start', vmName], 10000);
                }
            } else {
                const startResult = await runCommand('virsh', ['start', vmName], 10000);
                if (!startResult.ok) {
                    return res.status(500).json({
                        error: `VMの起動に失敗しました: ${startResult.stderr || 'unknown error'}`
                    });
                }
            }

            db.prepare(`
                UPDATE containers SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = ?
            `).run(req.params.id);

            logActivity(req.user.id, req.params.id, 'container_restart', { backend: 'vm', vm: vmName }, req.ip);
            return res.json({ success: true, message: 'サーバーを再起動しました' });
        }
        
        if (dockerAvailable && isDockerBackendRef(container.docker_id)) {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                await dockerContainer.restart({ t: 10 });
                
                // 起動コマンドがあれば実行
                if (container.startup_command) {
                    setTimeout(async () => {
                        try {
                            const exec = await dockerContainer.exec({
                                Cmd: ['bash', '-c', `cd /home/user/app && ${container.startup_command} &`],
                                User: 'root'
                            });
                            await exec.start();
                        } catch (e) {}
                    }, 2000);
                }
            } catch (e) {
                console.error('Docker restart error:', e);
            }
        }
        
        db.prepare(`
            UPDATE containers SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = ?
        `).run(req.params.id);
        
        logActivity(req.user.id, req.params.id, 'container_restart', {}, req.ip);
        
        res.json({ success: true, message: 'サーバーを再起動しました' });
    } catch (e) {
        console.error('Restart container error:', e);
        res.status(500).json({ error: 'サーバーの再起動に失敗しました' });
    }
});

app.delete('/api/containers/:id', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        
        if (dockerAvailable && isDockerBackendRef(container.docker_id)) {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                await dockerContainer.stop().catch(() => {});
                await dockerContainer.remove({ force: true });
            } catch (e) {
                console.error('Docker remove error:', e);
            }
        }
        
        // ファイル削除
        const containerDir = path.join(CONFIG.CONTAINERS_DIR, req.params.id);
        if (fs.existsSync(containerDir)) {
            fs.rmSync(containerDir, { recursive: true });
        }
        
        db.prepare('DELETE FROM container_stats WHERE container_id = ?').run(req.params.id);
        db.prepare('DELETE FROM containers WHERE id = ?').run(req.params.id);
        
        logActivity(req.user.id, req.params.id, 'container_delete', {}, req.ip);
        
        res.json({ success: true, message: 'サーバーを削除しました' });
    } catch (e) {
        console.error('Delete container error:', e);
        res.status(500).json({ error: 'サーバーの削除に失敗しました' });
    }
});

// 起動コマンド更新
app.put('/api/containers/:id/startup', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        
        const { startup_command } = req.body;
        
        db.prepare(`
            UPDATE containers SET startup_command = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
        `).run(startup_command || '', req.params.id);
        
        res.json({ success: true, message: '起動コマンドを更新しました' });
    } catch (e) {
        console.error('Update startup command error:', e);
        res.status(500).json({ error: '起動コマンドの更新に失敗しました' });
    }
});

// コンテナ内でコマンド実行
app.post('/api/containers/:id/exec', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        
        const { command } = req.body;
        
        if (!command) {
            return res.status(400).json({ error: 'コマンドを指定してください' });
        }
        
        if (isVmBackendRef(container.docker_id)) {
            const vmName = container.docker_id.replace(/^vm:/, '');
            const vmInfo = await getVmBackendInfo(vmName);
            if (!vmInfo?.available) {
                return res.status(500).json({ error: 'VM状態の取得に失敗しました' });
            }
            if (vmInfo.status !== 'running') {
                return res.status(400).json({ error: 'VMが停止中です。先に起動してください。' });
            }

            const cmd = `cd /app 2>/dev/null || cd / && ${command}`;
            const vmExecResult = await vmExecBash(vmName, cmd, { timeoutMs: 30000 });
            if (!vmExecResult.ok && vmExecResult.stderr) {
                return res.json({ output: vmExecResult.stdout + vmExecResult.stderr });
            }
            return res.json({ output: vmExecResult.stdout + vmExecResult.stderr });
        }

        if (!dockerAvailable || !isDockerBackendRef(container.docker_id)) {
            return res.json({
                output: `[シミュレーション] $ ${command}\nDocker環境では実際に実行されます。`
            });
        }

        const dockerContainer = docker.getContainer(container.docker_id);
        const exec = await dockerContainer.exec({
            Cmd: ['bash', '-c', `cd /home/user/app && ${command}`],
            User: 'root',
            AttachStdout: true,
            AttachStderr: true
        });

        const stream = await exec.start();
        let output = '';

        await new Promise((resolve) => {
            stream.on('data', (data) => { output += data.toString(); });
            stream.on('end', resolve);
            setTimeout(resolve, 30000); // 30秒タイムアウト
        });

        res.json({ output });
    } catch (e) {
        console.error('Exec error:', e);
        res.status(500).json({ error: 'コマンド実行に失敗しました: ' + e.message });
    }
});

// VM ttyd + trycloudflare 一時トンネル発行
app.post('/api/containers/:id/vm/ttyd-tunnel', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);

        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        if (!isVmBackendRef(container.docker_id)) {
            return res.status(400).json({ error: 'VMバックエンドのサーバーのみ対応しています' });
        }

        const vmName = container.docker_id.replace(/^vm:/, '');
        const vmInfo = await getVmBackendInfo(vmName);
        if (!vmInfo?.available) {
            return res.status(500).json({ error: 'VM状態の取得に失敗しました' });
        }

        if (vmInfo.status !== 'running') {
            const startResult = await runCommand('virsh', ['start', vmName], 10000);
            const alreadyRunning = /already active|already running/i.test(startResult.stderr || '');
            if (!startResult.ok && !alreadyRunning) {
                return res.status(500).json({
                    error: `VMの起動に失敗しました: ${startResult.stderr || 'unknown error'}`
                });
            }
            await sleep(1500);
        }

        const requestedPort = req.body?.port;
        const requestedUser = req.body?.username;
        const requestedPassword = req.body?.password;
        const tunnelPort = normalizeVmTtydPort(requestedPort, normalizeVmTtydPort(CONFIG.VM_TTYD_PORT, 7681));
        const tunnelUser = sanitizeTtydAuthUser(requestedUser || container.ssh_user || 'admin', 'admin');
        const tunnelPassword = String(requestedPassword || container.ssh_password || '').trim() || generateTunnelPassword();

        const tunnel = await ensureVmTtydTryCloudflareTunnel(vmName, {
            port: tunnelPort,
            authUser: tunnelUser,
            authPass: tunnelPassword
        });

        if (!tunnel.ok) {
            return res.status(500).json({ error: `トンネル作成に失敗しました: ${tunnel.error}` });
        }

        logActivity(req.user.id, req.params.id, 'vm_ttyd_tunnel_create', {
            backend: 'vm',
            vm: vmName,
            port: tunnel.port,
            url: tunnel.url
        }, req.ip);

        res.json({
            success: true,
            message: '一時トンネルを発行しました',
            backend: 'vm',
            vm_name: vmName,
            url: tunnel.url,
            ttyd_port: tunnel.port,
            username: tunnel.username,
            password: tunnel.password
        });
    } catch (e) {
        console.error('Create VM ttyd tunnel error:', e);
        res.status(500).json({ error: 'VMの一時トンネル発行に失敗しました' });
    }
});

// 環境変数更新
app.put('/api/containers/:id/env', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        
        const { env_vars } = req.body;
        
        db.prepare(`
            UPDATE containers SET env_vars = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
        `).run(JSON.stringify(env_vars || {}), req.params.id);
        
        logActivity(req.user.id, req.params.id, 'env_update', {}, req.ip);
        
        res.json({ success: true, message: '環境変数を更新しました。反映には再起動が必要です。' });
    } catch (e) {
        console.error('Update env error:', e);
        res.status(500).json({ error: '環境変数の更新に失敗しました' });
    }
});

// ログ取得
app.get('/api/containers/:id/logs', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        
        let logs = '';
        
        if (dockerAvailable && isDockerBackendRef(container.docker_id)) {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                const logBuffer = await dockerContainer.logs({
                    stdout: true,
                    stderr: true,
                    tail: 200,
                    timestamps: true
                });
                logs = logBuffer.toString('utf8').replace(/[\x00-\x08]/g, '');
            } catch (e) {
                console.error('Docker logs error:', e);
            }
        }
        
        if (!logs) {
            const now = new Date().toISOString();
            logs = `[${now}] Server: ${container.name}
[${now}] Type: ${container.type}
[${now}] Status: ${container.status}
[${now}] Plan: ${container.plan}
[${now}] ---
[${now}] サーバーを起動するとログが表示されます`;
        }
        
        res.json({ logs });
    } catch (e) {
        console.error('Get logs error:', e);
        res.status(500).json({ error: 'ログの取得に失敗しました' });
    }
});

// ============ ファイル管理API ============
// Dockerコンテナ内のファイル一覧を取得
app.get('/api/containers/:id/files', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }

        if (isVmBackendRef(container.docker_id)) {
            const vmCtx = await resolveVmFileContext(container);
            if (!vmCtx.ok) {
                return res.status(vmCtx.status).json({ error: vmCtx.error });
            }
            const pathResolved = resolveVmFilePath(req.query.path || '/app', vmCtx.vmHomeDir, { allowHome: true });
            if (!pathResolved.ok) {
                return res.status(403).json({ error: pathResolved.error });
            }
            const targetPath = pathResolved.path;

            const script = `ls -la --time-style=+%Y-%m-%dT%H:%M:%S -- ${shellSingleQuote(targetPath)} 2>/dev/null || echo "DIR_NOT_FOUND"`;
            const vmResult = await vmExecBashAsUser(vmCtx.vmName, vmCtx.vmExecUser, script, { timeoutMs: 10000 });
            if (!vmResult.ok && vmResult.stderr) {
                return res.status(500).json({ error: `ファイル一覧の取得に失敗しました: ${vmResult.stderr}` });
            }
            if ((vmResult.stdout || '').includes('DIR_NOT_FOUND')) {
                return res.json({ path: targetPath, items: [] });
            }
            return res.json({ path: targetPath, items: parseLsOutput(vmResult.stdout) });
        }

        const targetPath = normalizeAppPath(req.query.path || '/app');
        if (!isPathWithinApp(targetPath)) {
            return res.status(403).json({ error: 'アクセスが拒否されました (/app 以下のみ)' });
        }
        
        // Dockerコンテナが稼働中の場合はコンテナ内のファイルを取得
        if (dockerAvailable && isDockerBackendRef(container.docker_id) && container.status === 'running') {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                const exec = await dockerContainer.exec({
                    Cmd: ['bash', '-c', `ls -la --time-style=+%Y-%m-%dT%H:%M:%S "${targetPath}" 2>/dev/null || echo "DIR_NOT_FOUND"`],
                    AttachStdout: true,
                    AttachStderr: true
                });
                
                const stream = await exec.start();
                let output = '';
                
                await new Promise((resolve, reject) => {
                    stream.on('data', (chunk) => { output += chunk.toString(); });
                    stream.on('end', resolve);
                    stream.on('error', reject);
                });
                
                if (output.includes('DIR_NOT_FOUND')) {
                    return res.json({ path: targetPath, items: [] });
                }
                
                const items = parseLsOutput(output);
                return res.json({ path: targetPath, items });
            } catch (dockerError) {
                console.error('Docker file list error:', dockerError);
            }
        }
        
        // フォールバック: ローカルファイルシステム
        const containerDir = path.join(CONFIG.CONTAINERS_DIR, req.params.id);
        const localPath = path.join(containerDir, targetPath.replace(/^\/app\/?/, ''));
        
        if (!localPath.startsWith(containerDir)) {
            return res.status(403).json({ error: 'アクセスが拒否されました' });
        }
        
        if (!fs.existsSync(localPath)) {
            return res.json({ path: targetPath, items: [] });
        }
        
        const items = fs.readdirSync(localPath, { withFileTypes: true })
            .filter(item => !item.name.startsWith('.'))
            .map(item => ({
                name: item.name,
                type: item.isDirectory() ? 'directory' : 'file',
                size: item.isFile() ? fs.statSync(path.join(localPath, item.name)).size : 0,
                modified: fs.statSync(path.join(localPath, item.name)).mtime
            }));
        
        res.json({ path: targetPath, items });
    } catch (e) {
        console.error('Get files error:', e);
        res.status(500).json({ error: 'ファイル一覧の取得に失敗しました' });
    }
});

// Dockerコンテナ内のファイル内容を取得
app.get('/api/containers/:id/files/content', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }

        if (isVmBackendRef(container.docker_id)) {
            const vmCtx = await resolveVmFileContext(container);
            if (!vmCtx.ok) {
                return res.status(vmCtx.status).json({ error: vmCtx.error });
            }
            const pathResolved = resolveVmFilePath(req.query.path || '/app', vmCtx.vmHomeDir, { allowHome: false });
            if (!pathResolved.ok) {
                return res.status(403).json({ error: pathResolved.error });
            }
            const filePath = pathResolved.path;

            const script = `cat -- ${shellSingleQuote(filePath)}`;
            const vmResult = await vmExecBashAsUser(vmCtx.vmName, vmCtx.vmExecUser, script, { timeoutMs: 10000 });
            if (!vmResult.ok) {
                return res.status(404).json({ error: 'ファイルが見つかりません' });
            }
            return res.json({ content: vmResult.stdout });
        }

        const filePath = normalizeAppPath(req.query.path || '/app');
        if (!isPathWithinApp(filePath) || filePath === '/app') {
            return res.status(403).json({ error: 'アクセスが拒否されました (/app 以下のファイルのみ)' });
        }
        
        // Dockerコンテナが稼働中の場合
        if (dockerAvailable && isDockerBackendRef(container.docker_id) && container.status === 'running') {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                const exec = await dockerContainer.exec({
                    Cmd: ['cat', filePath],
                    AttachStdout: true,
                    AttachStderr: true
                });
                
                const stream = await exec.start();
                let content = '';
                
                await new Promise((resolve, reject) => {
                    stream.on('data', (chunk) => { 
                        // バイナリヘッダーを除去
                        const str = chunk.toString();
                        content += str.replace(/[\x00-\x08]/g, '');
                    });
                    stream.on('end', resolve);
                    stream.on('error', reject);
                });
                
                return res.json({ content: content.trim() });
            } catch (dockerError) {
                console.error('Docker file read error:', dockerError);
                return res.status(404).json({ error: 'ファイルが見つかりません' });
            }
        }
        
        // フォールバック
        const containerDir = path.join(CONFIG.CONTAINERS_DIR, req.params.id);
        const localPath = path.join(containerDir, filePath.replace(/^\/app\/?/, ''));
        
        if (!localPath.startsWith(containerDir)) {
            return res.status(403).json({ error: 'アクセスが拒否されました' });
        }
        
        if (!fs.existsSync(localPath) || fs.statSync(localPath).isDirectory()) {
            return res.status(404).json({ error: 'ファイルが見つかりません' });
        }
        
        const content = fs.readFileSync(localPath, 'utf8');
        res.json({ content });
    } catch (e) {
        console.error('Get file content error:', e);
        res.status(500).json({ error: 'ファイル内容の取得に失敗しました' });
    }
});

// Dockerコンテナ内にファイルを保存
app.post('/api/containers/:id/files/content', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        const content = req.body.content || '';

        if (isVmBackendRef(container.docker_id)) {
            const vmCtx = await resolveVmFileContext(container);
            if (!vmCtx.ok) {
                return res.status(vmCtx.status).json({ error: vmCtx.error });
            }
            const pathResolved = resolveVmFilePath(req.body.path || '/app', vmCtx.vmHomeDir, { allowHome: false });
            if (!pathResolved.ok) {
                return res.status(403).json({ error: pathResolved.error });
            }
            const filePath = pathResolved.path;

            const mkdirResult = await vmExecBashAsUser(
                vmCtx.vmName,
                vmCtx.vmExecUser,
                `mkdir -p -- ${shellSingleQuote(path.posix.dirname(filePath))}`
            );
            if (!mkdirResult.ok) {
                return res.status(500).json({ error: `ディレクトリ作成に失敗しました: ${mkdirResult.stderr}` });
            }

            const writeResult = await vmGuestExec(vmCtx.vmName, {
                execPath: '/bin/bash',
                args: ['-lc', buildVmUserScript(vmCtx.vmExecUser, `cat > ${shellSingleQuote(filePath)}`)],
                input: content,
                timeoutMs: 15000
            });
            if (!writeResult.ok) {
                return res.status(500).json({ error: `ファイルの保存に失敗しました: ${writeResult.stderr}` });
            }

            logActivity(req.user.id, req.params.id, 'file_save', { path: filePath, backend: 'vm' }, req.ip);
            return res.json({ success: true, message: 'ファイルを保存しました' });
        }

        const filePath = normalizeAppPath(req.body.path || '/app');
        if (!isPathWithinApp(filePath) || filePath === '/app') {
            return res.status(403).json({ error: 'アクセスが拒否されました (/app 以下のファイルのみ)' });
        }
        
        // Dockerコンテナが稼働中の場合
        if (dockerAvailable && isDockerBackendRef(container.docker_id) && container.status === 'running') {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                
                // ディレクトリが存在しない場合は作成
                const dirPath = path.dirname(filePath);
                if (dirPath && dirPath !== '/' && dirPath !== '.') {
                    const mkdirExec = await dockerContainer.exec({
                        Cmd: ['mkdir', '-p', dirPath],
                        AttachStdout: true,
                        AttachStderr: true
                    });
                    await mkdirExec.start();
                }
                
                // Base64エンコードしてデコードすることで特殊文字に対応
                const base64Content = Buffer.from(content).toString('base64');
                const exec = await dockerContainer.exec({
                    Cmd: ['bash', '-c', `echo '${base64Content}' | base64 -d > '${filePath}'`],
                    AttachStdout: true,
                    AttachStderr: true
                });
                
                await exec.start();
                
                logActivity(req.user.id, req.params.id, 'file_save', { path: filePath }, req.ip);
                return res.json({ success: true, message: 'ファイルを保存しました' });
            } catch (dockerError) {
                console.error('Docker file write error:', dockerError);
                return res.status(500).json({ error: 'ファイルの保存に失敗しました' });
            }
        }
        
        // フォールバック
        const containerDir = path.join(CONFIG.CONTAINERS_DIR, req.params.id);
        const localPath = path.join(containerDir, filePath.replace(/^\/app\/?/, ''));
        
        if (!localPath.startsWith(containerDir)) {
            return res.status(403).json({ error: 'アクセスが拒否されました' });
        }
        
        const dir = path.dirname(localPath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
        
        fs.writeFileSync(localPath, content);
        logActivity(req.user.id, req.params.id, 'file_save', { path: filePath }, req.ip);
        
        res.json({ success: true, message: 'ファイルを保存しました' });
    } catch (e) {
        console.error('Save file error:', e);
        res.status(500).json({ error: 'ファイルの保存に失敗しました' });
    }
});

// 一時ディレクトリにアップロードしてからDockerコンテナにコピー
const tempUpload = multer({ 
    dest: '/tmp/uploads/',
    limits: { fileSize: CONFIG.MAX_FILE_SIZE }
});

app.post('/api/containers/:id/files/upload', authMiddleware, tempUpload.array('files', 10), async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        const uploadedFiles = [];

        if (isVmBackendRef(container.docker_id)) {
            const vmCtx = await resolveVmFileContext(container);
            if (!vmCtx.ok) {
                return res.status(vmCtx.status).json({ error: vmCtx.error });
            }
            const pathResolved = resolveVmFilePath(req.query.path || '/app', vmCtx.vmHomeDir, { allowHome: true });
            if (!pathResolved.ok) {
                return res.status(403).json({ error: pathResolved.error });
            }
            const targetPath = pathResolved.path;

            const mkdirResult = await vmExecBashAsUser(vmCtx.vmName, vmCtx.vmExecUser, `mkdir -p -- ${shellSingleQuote(targetPath)}`);
            if (!mkdirResult.ok) {
                return res.status(500).json({ error: `アップロード先ディレクトリの作成に失敗しました: ${mkdirResult.stderr}` });
            }
            for (const file of req.files || []) {
                try {
                    const destPath = path.posix.normalize(path.posix.join(targetPath, file.originalname));
                    if (!(destPath === vmCtx.vmHomeDir || destPath.startsWith(`${vmCtx.vmHomeDir}/`))) {
                        continue;
                    }

                    const fileBuffer = fs.readFileSync(file.path);
                    const writeResult = await vmGuestExec(vmCtx.vmName, {
                        execPath: '/bin/bash',
                        args: ['-lc', buildVmUserScript(vmCtx.vmExecUser, `cat > ${shellSingleQuote(destPath)}`)],
                        input: fileBuffer,
                        timeoutMs: 20000
                    });
                    if (writeResult.ok) {
                        uploadedFiles.push({ name: file.originalname, size: file.size });
                    }
                } catch (error) {
                    console.error(`Failed to upload ${file.originalname} to VM:`, error);
                } finally {
                    if (fs.existsSync(file.path)) fs.unlinkSync(file.path);
                }
            }
            logActivity(req.user.id, req.params.id, 'file_upload', {
                files: uploadedFiles.map(f => f.name),
                path: targetPath,
                backend: 'vm'
            }, req.ip);
            return res.json({
                success: true,
                message: `${uploadedFiles.length}個のファイルをアップロードしました`,
                files: uploadedFiles
            });
        }

        const targetPath = normalizeAppPath(req.query.path || '/app');
        if (!isPathWithinApp(targetPath)) {
            return res.status(403).json({ error: 'アクセスが拒否されました (/app 以下のみ)' });
        }

        // Dockerコンテナが稼働中の場合
        if (dockerAvailable && isDockerBackendRef(container.docker_id) && container.status === 'running') {
            const dockerContainer = docker.getContainer(container.docker_id);
            
            for (const file of req.files) {
                try {
                    // docker cp でファイルをコンテナにコピー
                    const destPath = `${targetPath}/${file.originalname}`;
                    
                    // ファイル内容を読み込んでBase64エンコード
                    const fileContent = fs.readFileSync(file.path);
                    const base64Content = fileContent.toString('base64');
                    
                    // コンテナ内でBase64デコードしてファイルを作成
                    const exec = await dockerContainer.exec({
                        Cmd: ['bash', '-c', `echo '${base64Content}' | base64 -d > '${destPath}'`],
                        AttachStdout: true,
                        AttachStderr: true
                    });
                    await exec.start();
                    
                    uploadedFiles.push({ name: file.originalname, size: file.size });
                    
                    // 一時ファイルを削除
                    fs.unlinkSync(file.path);
                } catch (err) {
                    console.error(`Failed to upload ${file.originalname}:`, err);
                    // 一時ファイルを削除
                    if (fs.existsSync(file.path)) fs.unlinkSync(file.path);
                }
            }
        } else {
            // フォールバック: ローカルにコピー
            const containerDir = path.join(CONFIG.CONTAINERS_DIR, req.params.id);
            const localTargetPath = path.join(containerDir, targetPath.replace(/^\/app\/?/, ''));
            
            if (!fs.existsSync(localTargetPath)) {
                fs.mkdirSync(localTargetPath, { recursive: true });
            }
            
            for (const file of req.files) {
                const destPath = path.join(localTargetPath, file.originalname);
                fs.renameSync(file.path, destPath);
                uploadedFiles.push({ name: file.originalname, size: file.size });
            }
        }
        
        logActivity(req.user.id, req.params.id, 'file_upload', { 
            files: uploadedFiles.map(f => f.name),
            path: targetPath
        }, req.ip);
        
        res.json({ 
            success: true, 
            message: `${uploadedFiles.length}個のファイルをアップロードしました`,
            files: uploadedFiles
        });
    } catch (e) {
        console.error('Upload error:', e);
        // 一時ファイルをクリーンアップ
        if (req.files) {
            for (const file of req.files) {
                if (fs.existsSync(file.path)) fs.unlinkSync(file.path);
            }
        }
        res.status(500).json({ error: 'アップロードに失敗しました' });
    }
});

app.delete('/api/containers/:id/files', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }

        if (isVmBackendRef(container.docker_id)) {
            const vmCtx = await resolveVmFileContext(container);
            if (!vmCtx.ok) {
                return res.status(vmCtx.status).json({ error: vmCtx.error });
            }
            const pathResolved = resolveVmFilePath(req.body.path || '/app', vmCtx.vmHomeDir, { allowHome: false });
            if (!pathResolved.ok) {
                return res.status(403).json({ error: pathResolved.error });
            }
            const targetPath = pathResolved.path;

            const vmResult = await vmExecBashAsUser(vmCtx.vmName, vmCtx.vmExecUser, `rm -rf -- ${shellSingleQuote(targetPath)}`);
            if (!vmResult.ok) {
                return res.status(500).json({ error: `削除に失敗しました: ${vmResult.stderr}` });
            }

            logActivity(req.user.id, req.params.id, 'file_delete', { path: targetPath, backend: 'vm' }, req.ip);
            return res.json({ success: true, message: '削除しました' });
        }

        const targetPath = normalizeAppPath(req.body.path || '/app');
        // セキュリティチェック: /app 以下のみ削除可能
        if (!targetPath.startsWith('/app/') || targetPath === '/app' || targetPath === '/app/') {
            return res.status(403).json({ error: 'アクセスが拒否されました' });
        }
        
        // Dockerコンテナが稼働中の場合
        if (dockerAvailable && isDockerBackendRef(container.docker_id) && container.status === 'running') {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                const exec = await dockerContainer.exec({
                    Cmd: ['rm', '-rf', targetPath],
                    AttachStdout: true,
                    AttachStderr: true
                });
                await exec.start();
                
                logActivity(req.user.id, req.params.id, 'file_delete', { path: targetPath }, req.ip);
                return res.json({ success: true, message: '削除しました' });
            } catch (dockerError) {
                console.error('Docker delete error:', dockerError);
                return res.status(500).json({ error: '削除に失敗しました' });
            }
        }
        
        // フォールバック: ローカルファイルシステム
        const containerDir = path.join(CONFIG.CONTAINERS_DIR, req.params.id);
        const localPath = path.join(containerDir, targetPath.replace(/^\/app\/?/, ''));
        
        if (!localPath.startsWith(containerDir) || localPath === containerDir) {
            return res.status(403).json({ error: 'アクセスが拒否されました' });
        }
        
        if (fs.existsSync(localPath)) {
            fs.rmSync(localPath, { recursive: true });
        }
        
        logActivity(req.user.id, req.params.id, 'file_delete', { path: targetPath }, req.ip);
        
        res.json({ success: true, message: '削除しました' });
    } catch (e) {
        console.error('Delete file error:', e);
        res.status(500).json({ error: '削除に失敗しました' });
    }
});

// フォルダ作成
app.post('/api/containers/:id/files/mkdir', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }

        if (isVmBackendRef(container.docker_id)) {
            const vmCtx = await resolveVmFileContext(container);
            if (!vmCtx.ok) {
                return res.status(vmCtx.status).json({ error: vmCtx.error });
            }
            const pathResolved = resolveVmFilePath(req.body.path || '/app', vmCtx.vmHomeDir, { allowHome: true });
            if (!pathResolved.ok) {
                return res.status(403).json({ error: pathResolved.error });
            }
            const folderPath = pathResolved.path;

            const vmResult = await vmExecBashAsUser(vmCtx.vmName, vmCtx.vmExecUser, `mkdir -p -- ${shellSingleQuote(folderPath)}`);
            if (!vmResult.ok) {
                return res.status(500).json({ error: `フォルダの作成に失敗しました: ${vmResult.stderr}` });
            }

            return res.json({ success: true, message: 'フォルダを作成しました' });
        }

        const folderPath = normalizeAppPath(req.body.path || '/app');
        // セキュリティチェック: /app 以下のみ作成可能
        if (!folderPath.startsWith('/app/') && folderPath !== '/app') {
            return res.status(403).json({ error: 'アクセスが拒否されました' });
        }
        
        // Dockerコンテナが稼働中の場合
        if (dockerAvailable && isDockerBackendRef(container.docker_id) && container.status === 'running') {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                const exec = await dockerContainer.exec({
                    Cmd: ['mkdir', '-p', folderPath],
                    AttachStdout: true,
                    AttachStderr: true
                });
                await exec.start();
                
                return res.json({ success: true, message: 'フォルダを作成しました' });
            } catch (dockerError) {
                console.error('Docker mkdir error:', dockerError);
                return res.status(500).json({ error: 'フォルダの作成に失敗しました' });
            }
        }
        
        // フォールバック: ローカルファイルシステム
        const containerDir = path.join(CONFIG.CONTAINERS_DIR, req.params.id);
        const localPath = path.join(containerDir, folderPath.replace(/^\/app\/?/, ''));
        
        if (!localPath.startsWith(containerDir)) {
            return res.status(403).json({ error: 'アクセスが拒否されました' });
        }
        
        fs.mkdirSync(localPath, { recursive: true });
        
        res.json({ success: true, message: 'フォルダを作成しました' });
    } catch (e) {
        console.error('Mkdir error:', e);
        res.status(500).json({ error: 'フォルダの作成に失敗しました' });
    }
});

// ============ バックアップAPI ============
app.post('/api/containers/:id/backup', authMiddleware, async (req, res) => {
    try {
        const container = db.prepare(`
            SELECT * FROM containers WHERE id = ? AND user_id = ?
        `).get(req.params.id, req.user.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        
        const containerDir = path.join(CONFIG.CONTAINERS_DIR, req.params.id);
        const backupName = `backup-${req.params.id}-${Date.now()}.tar.gz`;
        const backupPath = path.join(CONFIG.BACKUPS_DIR, backupName);
        
        const { execSync } = await import('child_process');
        execSync(`tar -czf "${backupPath}" -C "${containerDir}" .`);
        
        logActivity(req.user.id, req.params.id, 'backup_create', { backupName }, req.ip);
        
        res.json({ success: true, message: 'バックアップを作成しました', backupName });
    } catch (e) {
        console.error('Backup error:', e);
        res.status(500).json({ error: 'バックアップの作成に失敗しました' });
    }
});

app.get('/api/containers/:id/backups', authMiddleware, (req, res) => {
    try {
        const files = fs.readdirSync(CONFIG.BACKUPS_DIR)
            .filter(f => f.includes(req.params.id))
            .map(f => ({
                name: f,
                size: fs.statSync(path.join(CONFIG.BACKUPS_DIR, f)).size,
                created: fs.statSync(path.join(CONFIG.BACKUPS_DIR, f)).mtime
            }))
            .sort((a, b) => new Date(b.created) - new Date(a.created));
        
        res.json(files);
    } catch (e) {
        console.error('Get backups error:', e);
        res.status(500).json({ error: 'バックアップ一覧の取得に失敗しました' });
    }
});

// ============ 統計API ============
app.get('/api/stats', authMiddleware, (req, res) => {
    const containers = db.prepare(`
        SELECT * FROM containers WHERE user_id = ?
    `).all(req.user.id);
    
    const running = containers.filter(c => c.status === 'running').length;
    const stopped = containers.filter(c => c.status === 'stopped').length;
    const limits = getEffectiveUserLimits(req.user);
    
    res.json({
        totalContainers: containers.length,
        maxContainers: limits.maxContainers,
        running,
        stopped,
        totalCpu: containers.reduce((acc, c) => acc + (c.status === 'running' ? c.cpu_limit : 0), 0),
        totalMemory: containers.reduce((acc, c) => acc + (c.status === 'running' ? c.memory_limit : 0), 0),
        plan: req.user.plan,
        dockerAvailable
    });
});

// ============ プラン情報 ============
app.get('/api/plans', (req, res) => {
    res.json(Object.entries(CONFIG.PLANS).map(([id, plan]) => ({
        id,
        name: id.charAt(0).toUpperCase() + id.slice(1),
        price: plan.price,
        cpu: `${plan.cpu} vCPU`,
        memory: `${plan.memory}MB`,
        storage: `${plan.storage}MB`,
        containers: plan.containers === -1 ? '無制限' : `${plan.containers}個`
    })));
});

// ============ Billing (Stripe) ============
const PLAN_ORDER = ['free', 'standard', 'pro'];

const getStripePriceIdForPlan = (planId) => {
    if (planId === 'standard') return CONFIG.STRIPE_PRICE_STANDARD;
    if (planId === 'pro') return CONFIG.STRIPE_PRICE_PRO;
    return '';
};

app.post('/api/billing/checkout-session', authMiddleware, async (req, res) => {
    try {
        const requestedPlan = String(req.body?.plan || '').trim().toLowerCase();
        if (!['standard', 'pro'].includes(requestedPlan)) {
            return res.status(400).json({ error: 'アップグレード対象プランが不正です' });
        }

        const currentRank = PLAN_ORDER.indexOf(req.user.plan || 'free');
        const requestedRank = PLAN_ORDER.indexOf(requestedPlan);
        if (requestedRank <= currentRank) {
            return res.status(400).json({ error: '現在のプランより上位のプランを選択してください' });
        }

        if (!stripe) {
            return res.status(501).json({ error: 'Stripeが設定されていません' });
        }

        const priceId = getStripePriceIdForPlan(requestedPlan);
        if (!priceId) {
            return res.status(500).json({ error: '対象プランのStripe Price IDが設定されていません' });
        }

        const customerId = String(req.user.stripe_customer_id || '').trim();
        const hasStandardCoupon = requestedPlan === 'standard' && !!CONFIG.STRIPE_STANDARD_COUPON_ID;
        const session = await stripe.checkout.sessions.create({
            ...(hasStandardCoupon ? { discounts: [{ coupon: CONFIG.STRIPE_STANDARD_COUPON_ID }] } : {}),
            mode: 'subscription',
            payment_method_types: ['card'],
            line_items: [{ price: priceId, quantity: 1 }],
            client_reference_id: req.user.id,
            ...(customerId ? { customer: customerId } : { customer_email: req.user.email || undefined }),
            ...(hasStandardCoupon ? {} : { allow_promotion_codes: true }),
            metadata: {
                user_id: req.user.id,
                target_plan: requestedPlan
            },
            success_url: `${CONFIG.RP_ORIGIN}/dashboard.html?billing=success&session_id={CHECKOUT_SESSION_ID}`,
            cancel_url: `${CONFIG.RP_ORIGIN}/dashboard.html?billing=cancel`
        });

        logActivity(req.user.id, null, 'billing_checkout_create', {
            plan: requestedPlan,
            session_id: session.id
        }, req.ip);

        res.json({ success: true, url: session.url, session_id: session.id });
    } catch (e) {
        console.error('Create checkout session error:', e);
        res.status(500).json({ error: 'チェックアウトの作成に失敗しました' });
    }
});

app.post('/api/billing/portal-session', authMiddleware, async (req, res) => {
    try {
        if (!stripe) {
            return res.status(501).json({ error: 'Stripeが設定されていません' });
        }

        let customerId = String(req.user.stripe_customer_id || '').trim();
        if (!customerId && req.user.email) {
            const customerList = await stripe.customers.list({
                email: req.user.email,
                limit: 10
            });
            customerId = customerList.data[0]?.id || '';
        }

        if (!customerId) {
            return res.status(400).json({ error: 'サブスクリプション情報が見つかりません。先にアップグレードを実行してください。' });
        }

        const portalSession = await stripe.billingPortal.sessions.create({
            customer: customerId,
            return_url: `${CONFIG.RP_ORIGIN}/dashboard.html`
        });

        db.prepare(`
            UPDATE users
            SET stripe_customer_id = COALESCE(?, stripe_customer_id),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        `).run(customerId, req.user.id);

        logActivity(req.user.id, null, 'billing_portal_open', { customer_id: customerId }, req.ip);

        res.json({ success: true, url: portalSession.url });
    } catch (e) {
        console.error('Create billing portal session error:', e);
        res.status(500).json({ error: 'サブスク管理ページの作成に失敗しました' });
    }
});

app.post('/api/billing/confirm', authMiddleware, async (req, res) => {
    try {
        const sessionId = String(req.body?.session_id || '').trim();
        if (!sessionId) {
            return res.status(400).json({ error: 'session_id が必要です' });
        }
        if (!stripe) {
            return res.status(501).json({ error: 'Stripeが設定されていません' });
        }

        const session = await stripe.checkout.sessions.retrieve(sessionId, {
            expand: ['subscription']
        });
        if (!session) {
            return res.status(404).json({ error: '決済セッションが見つかりません' });
        }

        const ownerUserId = session.client_reference_id || session.metadata?.user_id;
        if (!ownerUserId || ownerUserId !== req.user.id) {
            return res.status(403).json({ error: 'この決済セッションへのアクセス権がありません' });
        }

        const paid = session.payment_status === 'paid' || session.status === 'complete';
        if (!paid) {
            return res.status(400).json({ error: '決済が完了していません' });
        }

        const targetPlan = String(session.metadata?.target_plan || '').toLowerCase();
        if (!CONFIG.PLANS[targetPlan]) {
            return res.status(400).json({ error: '決済データのプラン情報が不正です' });
        }

        const limits = await applyUserPlanAndLimits({
            userId: req.user.id,
            planId: targetPlan,
            actorUserId: req.user.id,
            ip: req.ip,
            activity: 'billing_upgrade_complete'
        });

        const stripeCustomerId = typeof session.customer === 'string'
            ? session.customer
            : (session.customer?.id || null);
        const stripeSubscriptionId = typeof session.subscription === 'string'
            ? session.subscription
            : (session.subscription?.id || null);

        db.prepare(`
            UPDATE users
            SET stripe_customer_id = COALESCE(?, stripe_customer_id),
                stripe_subscription_id = COALESCE(?, stripe_subscription_id),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        `).run(stripeCustomerId, stripeSubscriptionId, req.user.id);

        res.json({
            success: true,
            message: `${targetPlan} プランへアップグレードしました`,
            plan: limits.planId,
            limits
        });
    } catch (e) {
        console.error('Confirm billing session error:', e);
        res.status(500).json({ error: '決済確認に失敗しました' });
    }
});

app.post('/api/billing/reapply-limits', authMiddleware, async (req, res) => {
    try {
        const limits = getEffectiveUserLimits(req.user);
        await syncContainersToPlanLimits(req.user.id, {
            planId: req.user.plan || 'free',
            ...limits
        });

        logActivity(req.user.id, null, 'plan_limits_reapply', {
            plan: req.user.plan || 'free',
            limits
        }, req.ip);

        res.json({
            success: true,
            message: '既存サーバーへプラン値を再適用しました',
            limits
        });
    } catch (e) {
        console.error('Reapply plan limits error:', e);
        res.status(500).json({ error: 'プラン値の再適用に失敗しました' });
    }
});

// ============ 管理者API ============
app.get('/api/admin/users', authMiddleware, adminMiddleware, (req, res) => {
    const users = db.prepare(`
        SELECT id, discord_id, email, name, avatar, plan, is_admin, 
               max_containers, max_cpu, max_memory, max_storage, created_at 
        FROM users ORDER BY created_at DESC
    `).all();
    
    // 各ユーザーのコンテナ数を取得
    const usersWithStats = users.map(user => {
        const containerCount = db.prepare('SELECT COUNT(*) as count FROM containers WHERE user_id = ?').get(user.id).count;
        return {
            ...user,
            container_count: containerCount
        };
    });
    
    res.json(usersWithStats);
});

app.get('/api/admin/containers', authMiddleware, adminMiddleware, async (req, res) => {
    const containers = db.prepare(`
        SELECT c.*, u.email as user_email, u.name as user_name, u.discord_id as user_discord_id
        FROM containers c
        JOIN users u ON c.user_id = u.id
        ORDER BY c.created_at DESC
    `).all();
    
    // リアルタイムリソース情報を取得
    const containersWithResources = await Promise.all(containers.map(async (c) => {
        let realStatus = c.status;
        let cpuUsage = 0;
        let memoryUsage = 0;
        let networkIn = '0 B';
        let networkOut = '0 B';
        
        if (docker && isDockerBackendRef(c.docker_id)) {
            try {
                const container = docker.getContainer(c.docker_id);
                const info = await container.inspect();
                realStatus = info.State.Running ? 'running' : 'stopped';
                
                if (info.State.Running) {
                    const stats = await getContainerStats(c.docker_id);
                    cpuUsage = stats.cpuUsage || 0;
                    memoryUsage = stats.memoryUsage || 0;
                    networkIn = formatBytes(stats.networkIn || 0);
                    networkOut = formatBytes(stats.networkOut || 0);
                }
                
                // DBのステータスを更新
                if (realStatus !== c.status) {
                    db.prepare('UPDATE containers SET status = ? WHERE id = ?').run(realStatus, c.id);
                }
            } catch (e) {
                // コンテナが存在しない場合
            }
        }
        
        return {
            ...c,
            status: realStatus,
            cpu_usage: cpuUsage,
            memory_usage: memoryUsage,
            network_in: networkIn,
            network_out: networkOut
        };
    }));
    
    res.json(containersWithResources);
});

app.get('/api/admin/logs', authMiddleware, adminMiddleware, (req, res) => {
    const logs = db.prepare(`
        SELECT * FROM activity_logs ORDER BY created_at DESC LIMIT 100
    `).all();
    res.json(logs);
});

// ユーザー制限更新API
app.put('/api/admin/users/:id/limits', authMiddleware, adminMiddleware, async (req, res) => {
    const { max_containers, max_cpu, max_memory, max_storage, is_admin } = req.body;
    
    const user = db.prepare('SELECT * FROM users WHERE id = ?').get(req.params.id);
    if (!user) {
        return res.status(404).json({ error: 'ユーザーが見つかりません' });
    }
    
    db.prepare(`
        UPDATE users SET 
            max_containers = ?,
            max_cpu = ?,
            max_memory = ?,
            max_storage = ?,
            is_admin = ?,
            updated_at = CURRENT_TIMESTAMP 
        WHERE id = ?
    `).run(
        max_containers ?? user.max_containers,
        max_cpu ?? user.max_cpu,
        max_memory ?? user.max_memory,
        max_storage ?? user.max_storage,
        is_admin ?? user.is_admin,
        req.params.id
    );

    const updatedUser = db.prepare('SELECT * FROM users WHERE id = ?').get(req.params.id);
    const updatedLimits = getEffectiveUserLimits(updatedUser);
    await syncContainersToPlanLimits(req.params.id, {
        planId: updatedUser.plan || 'free',
        ...updatedLimits
    });
    
    logActivity(req.user.id, null, 'admin_update_user', { 
        target_user: req.params.id, 
        max_containers, max_cpu, max_memory, max_storage, is_admin 
    }, req.ip);
    
    res.json({
        success: true,
        message: 'ユーザー設定を更新しました',
        limits: updatedLimits
    });
});

// ユーザー削除API
app.delete('/api/admin/users/:id', authMiddleware, adminMiddleware, async (req, res) => {
    try {
        const user = db.prepare('SELECT * FROM users WHERE id = ?').get(req.params.id);
        if (!user) {
            return res.status(404).json({ error: 'ユーザーが見つかりません' });
        }
        
        // 自分自身は削除不可
        if (user.id === req.user.id) {
            return res.status(400).json({ error: '自分自身は削除できません' });
        }
        
        // ユーザーのコンテナを全て削除
        const containers = db.prepare('SELECT * FROM containers WHERE user_id = ?').all(user.id);
        for (const container of containers) {
            if (dockerAvailable && isDockerBackendRef(container.docker_id)) {
                try {
                    const dockerContainer = docker.getContainer(container.docker_id);
                    await dockerContainer.stop({ t: 5 }).catch(() => {});
                    await dockerContainer.remove({ force: true });
                } catch (e) {}
            }
            // コンテナディレクトリ削除
            const containerDir = path.join(CONFIG.CONTAINERS_DIR, container.id);
            if (fs.existsSync(containerDir)) {
                fs.rmSync(containerDir, { recursive: true, force: true });
            }
        }
        
        // DB削除
        db.prepare('DELETE FROM containers WHERE user_id = ?').run(user.id);
        db.prepare('DELETE FROM activity_logs WHERE user_id = ?').run(user.id);
        db.prepare('DELETE FROM users WHERE id = ?').run(user.id);
        
        logActivity(req.user.id, null, 'admin_delete_user', { deleted_user: user.id, email: user.email }, req.ip);
        
        res.json({ success: true, message: 'ユーザーを削除しました' });
    } catch (e) {
        console.error('Delete user error:', e);
        res.status(500).json({ error: 'ユーザーの削除に失敗しました' });
    }
});

// 統計情報API
app.get('/api/admin/stats', authMiddleware, adminMiddleware, (req, res) => {
    const totalUsers = db.prepare('SELECT COUNT(*) as count FROM users').get().count;
    const totalContainers = db.prepare('SELECT COUNT(*) as count FROM containers').get().count;
    const runningContainers = db.prepare("SELECT COUNT(*) as count FROM containers WHERE status = 'running'").get().count;
    
    res.json({
        totalUsers,
        totalContainers,
        runningContainers,
        stoppedContainers: totalContainers - runningContainers
    });
});

// ============ 管理者用サーバー操作API ============

// 管理者用: サーバー起動
app.post('/api/admin/containers/:id/start', authMiddleware, adminMiddleware, async (req, res) => {
    try {
        const container = db.prepare('SELECT * FROM containers WHERE id = ?').get(req.params.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        
        if (dockerAvailable && isDockerBackendRef(container.docker_id)) {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                await dockerContainer.start();
                
                // 起動コマンドがあれば実行
                if (container.startup_command) {
                    setTimeout(async () => {
                        try {
                            const exec = await dockerContainer.exec({
                                Cmd: ['bash', '-c', `cd /home/user/app && ${container.startup_command} &`],
                                User: 'root'
                            });
                            await exec.start();
                        } catch (e) {}
                    }, 2000);
                }
            } catch (e) {
                console.error('Admin Docker start error:', e);
            }
        }
        
        db.prepare(`
            UPDATE containers SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = ?
        `).run(req.params.id);
        
        logActivity(req.user.id, req.params.id, 'admin_container_start', { container_name: container.name }, req.ip);
        
        res.json({ success: true, message: 'サーバーを起動しました' });
    } catch (e) {
        console.error('Admin start container error:', e);
        res.status(500).json({ error: 'サーバーの起動に失敗しました' });
    }
});

// 管理者用: サーバー停止
app.post('/api/admin/containers/:id/stop', authMiddleware, adminMiddleware, async (req, res) => {
    try {
        const container = db.prepare('SELECT * FROM containers WHERE id = ?').get(req.params.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        
        if (dockerAvailable && isDockerBackendRef(container.docker_id)) {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                await dockerContainer.stop({ t: 10 });
            } catch (e) {
                console.error('Admin Docker stop error:', e);
            }
        }
        
        db.prepare(`
            UPDATE containers SET status = 'stopped', updated_at = CURRENT_TIMESTAMP WHERE id = ?
        `).run(req.params.id);
        
        logActivity(req.user.id, req.params.id, 'admin_container_stop', { container_name: container.name }, req.ip);
        
        res.json({ success: true, message: 'サーバーを停止しました' });
    } catch (e) {
        console.error('Admin stop container error:', e);
        res.status(500).json({ error: 'サーバーの停止に失敗しました' });
    }
});

// 管理者用: サーバー再起動
app.post('/api/admin/containers/:id/restart', authMiddleware, adminMiddleware, async (req, res) => {
    try {
        const container = db.prepare('SELECT * FROM containers WHERE id = ?').get(req.params.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        
        if (dockerAvailable && isDockerBackendRef(container.docker_id)) {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                await dockerContainer.restart({ t: 10 });
                
                // 起動コマンドがあれば実行
                if (container.startup_command) {
                    setTimeout(async () => {
                        try {
                            const exec = await dockerContainer.exec({
                                Cmd: ['bash', '-c', `cd /home/user/app && ${container.startup_command} &`],
                                User: 'root'
                            });
                            await exec.start();
                        } catch (e) {}
                    }, 2000);
                }
            } catch (e) {
                console.error('Admin Docker restart error:', e);
            }
        }
        
        db.prepare(`
            UPDATE containers SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE id = ?
        `).run(req.params.id);
        
        logActivity(req.user.id, req.params.id, 'admin_container_restart', { container_name: container.name }, req.ip);
        
        res.json({ success: true, message: 'サーバーを再起動しました' });
    } catch (e) {
        console.error('Admin restart container error:', e);
        res.status(500).json({ error: 'サーバーの再起動に失敗しました' });
    }
});

// 管理者用: サーバー削除
app.delete('/api/admin/containers/:id', authMiddleware, adminMiddleware, async (req, res) => {
    try {
        const container = db.prepare('SELECT * FROM containers WHERE id = ?').get(req.params.id);
        
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        
        if (dockerAvailable && isDockerBackendRef(container.docker_id)) {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                await dockerContainer.stop().catch(() => {});
                await dockerContainer.remove({ force: true });
            } catch (e) {
                console.error('Admin Docker remove error:', e);
            }
        }
        
        // ファイル削除
        const containerDir = path.join(CONFIG.CONTAINERS_DIR, req.params.id);
        if (fs.existsSync(containerDir)) {
            fs.rmSync(containerDir, { recursive: true });
        }
        
        db.prepare('DELETE FROM container_stats WHERE container_id = ?').run(req.params.id);
        db.prepare('DELETE FROM containers WHERE id = ?').run(req.params.id);
        
        logActivity(req.user.id, req.params.id, 'admin_container_delete', { container_name: container.name }, req.ip);
        
        res.json({ success: true, message: 'サーバーを削除しました' });
    } catch (e) {
        console.error('Admin delete container error:', e);
        res.status(500).json({ error: 'サーバーの削除に失敗しました' });
    }
});

// 管理者用: サーバーのリソース制限を更新
app.put('/api/admin/containers/:id/limits', authMiddleware, adminMiddleware, async (req, res) => {
    try {
        const { cpu_limit, memory_limit, storage_limit } = req.body;
        
        const container = db.prepare('SELECT * FROM containers WHERE id = ?').get(req.params.id);
        if (!container) {
            return res.status(404).json({ error: 'サーバーが見つかりません' });
        }
        
        // 値の検証
        const newCpuLimit = parseFloat(cpu_limit) || container.cpu_limit;
        const newMemoryLimit = parseInt(memory_limit) || container.memory_limit;
        const newStorageLimit = parseInt(storage_limit) || container.storage_limit;
        
        if (newCpuLimit < 0.1 || newCpuLimit > 16) {
            return res.status(400).json({ error: 'CPU制限は0.1〜16の範囲で指定してください' });
        }
        if (newMemoryLimit < 128 || newMemoryLimit > 65536) {
            return res.status(400).json({ error: 'メモリ制限は128〜65536MBの範囲で指定してください' });
        }
        if (newStorageLimit < 512 || newStorageLimit > 102400) {
            return res.status(400).json({ error: 'ストレージ制限は512〜102400MBの範囲で指定してください' });
        }
        
        // Dockerコンテナのリソース制限を更新
        if (dockerAvailable && isDockerBackendRef(container.docker_id)) {
            try {
                const dockerContainer = docker.getContainer(container.docker_id);
                await dockerContainer.update({
                    CpuQuota: Math.floor(newCpuLimit * 100000),
                    CpuPeriod: 100000,
                    Memory: newMemoryLimit * 1024 * 1024,
                    MemorySwap: newMemoryLimit * 1024 * 1024 * 2
                });
            } catch (e) {
                console.error('Docker update error:', e);
                // Docker更新に失敗してもDB更新は続行
            }
        }
        
        // DB更新
        db.prepare(`
            UPDATE containers 
            SET cpu_limit = ?, memory_limit = ?, storage_limit = ?, updated_at = CURRENT_TIMESTAMP 
            WHERE id = ?
        `).run(newCpuLimit, newMemoryLimit, newStorageLimit, req.params.id);
        
        logActivity(req.user.id, req.params.id, 'admin_container_update_limits', { 
            container_name: container.name,
            cpu_limit: newCpuLimit,
            memory_limit: newMemoryLimit,
            storage_limit: newStorageLimit
        }, req.ip);
        
        res.json({ 
            success: true, 
            message: 'サーバーのリソース制限を更新しました',
            limits: { cpu_limit: newCpuLimit, memory_limit: newMemoryLimit, storage_limit: newStorageLimit }
        });
    } catch (e) {
        console.error('Admin update container limits error:', e);
        res.status(500).json({ error: 'リソース制限の更新に失敗しました' });
    }
});

// 旧APIは互換性のため残す（内部的には上限値も同期）
app.put('/api/admin/users/:id/plan', authMiddleware, adminMiddleware, async (req, res) => {
    try {
        const { plan } = req.body;
        if (!CONFIG.PLANS[plan]) {
            return res.status(400).json({ error: '無効なプランです' });
        }

        const targetUser = db.prepare('SELECT id FROM users WHERE id = ?').get(req.params.id);
        if (!targetUser) {
            return res.status(404).json({ error: 'ユーザーが見つかりません' });
        }

        const limits = await applyUserPlanAndLimits({
            userId: req.params.id,
            planId: plan,
            actorUserId: req.user.id,
            ip: req.ip,
            activity: 'admin_update_user_plan'
        });

        res.json({ success: true, message: 'プランを更新しました', limits });
    } catch (e) {
        console.error('Admin update user plan error:', e);
        res.status(500).json({ error: 'プラン更新に失敗しました' });
    }
});

// ============ WebSocket (ターミナル) ============
const wsClients = new Map();

wss.on('connection', async (ws, req) => {
    const url = new URL(req.url, 'http://localhost');
    const token = url.searchParams.get('token');
    const containerId = url.searchParams.get('container');
    
    try {
        const decoded = jwt.verify(token, CONFIG.JWT_SECRET);
        const user = db.prepare('SELECT * FROM users WHERE id = ?').get(decoded.userId);
        
        if (!user) {
            ws.close(4001, 'Unauthorized');
            return;
        }
        
        ws.userId = user.id;
        ws.isAlive = true;
        
        // コンテナ接続
        if (containerId) {
            const container = db.prepare(`
                SELECT * FROM containers WHERE id = ? AND user_id = ?
            `).get(containerId, user.id);
            
            if (!container) {
                ws.close(4002, 'Container not found');
                return;
            }
            
            ws.containerId = containerId;
            
            // Docker exec ストリーム
            if (dockerAvailable && isDockerBackendRef(container.docker_id)) {
                try {
                    const dockerContainer = docker.getContainer(container.docker_id);
                    
                    // コンテナ状態確認
                    const inspect = await dockerContainer.inspect();
                    if (!inspect.State.Running) {
                        await dockerContainer.start();
                    }
                    
                    const exec = await dockerContainer.exec({
                        Cmd: ['/bin/bash'],
                        AttachStdin: true,
                        AttachStdout: true,
                        AttachStderr: true,
                        Tty: true,
                        User: 'root',
                        WorkingDir: '/root'
                    });
                    
                    const stream = await exec.start({ hijack: true, stdin: true });
                    
                    ws.dockerStream = stream;
                    
                    stream.on('data', (data) => {
                        if (ws.readyState === ws.OPEN) {
                            ws.send(JSON.stringify({ type: 'output', data: data.toString() }));
                        }
                    });
                    
                    stream.on('end', () => {
                        if (ws.readyState === ws.OPEN) {
                            ws.send(JSON.stringify({ type: 'disconnect', message: 'Connection closed' }));
                        }
                    });
                    
                    ws.send(JSON.stringify({ type: 'connected', message: 'Terminal connected' }));
                    
                } catch (e) {
                    console.error('Docker exec error:', e);
                    if (e.statusCode === 404) {
                        // コンテナが存在しない
                        ws.send(JSON.stringify({ 
                            type: 'error', 
                            message: 'コンテナが存在しません。ダッシュボードから「起動」ボタンを押して再作成してください。',
                            action: 'restart_required'
                        }));
                        // DBを更新
                        db.prepare('UPDATE containers SET docker_id = NULL, status = ? WHERE id = ?')
                            .run('stopped', containerId);
                    } else {
                        ws.send(JSON.stringify({ type: 'error', message: 'ターミナル接続に失敗しました: ' + e.message }));
                    }
                }
            } else if (isVmBackendRef(container.docker_id)) {
                const vmName = container.docker_id.replace(/^vm:/, '');
                const vmInfo = await getVmBackendInfo(vmName);
                if (!vmInfo?.available) {
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: `VM状態の取得に失敗しました: ${vmName}`
                    }));
                } else if (vmInfo.status !== 'running') {
                    ws.send(JSON.stringify({
                        type: 'error',
                        message: 'VMが停止中です。先に起動してください。'
                    }));
                } else {
                    const preferredUser = String(container.ssh_user || '').trim();
                    let vmExecUser = preferredUser;
                    if (!isValidLinuxUserName(vmExecUser) || !(await vmLinuxUserExists(vmName, vmExecUser))) {
                        vmExecUser = 'root';
                    }

                    ws.vmMode = true;
                    ws.vmName = vmName;
                    ws.vmInputBuffer = '';
                    ws.vmQueue = Promise.resolve();
                    ws.vmSshUser = vmExecUser;
                    ws.vmExecUser = vmExecUser;
                    ws.vmHomeDir = await resolveVmUserHomeDirectory(vmName, vmExecUser);
                    ws.vmCwd = ws.vmHomeDir;

                    const initialCwdResult = await vmExecBashAsUser(
                        vmName,
                        vmExecUser,
                        `cd ${shellSingleQuote(ws.vmHomeDir)} 2>/dev/null && pwd || pwd`,
                        { timeoutMs: 10000 }
                    );
                    if (initialCwdResult.ok) {
                        ws.vmCwd = (initialCwdResult.stdout || '')
                            .trim()
                            .split('\n')
                            .filter(Boolean)
                            .pop() || ws.vmHomeDir;
                    }

                    ws.send(JSON.stringify({ type: 'connected', message: `VM terminal connected as ${ws.vmSshUser}` }));
                    ws.send(JSON.stringify({ type: 'output', data: `Connected to VM: ${vmName}\n` }));
                    ws.send(JSON.stringify({ type: 'output', data: `${formatVmPrompt(ws.vmSshUser, ws.vmCwd)}` }));
                }
            } else if (dockerAvailable && !container.docker_id) {
                // docker_idがない（コンテナが削除されている）
                ws.send(JSON.stringify({ 
                    type: 'error', 
                    message: 'コンテナが存在しません。サーバー一覧から「起動」ボタンを押して再作成してください。',
                    action: 'restart_required'
                }));
            } else {
                ws.send(JSON.stringify({ type: 'info', message: 'Terminal simulation mode (Docker not available)' }));
            }
        }
        
        ws.on('pong', () => { ws.isAlive = true; });
        
        ws.on('message', async (message) => {
            try {
                const data = JSON.parse(message);
                
                if (data.type === 'input' && ws.dockerStream) {
                    ws.dockerStream.write(data.data);
                } else if (data.type === 'input' && ws.vmMode && ws.vmName) {
                    const incoming = String(data.data || '').replace(/\r/g, '');
                    if (!incoming) return;

                    if (incoming.includes('\u0003')) {
                        ws.vmInputBuffer = '';
                        if (ws.readyState === ws.OPEN) {
                            ws.send(JSON.stringify({ type: 'output', data: '^C\n' }));
                            ws.send(JSON.stringify({ type: 'output', data: `${formatVmPrompt(ws.vmSshUser, ws.vmCwd)}` }));
                        }
                        return;
                    }

                    if (incoming.includes('\u007f') && ws.vmInputBuffer.length > 0) {
                        ws.vmInputBuffer = ws.vmInputBuffer.slice(0, -1);
                        return;
                    }

                    ws.vmInputBuffer += incoming;
                    const commands = ws.vmInputBuffer.split('\n');
                    ws.vmInputBuffer = commands.pop() || '';

                    for (const rawLine of commands) {
                        const line = String(rawLine || '').trim();
                        ws.vmQueue = ws.vmQueue.then(async () => {
                            if (ws.readyState !== ws.OPEN) return;
                            if (!line) {
                                ws.send(JSON.stringify({ type: 'output', data: `${formatVmPrompt(ws.vmSshUser, ws.vmCwd)}` }));
                                return;
                            }

                            ws.send(JSON.stringify({ type: 'output', data: `${line}\n` }));

                            if (line === 'clear') {
                                ws.send(JSON.stringify({ type: 'output', data: '\u001bc' }));
                                ws.send(JSON.stringify({ type: 'output', data: `${formatVmPrompt(ws.vmSshUser, ws.vmCwd)}` }));
                                return;
                            }

                            if (line === 'pwd') {
                                ws.send(JSON.stringify({ type: 'output', data: `${ws.vmCwd}\n` }));
                                ws.send(JSON.stringify({ type: 'output', data: `${formatVmPrompt(ws.vmSshUser, ws.vmCwd)}` }));
                                return;
                            }

                            if (line === 'cd' || line.startsWith('cd ')) {
                                const target = line === 'cd' ? (ws.vmHomeDir || '/root') : line.slice(2).trim();
                                const resolved = await resolveVmCwd(
                                    ws.vmName,
                                    ws.vmCwd,
                                    target,
                                    ws.vmExecUser || 'root',
                                    ws.vmHomeDir || '/root'
                                );
                                if (!resolved.ok && resolved.error) {
                                    ws.send(JSON.stringify({ type: 'output', data: `${resolved.error}\n` }));
                                } else {
                                    ws.vmCwd = resolved.cwd;
                                }
                                ws.send(JSON.stringify({ type: 'output', data: `${formatVmPrompt(ws.vmSshUser, ws.vmCwd)}` }));
                                return;
                            }

                            const commandScript = `cd ${shellSingleQuote(ws.vmCwd)} 2>/dev/null || cd ${shellSingleQuote(ws.vmHomeDir || '/root')} 2>/dev/null || cd /; ${line}`;
                            const result = await vmExecBashAsUser(
                                ws.vmName,
                                ws.vmExecUser || 'root',
                                commandScript,
                                { timeoutMs: 30000 }
                            );
                            const output = `${result.stdout || ''}${result.stderr || ''}`;
                            if (output) {
                                ws.send(JSON.stringify({ type: 'output', data: output.endsWith('\n') ? output : `${output}\n` }));
                            }
                            ws.send(JSON.stringify({ type: 'output', data: `${formatVmPrompt(ws.vmSshUser, ws.vmCwd)}` }));
                        }).catch((error) => {
                            if (ws.readyState === ws.OPEN) {
                                ws.send(JSON.stringify({ type: 'error', message: `VM command error: ${error.message}` }));
                            }
                        });
                    }
                } else if (data.type === 'resize' && (ws.dockerStream || ws.vmMode)) {
                    // ターミナルリサイズ（将来実装）
                } else if (data.type === 'command' && !ws.dockerStream && !ws.vmMode) {
                    // シミュレーションモード
                    ws.send(JSON.stringify({ 
                        type: 'output', 
                        data: `$ ${data.command}\n[シミュレーション] Docker環境では実際に実行されます。\n` 
                    }));
                }
            } catch (e) {
                console.error('WebSocket message error:', e);
            }
        });
        
        ws.on('close', () => {
            if (ws.dockerStream) {
                ws.dockerStream.end();
            }
        });
        
    } catch (e) {
        ws.close(4001, 'Unauthorized');
    }
});

// ヘルスチェック
const interval = setInterval(() => {
    wss.clients.forEach(ws => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('close', () => clearInterval(interval));

// SPA用フォールバック
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, '../frontend/public/index.html'));
});

// ============ コンテナ状態同期 ============
async function syncContainerStates() {
    if (!dockerAvailable) return;
    
    console.log('🔄 Syncing container states with Docker...');
    
    const containers = db.prepare('SELECT * FROM containers WHERE docker_id IS NOT NULL').all();
    
    for (const container of containers) {
        if (isVmBackendRef(container.docker_id)) {
            const vmName = container.docker_id.replace(/^vm:/, '');
            const vmInfo = await getVmBackendInfo(vmName);
            if (!vmInfo?.status) {
                continue;
            }
            // VM backend entries are managed by libvirt, not Docker.
            if (container.status !== vmInfo.status) {
                db.prepare('UPDATE containers SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?')
                    .run(vmInfo.status, container.id);
            }
            continue;
        }

        try {
            const dockerContainer = docker.getContainer(container.docker_id);
            const inspect = await dockerContainer.inspect();
            
            const newStatus = inspect.State.Running ? 'running' : 'stopped';
            if (container.status !== newStatus) {
                db.prepare('UPDATE containers SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?')
                    .run(newStatus, container.id);
                console.log(`  ✅ ${container.name}: status updated to ${newStatus}`);
            }
        } catch (e) {
            if (e.statusCode === 404) {
                // コンテナが存在しない場合、停止状態にマークし、docker_idをクリア
                db.prepare('UPDATE containers SET status = ?, docker_id = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?')
                    .run('stopped', container.id);
                console.log(`  ⚠️  ${container.name}: container deleted, marked as stopped`);
            } else {
                console.error(`  ❌ ${container.name}: error checking status - ${e.message}`);
            }
        }
    }
    
    console.log('✅ Container state sync completed');
}

// サーバー起動
server.listen(CONFIG.PORT, '0.0.0.0', async () => {
    // コンテナ状態を同期
    await syncContainerStates();
    // 既存サーバーのスペックをユーザー上限へ同期
    await syncAllContainersToUserLimits();
    console.log(`
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   💻 LenovoServer v2.2 - Ubuntu Based                        ║
║                                                              ║
║   URL:     http://localhost:${CONFIG.PORT}                         ║
║   Docker:  ${dockerAvailable ? '✅ Connected' : '⚠️  Simulation Mode'}                              ║
║   Base:    Ubuntu 22.04 LTS                                  ║
║   Auth:    Discord OAuth                                     ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
    `);
});

// グレースフルシャットダウン
process.on('SIGTERM', () => {
    console.log('Shutting down...');
    db.close();
    server.close();
    process.exit(0);
});
