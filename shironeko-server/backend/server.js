import express from 'express';
import cors from 'cors';
import WebSocket, { WebSocketServer } from 'ws';
import http from 'http';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import { v4 as uuidv4 } from 'uuid';
import Docker from 'dockerode';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import os from 'os';
import multer from 'multer';
import nodemailer from 'nodemailer';
import Database from 'better-sqlite3';
import { spawn } from 'child_process';
import { Readable } from 'stream';
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
const parseEnvPositiveInt = (value, fallback) => {
    const parsed = Number.parseInt(String(value ?? ''), 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};
const parseEnvBoolean = (value, fallback = false) => {
    if (value === undefined || value === null || value === '') return !!fallback;
    const normalized = String(value).trim().toLowerCase();
    if (['1', 'true', 'yes', 'on', 'enabled'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off', 'disabled'].includes(normalized)) return false;
    return !!fallback;
};
const AUTO_BACKUP_MODES = new Set(['db', 'full']);
const normalizeAutoBackupMode = (value, fallback = 'db') => {
    const normalized = String(value || '').trim().toLowerCase();
    if (AUTO_BACKUP_MODES.has(normalized)) return normalized;
    return AUTO_BACKUP_MODES.has(fallback) ? fallback : 'db';
};
const readJwtSecret = () => {
    const secret = String(process.env.JWT_SECRET || '').trim();
    if (secret) return secret;
    if (String(process.env.NODE_ENV || '').toLowerCase() === 'production') {
        throw new Error('JWT_SECRET is required in production');
    }
    console.warn('⚠️ JWT_SECRET is not set. Falling back to a development-only secret.');
    return 'dev-only-jwt-secret-change-me';
};

// ============ 設定 ============
const CONFIG = {
    JWT_SECRET: readJwtSecret(),
    PORT: process.env.PORT || 4103,
    HOST: process.env.HOST || '0.0.0.0',
    PUBLIC_HOST: process.env.PUBLIC_HOST || 'lesve.tech',
    CHAT_HOST: process.env.CHAT_HOST || '',
    SSH_HOST: process.env.SSH_HOST || '106.73.68.66',
    ADMIN_HOST: process.env.ADMIN_HOST || 'manage.lesve.tech',
    DATA_DIR: process.env.DATA_DIR || path.join(__dirname, '../data'),
    CONTAINERS_DIR: process.env.CONTAINERS_DIR || path.join(__dirname, '../data/containers'),
    BACKUPS_DIR: process.env.BACKUPS_DIR || path.join(__dirname, '../data/backups'),
    DB_PATH: process.env.DB_PATH || path.join(__dirname, '../data/shironeko.db'),
    MAX_FILE_SIZE: 100 * 1024 * 1024, // 100MB
    
    // Discord OAuth設定
    DISCORD_CLIENT_ID: process.env.DISCORD_CLIENT_ID || '',
    DISCORD_CLIENT_SECRET: process.env.DISCORD_CLIENT_SECRET || '',
    DISCORD_REDIRECT_URI: process.env.DISCORD_REDIRECT_URI || 'https://lesve.tech/api/auth/discord/callback',
    DISCORD_SUPPORT_WEBHOOK_URL: process.env.DISCORD_SUPPORT_WEBHOOK_URL || '',
    DISCORD_SUPPORT_NOTIFY_MENTION: process.env.DISCORD_SUPPORT_NOTIFY_MENTION || '',
    DISCORD_COMPLIANCE_WEBHOOK_URL: process.env.DISCORD_COMPLIANCE_WEBHOOK_URL || '',
    DISCORD_COMPLIANCE_NOTIFY_MENTION: process.env.DISCORD_COMPLIANCE_NOTIFY_MENTION || '',
    DISCORD_OPS_WEBHOOK_URL: process.env.DISCORD_OPS_WEBHOOK_URL || '',
    DISCORD_OPS_NOTIFY_MENTION: process.env.DISCORD_OPS_NOTIFY_MENTION || '',
    
    // Cloudflare Turnstile設定
    TURNSTILE_SITE_KEY: process.env.TURNSTILE_SITE_KEY || '',
    TURNSTILE_SECRET_KEY: process.env.TURNSTILE_SECRET_KEY || '',
    STRIPE_SECRET_KEY: process.env.STRIPE_SECRET_KEY || '',
    STRIPE_PRICE_STANDARD: process.env.STRIPE_PRICE_STANDARD || '',
    STRIPE_PRICE_PRO: process.env.STRIPE_PRICE_PRO || '',
    STRIPE_STANDARD_COUPON_ID: process.env.STRIPE_STANDARD_COUPON_ID || '',
    STRIPE_PAYMENT_METHOD_TYPES: process.env.STRIPE_PAYMENT_METHOD_TYPES || '',
    STRIPE_WEBHOOK_SECRET: process.env.STRIPE_WEBHOOK_SECRET || '',
    MAILER_ENABLED: parseEnvBoolean(process.env.MAILER_ENABLED, false),
    MAILER_SMTP_HOST: process.env.MAILER_SMTP_HOST || '',
    MAILER_SMTP_PORT: parseEnvPositiveInt(process.env.MAILER_SMTP_PORT, 587),
    MAILER_SMTP_SECURE: parseEnvBoolean(process.env.MAILER_SMTP_SECURE, false),
    MAILER_SMTP_USER: process.env.MAILER_SMTP_USER || '',
    MAILER_SMTP_PASS: process.env.MAILER_SMTP_PASS || '',
    MAILER_SMTP_REJECT_UNAUTHORIZED: parseEnvBoolean(process.env.MAILER_SMTP_REJECT_UNAUTHORIZED, true),
    MAILER_FROM_EMAIL: process.env.MAILER_FROM_EMAIL || '',
    MAILER_FROM_NAME: process.env.MAILER_FROM_NAME || 'LenovoServer',
    MAILER_REPLY_TO: process.env.MAILER_REPLY_TO || '',
    MAILER_MAX_RECIPIENTS: parseEnvPositiveInt(process.env.MAILER_MAX_RECIPIENTS, 3000),
    MAILER_SEND_DELAY_MS: parseEnvPositiveInt(process.env.MAILER_SEND_DELAY_MS, 120),
    NOTIFY_EMAIL_TO: process.env.NOTIFY_EMAIL_TO || 'koutarou114@outlook.jp',
    NOTIFY_SUBJECT_PREFIX: process.env.NOTIFY_SUBJECT_PREFIX || '[LenovoServer]',
    
    // WebAuthn設定
    RP_NAME: 'LenovoServer',
    RP_ID: process.env.RP_ID || 'lesve.tech',
    RP_ORIGIN: process.env.RP_ORIGIN || 'https://lesve.tech',
    
    // SSH設定
    SSH_PORT: parseInt(process.env.SSH_PORT) || 4104,  // 直接IPアドレス経由
    VM_TTYD_PORT: parseInt(process.env.VM_TTYD_PORT || '7681', 10),
    VM_TTYD_STABLE_BASE_URL: process.env.VM_TTYD_STABLE_BASE_URL || `https://${process.env.PUBLIC_HOST || 'lesve.tech'}`,
    VM_CREATE_DEFAULT_BACKEND: (process.env.VM_CREATE_DEFAULT_BACKEND || 'vm').toLowerCase(),
    VM_PROVISION_SCRIPT: process.env.VM_PROVISION_SCRIPT || '/app/scripts/provision-lenovo-vm.sh',
    VM_PROVISION_BRIDGE: process.env.VM_PROVISION_BRIDGE || 'virbr0',
    VM_POOL_DIR: process.env.VM_POOL_DIR || '/tmp/libvirt-images',
    VM_BASE_IMAGE_URL: process.env.VM_BASE_IMAGE_URL || 'https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img',
    VM_PROVISION_TIMEOUT_MS: parseEnvPositiveInt(process.env.VM_PROVISION_TIMEOUT_MS, 420000),
    
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
// Robot認証は廃止: 常時無効
const TURNSTILE_AUTH_ENABLED = false;
const SUPPORT_CATEGORIES = new Set(['bug_report', 'question', 'inquiry']);
const SUPPORT_CATEGORY_LABELS = {
    bug_report: 'バグ報告',
    question: '質問',
    inquiry: 'お問い合わせ'
};
const SUPPORT_CATEGORY_ALIASES = new Map([
    ['bug', 'bug_report'],
    ['bugreport', 'bug_report'],
    ['bug_report', 'bug_report'],
    ['バグ', 'bug_report'],
    ['バグ報告', 'bug_report'],
    ['question', 'question'],
    ['質問', 'question'],
    ['inquiry', 'inquiry'],
    ['contact', 'inquiry'],
    ['お問い合わせ', 'inquiry'],
    ['問い合わせ', 'inquiry']
]);
const SUPPORT_STATUSES = new Set(['open', 'waiting_admin', 'waiting_user', 'closed']);
const COMPLIANCE_STATUSES = new Set(['open', 'investigating', 'resolved', 'ignored']);
const COMPLIANCE_SEVERITIES = new Set(['low', 'medium', 'high', 'critical']);
const COMPLIANCE_STATUS_LABELS = {
    open: '未対応',
    investigating: '調査中',
    resolved: '解決済み',
    ignored: '誤検知/除外'
};
const COMPLIANCE_SEVERITY_LABELS = {
    low: '低',
    medium: '中',
    high: '高',
    critical: '重大'
};
const COMPLIANCE_LOGIN_EVENTS = new Set([
    'login_email',
    'login_discord',
    'login_2fa_passkey',
    'login_2fa_totp',
    'login_2fa_backup_code'
]);
const COMPLIANCE_ACTION_BURST_ACTIONS = new Set([
    'container_start',
    'container_stop',
    'container_restart',
    'container_delete',
    'container_create',
    'admin_container_start',
    'admin_container_stop',
    'admin_container_restart',
    'admin_container_delete'
]);
const COMPLIANCE_SUPPORT_ACTIVITY_ACTIONS = new Set([
    'support_ticket_created',
    'support_ticket_continued',
    'support_ticket_replied'
]);
const COMPLIANCE_RULE_LIMITS = {
    loginUniqueIp24h: parseEnvPositiveInt(process.env.COMPLIANCE_LOGIN_UNIQUE_IP_24H, 4),
    loginEventMin24h: parseEnvPositiveInt(process.env.COMPLIANCE_LOGIN_EVENT_MIN_24H, 5),
    actionBurst10m: parseEnvPositiveInt(process.env.COMPLIANCE_ACTION_BURST_10M, 15),
    supportTickets30m: parseEnvPositiveInt(process.env.COMPLIANCE_SUPPORT_TICKETS_30M, 4),
    supportReplies10m: parseEnvPositiveInt(process.env.COMPLIANCE_SUPPORT_REPLIES_10M, 20),
    notificationCooldownMinutes: parseEnvPositiveInt(process.env.COMPLIANCE_NOTIFY_COOLDOWN_MINUTES, 60)
};
const AUTO_BACKUP_CONFIG = {
    enabled: parseEnvBoolean(process.env.AUTO_BACKUP_ENABLED, true),
    mode: normalizeAutoBackupMode(process.env.AUTO_BACKUP_MODE, 'db'),
    startupMode: normalizeAutoBackupMode(process.env.AUTO_BACKUP_STARTUP_MODE, 'db'),
    intervalMinutes: parseEnvPositiveInt(process.env.AUTO_BACKUP_INTERVAL_MINUTES, 1440),
    timeoutMinutes: parseEnvPositiveInt(process.env.AUTO_BACKUP_TIMEOUT_MINUTES, 180),
    retentionDays: parseEnvPositiveInt(process.env.AUTO_BACKUP_RETENTION_DAYS, 14),
    restoreCheckEnabled: parseEnvBoolean(process.env.AUTO_BACKUP_RESTORE_CHECK_ENABLED, true),
    notifyEnabled: parseEnvBoolean(process.env.AUTO_BACKUP_NOTIFY_ENABLED, true)
};
const SYSTEM_MONITOR_CONFIG = {
    enabled: parseEnvBoolean(process.env.SYSTEM_MONITOR_ENABLED, true),
    intervalSeconds: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_INTERVAL_SECONDS, 300),
    alertCooldownMinutes: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_ALERT_COOLDOWN_MINUTES, 30),
    diskUsageWarnPercent: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_DISK_WARN_PERCENT, 85),
    dbSizeWarnMb: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_DB_WARN_MB, 2048),
    rssWarnMb: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_RSS_WARN_MB, 900),
    api5xxWindowSeconds: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_API_5XX_WINDOW_SECONDS, 300),
    api5xxWarnCount: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_API_5XX_WARN_COUNT, 5),
    backupStaleMinutes: parseEnvPositiveInt(
        process.env.SYSTEM_MONITOR_BACKUP_STALE_MINUTES,
        Math.max(AUTO_BACKUP_CONFIG.intervalMinutes * 2, 60)
    ),
    backupFailureWindowMinutes: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_BACKUP_FAILURE_WINDOW_MINUTES, 180),
    backupFailureWarnCount: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_BACKUP_FAILURE_WARN_COUNT, 1),
    stripeFailureWindowMinutes: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_STRIPE_FAILURE_WINDOW_MINUTES, 180),
    stripeFailureWarnCount: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_STRIPE_FAILURE_WARN_COUNT, 1),
    vmStoppedGraceMinutes: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_VM_STOPPED_GRACE_MINUTES, 10),
    vmStoppedCheckLimit: parseEnvPositiveInt(process.env.SYSTEM_MONITOR_VM_STOPPED_CHECK_LIMIT, 30)
};
const RATE_LIMIT_CONFIG = {
    enabled: parseEnvBoolean(process.env.RATE_LIMIT_ENABLED, true),
    authWindowMs: parseEnvPositiveInt(process.env.RATE_LIMIT_AUTH_WINDOW_SEC, 60) * 1000,
    authMax: parseEnvPositiveInt(process.env.RATE_LIMIT_AUTH_MAX, 20),
    supportCreateWindowMs: parseEnvPositiveInt(process.env.RATE_LIMIT_SUPPORT_CREATE_WINDOW_SEC, 600) * 1000,
    supportCreateMax: parseEnvPositiveInt(process.env.RATE_LIMIT_SUPPORT_CREATE_MAX, 6),
    supportReplyWindowMs: parseEnvPositiveInt(process.env.RATE_LIMIT_SUPPORT_REPLY_WINDOW_SEC, 600) * 1000,
    supportReplyMax: parseEnvPositiveInt(process.env.RATE_LIMIT_SUPPORT_REPLY_MAX, 30),
    billingWindowMs: parseEnvPositiveInt(process.env.RATE_LIMIT_BILLING_WINDOW_SEC, 600) * 1000,
    billingMax: parseEnvPositiveInt(process.env.RATE_LIMIT_BILLING_MAX, 30),
    adminWindowMs: parseEnvPositiveInt(process.env.RATE_LIMIT_ADMIN_WINDOW_SEC, 60) * 1000,
    adminMax: parseEnvPositiveInt(process.env.RATE_LIMIT_ADMIN_MAX, 120)
};

const sanitizeSupportText = (value) => String(value ?? '')
    .replace(/\r\n/g, '\n')
    .trim();

const normalizeSupportCategory = (value) => {
    const key = sanitizeSupportText(value).toLowerCase();
    return SUPPORT_CATEGORY_ALIASES.get(key) || null;
};

const normalizeSupportStatus = (value) => {
    const key = sanitizeSupportText(value).toLowerCase();
    if (SUPPORT_STATUSES.has(key)) return key;
    if (['resolved', 'done', '完了', 'close'].includes(key)) return 'closed';
    if (['reply_needed', 'admin_waiting', '運営対応待ち'].includes(key)) return 'waiting_admin';
    if (['user_waiting', '返信待ち'].includes(key)) return 'waiting_user';
    return null;
};

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

const commandResultMessage = (result, fallback = 'unknown error') => {
    const stderr = String(result?.stderr || '').trim();
    if (stderr) return stderr;
    const stdout = String(result?.stdout || '').trim();
    if (stdout) return stdout;
    return fallback;
};

const removeFileQuietly = (targetPath) => {
    if (!targetPath) return;
    try {
        fs.rmSync(targetPath, { force: true });
    } catch {
        // no-op
    }
};

const waitForVmGuestAgentReady = async (vmName, timeoutMs = 45000) => {
    const startedAt = Date.now();
    while ((Date.now() - startedAt) < timeoutMs) {
        const ping = await qemuAgentCommand(vmName, { execute: 'guest-ping' }, 4000);
        if (ping.ok) return true;
        await sleep(1500);
    }
    return false;
};

const setVmUserPassword = async (vmName, userName, plainPassword) => {
    const normalizedUser = String(userName || '').trim();
    const normalizedPassword = String(plainPassword || '').trim();
    if (!normalizedUser || !normalizedPassword) {
        return { ok: false, error: 'username/password is required' };
    }

    const passwordBase64 = Buffer.from(normalizedPassword, 'utf8').toString('base64');
    return qemuAgentCommand(vmName, {
        execute: 'guest-set-user-password',
        arguments: {
            username: normalizedUser,
            password: passwordBase64,
            crypted: false
        }
    }, 8000);
};

const normalizeVmCpu = (value, fallback = 1) => {
    const parsed = Number.parseFloat(String(value ?? ''));
    if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
    return Math.max(1, Math.ceil(parsed));
};

const normalizeVmMemoryMb = (value, fallback = 512) => {
    const parsed = Number.parseFloat(String(value ?? ''));
    if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
    return Math.max(256, Math.ceil(parsed));
};

const normalizeVmDiskGb = (storageMb, fallback = 20) => {
    const parsed = Number.parseFloat(String(storageMb ?? ''));
    if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
    return Math.max(1, Math.ceil(parsed / 1024));
};

const resolveContainerCreateBackend = (requestedBackend) => {
    const requested = String(requestedBackend || '').trim().toLowerCase();
    if (requested === 'docker' || requested === 'vm') return requested;
    return CONFIG.VM_CREATE_DEFAULT_BACKEND === 'docker' ? 'docker' : 'vm';
};

const provisionVmForContainer = async ({
    containerId,
    sshUser,
    sshPassword,
    planConfig,
    autoRestart
}) => {
    const vmName = `vm-${containerId}`;
    const provisionScript = String(CONFIG.VM_PROVISION_SCRIPT || '').trim();
    if (!provisionScript || !fs.existsSync(provisionScript)) {
        return { ok: false, error: `VM作成スクリプトが見つかりません: ${provisionScript || '(empty)'}` };
    }

    const vcpus = normalizeVmCpu(planConfig?.cpu, 1);
    const memoryMb = normalizeVmMemoryMb(planConfig?.memory, 512);
    const diskGb = normalizeVmDiskGb(planConfig?.storage, 20);
    const keyDir = path.join(CONFIG.DATA_DIR, 'tmp', 'vm-provision-keys');
    fs.mkdirSync(keyDir, { recursive: true, mode: 0o700 });

    const keyBase = path.join(
        keyDir,
        `${vmName}-${Date.now()}-${Math.random().toString(16).slice(2, 10)}`
    );
    const pubKeyPath = `${keyBase}.pub`;

    try {
        const keygen = await runCommand('ssh-keygen', [
            '-q',
            '-t', 'ed25519',
            '-N', '',
            '-C', `${sshUser}@${vmName}`,
            '-f', keyBase
        ], 15000);
        if (!keygen.ok || !fs.existsSync(pubKeyPath)) {
            return {
                ok: false,
                error: `SSH鍵生成に失敗しました: ${commandResultMessage(keygen, 'ssh-keygen failed')}`
            };
        }

        const provisionArgs = [
            provisionScript,
            '--tenant-id', containerId,
            '--name', vmName,
            '--ssh-key-file', pubKeyPath,
            '--admin-user', sshUser,
            '--memory-mb', String(memoryMb),
            '--vcpus', String(vcpus),
            '--disk-gb', String(diskGb),
            '--bridge', String(CONFIG.VM_PROVISION_BRIDGE || 'virbr0'),
            '--pool-dir', String(CONFIG.VM_POOL_DIR || '/tmp/libvirt-images')
        ];
        if (String(CONFIG.VM_BASE_IMAGE_URL || '').trim()) {
            provisionArgs.push('--image-url', String(CONFIG.VM_BASE_IMAGE_URL).trim());
        }

        const provisionResult = await runCommand(
            'bash',
            provisionArgs,
            Math.max(120000, CONFIG.VM_PROVISION_TIMEOUT_MS || 420000)
        );
        if (!provisionResult.ok) {
            return {
                ok: false,
                error: `VMの作成に失敗しました: ${commandResultMessage(provisionResult, 'vm provision failed')}`
            };
        }

        const autostartArgs = autoRestart
            ? ['autostart', vmName]
            : ['autostart', vmName, '--disable'];
        const autostartResult = await runCommand('virsh', autostartArgs, 10000);
        if (!autostartResult.ok) {
            console.warn(`VM autostart update failed (${vmName}): ${commandResultMessage(autostartResult)}`);
        }

        const guestAgentReady = await waitForVmGuestAgentReady(vmName, 45000);
        if (guestAgentReady) {
            const passwordSet = await setVmUserPassword(vmName, sshUser, sshPassword);
            if (!passwordSet.ok) {
                console.warn(`VM user password set skipped (${vmName}/${sshUser}): ${passwordSet.error}`);
            }
        } else {
            console.warn(`VM guest agent not ready in time: ${vmName}`);
        }

        const vmInfo = await getVmBackendInfo(vmName);
        return {
            ok: true,
            dockerId: `vm:${vmName}`,
            status: vmInfo?.status || 'running',
            vmName
        };
    } finally {
        removeFileQuietly(keyBase);
        removeFileQuietly(pubKeyPath);
    }
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
const normalizeShellScript = (value) => String(value || '').replace(/\0/g, '').trim();
const buildDetachedContainerStartupScript = (startupCommand) => {
    const normalized = normalizeShellScript(startupCommand);
    if (!normalized) return '';
    return `cd /app 2>/dev/null || cd /home/user/app 2>/dev/null || cd /; nohup /bin/bash -lc ${shellSingleQuote(normalized)} >/tmp/startup-command.log 2>&1 &`;
};
const buildContainerExecScript = (command) => {
    const normalized = normalizeShellScript(command);
    if (!normalized) return '';
    return `cd /home/user/app 2>/dev/null || cd /app 2>/dev/null || cd /; /bin/bash -lc ${shellSingleQuote(normalized)}`;
};
const PACKAGE_NAME_PATTERN = /^[a-z0-9][a-z0-9+.-]*$/i;
const normalizeAptPackages = (packages = []) => {
    const deduped = Array.from(new Set(
        (Array.isArray(packages) ? packages : [])
            .map((pkg) => String(pkg || '').trim())
            .filter(Boolean)
    ));
    for (const pkg of deduped) {
        if (!PACKAGE_NAME_PATTERN.test(pkg)) {
            throw new Error(`invalid package name: ${pkg}`);
        }
    }
    return deduped;
};
const buildAptSetupScript = (packages = []) => {
    const safePackages = normalizeAptPackages(packages);
    const installPart = safePackages.length ? `apt-get install -y ${safePackages.map(shellSingleQuote).join(' ')}` : 'true';
    return `set -e; apt-get update; ${installPart}; apt-get clean`;
};

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
const generatePublicLinkToken = () => crypto.randomBytes(16).toString('hex');
const normalizePublicBaseUrl = (value, fallback) => {
    const raw = String(value || fallback || '').trim();
    if (!raw) return fallback;
    const normalized = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
    return normalized.replace(/\/+$/, '');
};
const VM_TTYD_STABLE_BASE_URL = normalizePublicBaseUrl(
    CONFIG.VM_TTYD_STABLE_BASE_URL,
    `https://ter.${CONFIG.PUBLIC_HOST}`
);
const buildVmTtydStableUrl = (token) => `${VM_TTYD_STABLE_BASE_URL}/vm-ttyd/${encodeURIComponent(token)}`;

const parseIpv4Candidates = (rawValue = '') => {
    const matches = String(rawValue || '').match(/\b\d{1,3}(?:\.\d{1,3}){3}\b/g) || [];
    const unique = [];
    for (const value of matches) {
        const ip = String(value).trim();
        const octets = ip.split('.').map((part) => Number.parseInt(part, 10));
        if (octets.length !== 4 || octets.some((part) => !Number.isFinite(part) || part < 0 || part > 255)) {
            continue;
        }
        if (ip.startsWith('127.') || ip.startsWith('0.')) {
            continue;
        }
        if (!unique.includes(ip)) {
            unique.push(ip);
        }
    }
    return unique;
};

const isPrivateIpv4Address = (ipAddress = '') => {
    const parts = String(ipAddress || '').trim().split('.').map((part) => Number.parseInt(part, 10));
    if (parts.length !== 4 || parts.some((part) => !Number.isFinite(part) || part < 0 || part > 255)) {
        return false;
    }
    const [a, b] = parts;
    if (a === 10) return true;
    if (a === 192 && b === 168) return true;
    if (a === 172 && b >= 16 && b <= 31) return true;
    return false;
};

const pickVmTtydHostFromText = (rawValue = '') => {
    const candidates = parseIpv4Candidates(rawValue);
    if (candidates.length === 0) return null;
    return candidates.find((ip) => isPrivateIpv4Address(ip)) || candidates[0] || null;
};

const resolveVmTtydHostFromVirsh = async (vmName) => {
    const domifSources = ['agent', 'lease', 'arp'];
    for (const source of domifSources) {
        const domifaddr = await runCommand('virsh', ['domifaddr', vmName, '--source', source], 5000);
        if (!domifaddr.ok || !domifaddr.stdout) continue;
        const detected = pickVmTtydHostFromText(domifaddr.stdout);
        if (detected) return detected;
    }

    const fallbackDomifaddr = await runCommand('virsh', ['domifaddr', vmName], 5000);
    if (fallbackDomifaddr.ok && fallbackDomifaddr.stdout) {
        return pickVmTtydHostFromText(fallbackDomifaddr.stdout);
    }

    return null;
};

const ensureVmTtydDirectEndpoint = async (vmName, { port, authUser, authPass }) => {
    const safePort = normalizeVmTtydPort(port, normalizeVmTtydPort(CONFIG.VM_TTYD_PORT, 7681));
    const safeUser = sanitizeTtydAuthUser(authUser, 'admin');
    const safePass = String(authPass || '').trim() || generateTunnelPassword();

    const script = `
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
PORT=${safePort}
TTYD_USER=${shellSingleQuote(safeUser)}
TTYD_PASS=${shellSingleQuote(safePass)}

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

if ! command -v ttyd >/dev/null 2>&1; then
  retry apt-get update -y
  retry apt-get install -y ttyd curl ca-certificates
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
  local near_end=$((base + 256))
  local p
  for p in $(seq "$base" "$near_end"); do
    if ! port_in_use "$p"; then
      echo "$p"
      return 0
    fi
  done

  if command -v ss >/dev/null 2>&1 && command -v shuf >/dev/null 2>&1; then
    local used_ports candidate
    used_ports="$(ss -H -ltn 2>/dev/null | awk '{print $4}' | sed -E 's/.*:([0-9]+)$/\\1/' | awk '/^[0-9]+$/' | sort -n -u)"
    candidate="$(comm -23 <(seq 10000 60999) <(printf '%s\n' "$used_ports") | shuf -n 1 || true)"
    if [ -n "$candidate" ]; then
      echo "$candidate"
      return 0
    fi
  fi

  return 1
}

# 古いttydが溜まるとポート枯渇するため、同一ユーザーの既存セッションを掃除する
if pgrep -f "ttyd .* /bin/login -f \${TTYD_USER}" >/dev/null 2>&1; then
  pkill -f "ttyd .* /bin/login -f \${TTYD_USER}" >/dev/null 2>&1 || true
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

nohup ttyd --port "\${ACTUAL_PORT}" --interface 0.0.0.0 --writable -c "\${TTYD_USER}:\${TTYD_PASS}" /bin/login -f "\${TTYD_USER}" >/var/log/lenovo/ttyd.log 2>&1 &
sleep 1

if ! pgrep -f "ttyd .*--port \${ACTUAL_PORT}" >/dev/null 2>&1; then
  echo "failed to start ttyd" >&2
  tail -n 60 /var/log/lenovo/ttyd.log >&2 || true
  exit 1
fi

if command -v ufw >/dev/null 2>&1; then
  if ufw status 2>/dev/null | grep -q '^Status: active'; then
    ufw allow "\${ACTUAL_PORT}/tcp" >/dev/null 2>&1 || true
  fi
fi

VM_IP="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if ($i==\"src\") {print $(i+1); exit}}' || true)"
if [ -z "\${VM_IP}" ]; then
  VM_IP="$(hostname -I 2>/dev/null | tr ' ' '\\n' | grep -E '^[0-9]+(\\.[0-9]+){3}$' | grep -v '^127\\.' | head -n 1 || true)"
fi

echo "TTYD_PORT=\${ACTUAL_PORT}"
if [ -n "\${VM_IP}" ]; then
  echo "TTYD_HOST=\${VM_IP}"
fi
`.trim();

    const result = await vmExecBash(vmName, script, { timeoutMs: 180000 });
    const combined = `${result.stdout || ''}\n${result.stderr || ''}`;
    const hostLineMatch = combined.match(/TTYD_HOST=([0-9]{1,3}(?:\.[0-9]{1,3}){3})/);
    const portLineMatch = combined.match(/TTYD_PORT=(\d{2,5})/);
    let detectedHost = hostLineMatch ? hostLineMatch[1] : pickVmTtydHostFromText(combined);
    const detectedPort = portLineMatch ? normalizeVmTtydPort(portLineMatch[1], safePort) : safePort;
    if (!detectedHost) {
        detectedHost = await resolveVmTtydHostFromVirsh(vmName);
    }
    if (!result.ok || !detectedHost) {
        const defaultError = result.ok
            ? 'ttyd started but VM IP could not be resolved'
            : 'ttyd direct endpoint setup failed';
        return {
            ok: false,
            error: (result.stderr || result.stdout || defaultError).trim()
        };
    }

    return {
        ok: true,
        url: `http://${detectedHost}:${detectedPort}`,
        port: detectedPort,
        username: safeUser,
        password: safePass
    };
};

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
  local near_end=$((base + 256))
  local p
  for p in $(seq "$base" "$near_end"); do
    if ! port_in_use "$p"; then
      echo "$p"
      return 0
    fi
  done

  if command -v ss >/dev/null 2>&1 && command -v shuf >/dev/null 2>&1; then
    local used_ports candidate
    used_ports="$(ss -H -ltn 2>/dev/null | awk '{print $4}' | sed -E 's/.*:([0-9]+)$/\\1/' | awk '/^[0-9]+$/' | sort -n -u)"
    candidate="$(comm -23 <(seq 10000 60999) <(printf '%s\n' "$used_ports") | shuf -n 1 || true)"
    if [ -n "$candidate" ]; then
      echo "$candidate"
      return 0
    fi
  fi

  return 1
}

# 古いttydが溜まるとポート枯渇するため、同一ユーザーの既存セッションを掃除する
if pgrep -f "ttyd .* /bin/login -f \${AUTH_USER}" >/dev/null 2>&1; then
  pkill -f "ttyd .* /bin/login -f \${AUTH_USER}" >/dev/null 2>&1 || true
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

const isRetryableTryCloudflareError = (message = '') => {
    const value = String(message || '').toLowerCase();
    return (
        value.includes('context deadline exceeded') ||
        value.includes('failed to request quick tunnel') ||
        value.includes('failed to create trycloudflare url')
    );
};

const ensureVmTtydTemporaryEndpointWithRetry = async (vmName, options, maxAttempts = 3) => {
    const attempts = Math.max(1, Number.parseInt(maxAttempts, 10) || 1);
    let lastResult = { ok: false, error: 'ttyd tunnel setup failed' };

    for (let attempt = 1; attempt <= attempts; attempt += 1) {
        const result = await ensureVmTtydTryCloudflareTunnel(vmName, options);
        if (result.ok) {
            return result;
        }

        lastResult = result;
        if (!isRetryableTryCloudflareError(result.error) || attempt >= attempts) {
            break;
        }

        await sleep(1200 * attempt);
    }

    return lastResult;
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

    CREATE TABLE IF NOT EXISTS access_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        event TEXT NOT NULL,
        path TEXT,
        method TEXT,
        ip_address TEXT,
        user_agent TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS compliance_incidents (
        id TEXT PRIMARY KEY,
        rule_key TEXT NOT NULL,
        user_id TEXT,
        container_id TEXT,
        severity TEXT NOT NULL DEFAULT 'medium',
        status TEXT NOT NULL DEFAULT 'open',
        title TEXT NOT NULL,
        description TEXT NOT NULL,
        evidence_json TEXT,
        occurrence_count INTEGER DEFAULT 1,
        first_detected_at TEXT DEFAULT CURRENT_TIMESTAMP,
        last_detected_at TEXT DEFAULT CURRENT_TIMESTAMP,
        last_notified_at TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        resolved_by TEXT,
        resolved_at TEXT,
        resolution_note TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (resolved_by) REFERENCES users(id)
    );

    CREATE TABLE IF NOT EXISTS stripe_webhook_events (
        event_id TEXT PRIMARY KEY,
        event_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'processed',
        attempts INTEGER NOT NULL DEFAULT 1,
        payload_hash TEXT,
        last_error TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        processed_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_containers_user ON containers(user_id);
    CREATE INDEX IF NOT EXISTS idx_stats_container ON container_stats(container_id);
    CREATE INDEX IF NOT EXISTS idx_logs_user ON activity_logs(user_id);
    CREATE INDEX IF NOT EXISTS idx_access_logs_user ON access_logs(user_id);
    CREATE INDEX IF NOT EXISTS idx_access_logs_created ON access_logs(created_at);
    CREATE INDEX IF NOT EXISTS idx_compliance_incidents_status ON compliance_incidents(status);
    CREATE INDEX IF NOT EXISTS idx_compliance_incidents_rule ON compliance_incidents(rule_key);
    CREATE INDEX IF NOT EXISTS idx_compliance_incidents_user ON compliance_incidents(user_id);
    CREATE INDEX IF NOT EXISTS idx_compliance_incidents_detected ON compliance_incidents(last_detected_at);
    CREATE INDEX IF NOT EXISTS idx_stripe_events_type ON stripe_webhook_events(event_type);
    CREATE INDEX IF NOT EXISTS idx_stripe_events_status ON stripe_webhook_events(status);
    CREATE INDEX IF NOT EXISTS idx_stripe_events_processed_at ON stripe_webhook_events(processed_at);
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

// VM ttyd 固定リンクテーブル
try {
    db.exec(`
        CREATE TABLE IF NOT EXISTS vm_ttyd_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            container_id TEXT NOT NULL UNIQUE,
            vm_name TEXT NOT NULL,
            link_token TEXT NOT NULL UNIQUE,
            target_url TEXT NOT NULL,
            username TEXT,
            password TEXT,
            ttyd_port INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (container_id) REFERENCES containers(id)
        )
    `);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_vm_ttyd_links_token ON vm_ttyd_links(link_token)`);
} catch (e) {}

// 問い合わせチケット
try {
    db.exec(`
        CREATE TABLE IF NOT EXISTS support_tickets (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            category TEXT NOT NULL DEFAULT 'inquiry',
            subject TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'waiting_admin',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            closed_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    `);
    db.exec(`
        CREATE TABLE IF NOT EXISTS support_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            sender_role TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (ticket_id) REFERENCES support_tickets(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    `);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_support_tickets_user ON support_tickets(user_id)`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_support_tickets_status ON support_tickets(status)`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_support_messages_ticket ON support_messages(ticket_id)`);
} catch (e) {}

// 一括メール配信テーブル
try {
    db.exec(`
        CREATE TABLE IF NOT EXISTS bulk_mail_campaigns (
            id TEXT PRIMARY KEY,
            created_by TEXT NOT NULL,
            recipient_mode TEXT NOT NULL,
            recipient_filter_json TEXT,
            subject TEXT NOT NULL,
            body_text TEXT NOT NULL,
            body_html TEXT,
            total_recipients INTEGER NOT NULL DEFAULT 0,
            sent_count INTEGER NOT NULL DEFAULT 0,
            failed_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'completed',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            started_at TEXT,
            completed_at TEXT,
            FOREIGN KEY (created_by) REFERENCES users(id)
        )
    `);
    db.exec(`
        CREATE TABLE IF NOT EXISTS bulk_mail_deliveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id TEXT NOT NULL,
            recipient_email TEXT NOT NULL,
            recipient_name TEXT,
            status TEXT NOT NULL DEFAULT 'queued',
            provider_message_id TEXT,
            error_message TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            sent_at TEXT,
            FOREIGN KEY (campaign_id) REFERENCES bulk_mail_campaigns(id)
        )
    `);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_bulk_mail_campaigns_created_at ON bulk_mail_campaigns(created_at)`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_bulk_mail_campaigns_status ON bulk_mail_campaigns(status)`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_bulk_mail_campaigns_created_by ON bulk_mail_campaigns(created_by)`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_bulk_mail_deliveries_campaign ON bulk_mail_deliveries(campaign_id)`);
    db.exec(`CREATE INDEX IF NOT EXISTS idx_bulk_mail_deliveries_status ON bulk_mail_deliveries(status)`);
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
const wss = new WebSocketServer({ noServer: true });
const LEGACY_HOST_REDIRECTS = new Map([
    ['lenovo.schale41.jp', CONFIG.PUBLIC_HOST],
    ['manage.schale41.jp', CONFIG.ADMIN_HOST]
]);

const getRequestHost = (req) => {
    const forwarded = String(req.headers['x-forwarded-host'] || '').split(',')[0].trim();
    const direct = String(req.headers.host || '').split(',')[0].trim();
    const rawHost = (forwarded || direct).toLowerCase();
    return rawHost.replace(/:\d+$/, '');
};

const normalizeHostName = (value) => String(value || '').trim().toLowerCase().replace(/:\d+$/, '');
const PUBLIC_HOSTNAME = normalizeHostName(CONFIG.PUBLIC_HOST);
const ADMIN_HOSTNAME = normalizeHostName(CONFIG.ADMIN_HOST);
const CHAT_HOSTNAME = normalizeHostName(CONFIG.CHAT_HOST);
const CORS_ALLOWED_ORIGINS = (() => {
    const origins = new Set();
    const envOrigins = String(process.env.CORS_ALLOWED_ORIGINS || '')
        .split(',')
        .map((origin) => origin.trim())
        .filter(Boolean);
    envOrigins.forEach((origin) => origins.add(origin));
    if (PUBLIC_HOSTNAME) origins.add(`https://${PUBLIC_HOSTNAME}`);
    if (PUBLIC_HOSTNAME) origins.add(`https://www.${PUBLIC_HOSTNAME}`);
    if (ADMIN_HOSTNAME) origins.add(`https://${ADMIN_HOSTNAME}`);
    if (CHAT_HOSTNAME) origins.add(`https://${CHAT_HOSTNAME}`);
    if (String(process.env.NODE_ENV || '').toLowerCase() !== 'production') {
        origins.add('http://localhost:4103');
        origins.add('http://127.0.0.1:4103');
    }
    return origins;
})();
const corsOptions = {
    origin: (origin, callback) => {
        if (!origin) return callback(null, true);
        if (CORS_ALLOWED_ORIGINS.has(origin)) return callback(null, true);
        return callback(null, false);
    },
    credentials: true
};
const TRUSTED_OAUTH_REDIRECT_HOSTS = new Set(
    [PUBLIC_HOSTNAME, ADMIN_HOSTNAME, PUBLIC_HOSTNAME ? `www.${PUBLIC_HOSTNAME}` : ''].filter(Boolean)
);

const isTrustedOAuthRedirectHost = (hostname) => {
    const normalized = normalizeHostName(hostname);
    if (!normalized) return false;
    if (TRUSTED_OAUTH_REDIRECT_HOSTS.has(normalized)) return true;
    return PUBLIC_HOSTNAME ? normalized.endsWith(`.${PUBLIC_HOSTNAME}`) : false;
};

const getDefaultOAuthRedirectUrl = (requestHost = '') => {
    const normalized = normalizeHostName(requestHost);
    if (normalized && normalized === ADMIN_HOSTNAME) {
        return `https://${ADMIN_HOSTNAME}/admin.html`;
    }
    return `https://${PUBLIC_HOSTNAME}/dashboard.html`;
};

const sanitizeOAuthRedirectUrl = (rawValue, requestHost = '') => {
    const value = String(rawValue || '').trim();
    if (!value) return '';
    try {
        const baseHost = normalizeHostName(requestHost) || PUBLIC_HOSTNAME;
        const parsed = new URL(value, `https://${baseHost}`);
        if (!['https:', 'http:'].includes(parsed.protocol)) return '';
        if (!isTrustedOAuthRedirectHost(parsed.hostname)) return '';

        parsed.searchParams.delete('token');
        parsed.searchParams.delete('redirect');

        const authPaths = new Set(['/auth', '/auth/', '/auth.html']);
        if (authPaths.has(parsed.pathname)) {
            parsed.pathname = normalizeHostName(parsed.hostname) === ADMIN_HOSTNAME ? '/admin.html' : '/dashboard.html';
            parsed.hash = '';
        }
        return parsed.toString();
    } catch {
        return '';
    }
};

const encodeOAuthState = (payload) => {
    try {
        return Buffer.from(JSON.stringify(payload || {}), 'utf8').toString('base64url');
    } catch {
        return '';
    }
};

const decodeOAuthState = (stateValue) => {
    const raw = String(stateValue || '').trim();
    if (!raw) return null;
    for (const encoding of ['base64url', 'base64']) {
        try {
            const parsed = JSON.parse(Buffer.from(raw, encoding).toString('utf8'));
            if (parsed && typeof parsed === 'object') return parsed;
        } catch {}
    }
    return null;
};

const normalizeClientIpValue = (value) => {
    const raw = String(value || '').trim();
    if (!raw) return '';

    // "client, proxy1, proxy2" の先頭だけ利用
    let token = raw.split(',')[0].trim();
    if (!token) return '';

    // quoted-string 形式を除去
    token = token.replace(/^"+|"+$/g, '');

    // [IPv6]:port 形式
    const bracketMatch = token.match(/^\[([^\]]+)\](?::\d+)?$/);
    if (bracketMatch) {
        token = bracketMatch[1];
    }

    // IPv4:port 形式
    if (/^\d{1,3}(?:\.\d{1,3}){3}:\d+$/.test(token)) {
        token = token.replace(/:\d+$/, '');
    }

    // IPv4-mapped IPv6 (::ffff:x.x.x.x)
    const mapped = token.match(/^::ffff:(\d{1,3}(?:\.\d{1,3}){3})$/i);
    if (mapped) {
        token = mapped[1];
    }

    if (token === '::1') return '127.0.0.1';
    return token;
};

const getClientIp = (req) => {
    if (!req) return '-';
    return (
        normalizeClientIpValue(req.headers['cf-connecting-ip']) ||
        normalizeClientIpValue(req.headers['x-real-ip']) ||
        normalizeClientIpValue(req.headers['x-forwarded-for']) ||
        normalizeClientIpValue(req.ip) ||
        normalizeClientIpValue(req.socket?.remoteAddress) ||
        normalizeClientIpValue(req.connection?.remoteAddress) ||
        '-'
    );
};

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

app.use(cors(corsOptions));
app.use(express.json({
    verify: (req, _res, buf) => {
        if (String(req.originalUrl || '').startsWith('/api/billing/webhook')) {
            req.rawBody = Buffer.from(buf);
        }
    }
}));
app.set('trust proxy', true);

app.use('/api', (req, res, next) => {
    const startedAt = Date.now();
    res.on('finish', () => {
        const statusCode = Number(res.statusCode || 0);
        if (statusCode < 500) return;

        const normalizedPath = String(req.originalUrl || req.url || '')
            .replace(/^https?:\/\/[^/]+/i, '')
            .split('?')[0];
        if (normalizedPath === '/api/health') return;

        pushSystemMonitorEvent('api5xx', {
            status: statusCode,
            method: String(req.method || 'GET').toUpperCase(),
            path: normalizedPath || '/api',
            ip: getClientIp(req),
            elapsed_ms: Math.max(0, Date.now() - startedAt)
        });
    });
    next();
});

// Canonical host redirect for migrated domains
app.use((req, res, next) => {
    const host = getRequestHost(req);
    if (!host) return next();
    const isRootPath = req.path === '/' || req.path === '/index.html';

    const chatHost = String(CONFIG.CHAT_HOST || '').toLowerCase();
    if (chatHost && host === chatHost) {
        if (isRootPath) {
            return res.redirect(302, '/chat/user');
        }
    }

    if (host === `www.${CONFIG.PUBLIC_HOST}`.toLowerCase()) {
        return res.redirect(301, `https://${CONFIG.PUBLIC_HOST}${req.originalUrl || '/'}`);
    }

    const redirectHost = LEGACY_HOST_REDIRECTS.get(host);
    if (redirectHost) {
        return res.redirect(301, `https://${redirectHost}${req.originalUrl || '/'}`);
    }

    // Public root no longer uses a marketing homepage; send users to auth flow.
    if (isRootPath) {
        return res.redirect(302, '/auth.html');
    }

    return next();
});

app.use(express.static(path.join(__dirname, '../frontend/public')));

// ダッシュボードUIを機能別URLで提供（URL直打ち/再読込対応）
const DASHBOARD_SHELL_ROUTES = [
    '/dashboard',
    '/dashboard/',
    '/overview',
    '/overview/',
    '/servers',
    '/servers/',
    '/console',
    '/console/',
    '/files',
    '/files/',
    '/env',
    '/env/',
    '/backups',
    '/backups/',
    '/patches',
    '/patches/',
    '/settings',
    '/settings/',
    '/admin/dashboard',
    '/admin/dashboard/'
];

app.get([...DASHBOARD_SHELL_ROUTES, '/server/:id'], (req, res) => {
    res.sendFile(path.join(__dirname, '../frontend/public/dashboard.html'));
});

// リファレンスの短縮URL
app.get(['/reference', '/reference/', '/docs', '/docs/'], (req, res) => {
    res.sendFile(path.join(__dirname, '../frontend/public/reference.html'));
});

// 管理UIの互換パス
app.get(['/manage', '/manage/'], (req, res) => {
    res.sendFile(path.join(__dirname, '../frontend/public/admin.html'));
});

// 問い合わせ専用チャット（ユーザー/管理者でURL分離）
app.get(['/chat/user', '/chat/user/', '/chat/admin', '/chat/admin/'], (req, res) => {
    res.set({
        'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
    });
    res.sendFile(path.join(__dirname, '../frontend/public/chat.html'));
});

// 旧チャットURL互換
app.get(['/chat', '/chat/', '/chat.html'], (req, res) => {
    const query = new URLSearchParams(req.query || {});
    const scope = String(query.get('scope') || '').toLowerCase();
    query.delete('scope');
    const targetBase = scope === 'all' ? '/chat/admin' : '/chat/user';
    const queryString = query.toString();
    const location = queryString ? `${targetBase}?${queryString}` : targetBase;
    return res.redirect(302, location);
});

// 旧問い合わせURLの互換リダイレクト
app.get(['/support', '/support/', '/support-chat', '/support-chat/'], (req, res) => {
    res.redirect(302, '/chat/user');
});

// VM ttyd 固定リンク（URLを維持したリバースプロキシ）
app.use('/vm-ttyd/:token', async (req, res) => {
    const token = String(req.params.token || '').trim();
    if (!VM_TTYD_TOKEN_PATTERN.test(token)) {
        return res.status(404).send('Not found');
    }

    const context = await buildVmTtydProxyContext(token, req.originalUrl || req.url || `/vm-ttyd/${token}`);
    if (!context.ok) {
        return res.status(context.statusCode || 500).send(context.error || 'Proxy error');
    }

    const shouldSendBody = !['GET', 'HEAD'].includes(String(req.method || 'GET').toUpperCase());
    const requestBody = shouldSendBody ? await readRequestBody(req) : undefined;
    const proxied = await fetchVmTtydHttp(context.upstreamUrl, {
        method: req.method,
        headers: req.headers,
        body: requestBody,
        username: context.record.username,
        password: context.record.password
    });

    if (!proxied.ok) {
        if (proxied.canRetry && context.record) {
            scheduleVmTtydRefresh(context.record, 'http-upstream-unavailable');
        }
        return sendVmTtydUnavailable(res);
    }

    return pipeVmTtydResponse(proxied.response, res);
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
const safeParseJson = (rawValue, fallback = {}) => {
    if (!rawValue || typeof rawValue !== 'string') return fallback;
    try {
        const parsed = JSON.parse(rawValue);
        return parsed && typeof parsed === 'object' ? parsed : fallback;
    } catch (e) {
        return fallback;
    }
};

const toSqliteDateTime = (isoValue) => String(isoValue || '').replace('T', ' ').replace('Z', '');

const toIsoFromSqliteDateTime = (value) => {
    const text = String(value || '').trim();
    if (!text) return '';
    const withT = text.includes('T') ? text : text.replace(' ', 'T');
    return withT.endsWith('Z') ? withT : `${withT}Z`;
};

const summarizeComplianceEvidence = (ruleKey, evidence = {}) => {
    if (!evidence || typeof evidence !== 'object') return '-';
    if (ruleKey === 'login_ip_spread_24h') {
        return `24hログイン${evidence.loginCount || 0}件 / IP${evidence.uniqueIpCount || 0}種`;
    }
    if (ruleKey === 'container_action_burst_10m') {
        return `10分操作${evidence.actionCount || 0}件 / 対象${evidence.uniqueContainerCount || 0}台`;
    }
    if (ruleKey === 'support_ticket_spam_30m') {
        return `30分の新規問い合わせ${evidence.ticketCount || 0}件`;
    }
    if (ruleKey === 'support_reply_spam_10m') {
        return `10分の返信${evidence.replyCount || 0}件`;
    }
    return shortenText(JSON.stringify(evidence), 180);
};

const sendComplianceDiscordNotification = async (incident) => {
    if (!incident?.id) return false;

    const user = incident.user_id
        ? db.prepare('SELECT id, email, name FROM users WHERE id = ?').get(incident.user_id)
        : null;
    const evidence = safeParseJson(incident.evidence_json, {});
    const adminUrl = `${CONFIG.RP_ORIGIN}/dashboard.html`;
    const title = incident.occurrence_count > 1
        ? '利用規約監視アラート（継続）'
        : '利用規約監視アラート（新規）';
    const severityText = COMPLIANCE_SEVERITY_LABELS[incident.severity] || incident.severity || '中';
    const statusText = COMPLIANCE_STATUS_LABELS[incident.status] || incident.status || '未対応';
    const userText = user ? `${user.name || '-'} (${user.email || '-'})` : '-';
    const summaryText = shortenText(incident.description || incident.title || '-', 260);
    const evidenceText = summarizeComplianceEvidence(incident.rule_key, evidence);

    return sendInternalNotificationEmail({
        subject: `${CONFIG.NOTIFY_SUBJECT_PREFIX} [COMPLIANCE] ${title}`,
        lines: [
            `分類: COMPLIANCE`,
            `時刻: ${new Date().toISOString()}`,
            `インシデントID: ${incident.id}`,
            `ルール: ${incident.rule_key || '-'}`,
            `重大度: ${severityText}`,
            `ステータス: ${statusText}`,
            `ユーザー: ${userText}`,
            `概要: ${summaryText}`,
            `根拠: ${evidenceText}`,
            `管理画面: ${adminUrl}`
        ]
    });
};

const loadActiveComplianceIncident = ({ ruleKey, userId = null, containerId = null }) => db.prepare(`
    SELECT *
    FROM compliance_incidents
    WHERE rule_key = ?
      AND COALESCE(user_id, '') = COALESCE(?, '')
      AND COALESCE(container_id, '') = COALESCE(?, '')
      AND status IN ('open', 'investigating')
    ORDER BY datetime(last_detected_at) DESC
    LIMIT 1
`).get(ruleKey, userId, containerId);

const upsertComplianceIncident = ({
    ruleKey,
    userId = null,
    containerId = null,
    severity = 'medium',
    title,
    description,
    evidence = {}
}) => {
    if (!ruleKey || !title || !description) return null;
    const normalizedSeverity = COMPLIANCE_SEVERITIES.has(severity) ? severity : 'medium';
    const now = new Date();
    const nowIso = now.toISOString();
    const nowDb = toSqliteDateTime(nowIso);
    const serializedEvidence = JSON.stringify(evidence || {});

    const existing = loadActiveComplianceIncident({ ruleKey, userId, containerId });
    if (existing) {
        db.prepare(`
            UPDATE compliance_incidents
            SET severity = ?,
                title = ?,
                description = ?,
                evidence_json = ?,
                occurrence_count = occurrence_count + 1,
                last_detected_at = ?,
                updated_at = ?
            WHERE id = ?
        `).run(normalizedSeverity, title, description, serializedEvidence, nowDb, nowDb, existing.id);

        const updated = db.prepare(`SELECT * FROM compliance_incidents WHERE id = ?`).get(existing.id);
        const lastNotifiedIso = toIsoFromSqliteDateTime(updated.last_notified_at);
        const elapsedMs = lastNotifiedIso ? (now.getTime() - new Date(lastNotifiedIso).getTime()) : Number.POSITIVE_INFINITY;
        const shouldNotify = !Number.isFinite(elapsedMs)
            || elapsedMs >= COMPLIANCE_RULE_LIMITS.notificationCooldownMinutes * 60 * 1000;

        if (shouldNotify) {
            sendComplianceDiscordNotification(updated)
                .then((notified) => {
                    if (notified) {
                        db.prepare(`
                            UPDATE compliance_incidents
                            SET last_notified_at = ?, updated_at = ?
                            WHERE id = ?
                        `).run(nowDb, nowDb, updated.id);
                    }
                })
                .catch((notifyError) => {
                    console.error('Compliance Discord notification error:', notifyError);
                });
        }
        return { id: updated.id, created: false };
    }

    const incidentId = `incident-${uuidv4().replace(/-/g, '').slice(0, 16)}`;
    db.prepare(`
        INSERT INTO compliance_incidents (
            id, rule_key, user_id, container_id, severity, status,
            title, description, evidence_json, occurrence_count,
            first_detected_at, last_detected_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, 'open', ?, ?, ?, 1, ?, ?, ?, ?)
    `).run(
        incidentId,
        ruleKey,
        userId,
        containerId,
        normalizedSeverity,
        title,
        description,
        serializedEvidence,
        nowDb,
        nowDb,
        nowDb,
        nowDb
    );

    const createdIncident = db.prepare(`SELECT * FROM compliance_incidents WHERE id = ?`).get(incidentId);
    sendComplianceDiscordNotification(createdIncident)
        .then((notified) => {
            if (notified) {
                db.prepare(`
                    UPDATE compliance_incidents
                    SET last_notified_at = ?, updated_at = ?
                    WHERE id = ?
                `).run(nowDb, nowDb, incidentId);
            }
        })
        .catch((notifyError) => {
            console.error('Compliance Discord notification error:', notifyError);
        });

    return { id: incidentId, created: true };
};

const evaluateLoginIpSpreadRule = (userId) => {
    const loginEvents = Array.from(COMPLIANCE_LOGIN_EVENTS);
    const placeholders = loginEvents.map(() => '?').join(', ');
    const rows = db.prepare(`
        SELECT ip_address, event, created_at
        FROM access_logs
        WHERE user_id = ?
          AND event IN (${placeholders})
          AND datetime(created_at) >= datetime('now', '-24 hours')
        ORDER BY datetime(created_at) DESC
        LIMIT 240
    `).all(userId, ...loginEvents);

    if (rows.length < COMPLIANCE_RULE_LIMITS.loginEventMin24h) return;

    const uniqueIps = Array.from(new Set(
        rows
            .map((row) => String(row.ip_address || '').trim())
            .filter((ip) => ip && ip !== '-')
    ));

    if (uniqueIps.length < COMPLIANCE_RULE_LIMITS.loginUniqueIp24h) return;

    const severity = uniqueIps.length >= COMPLIANCE_RULE_LIMITS.loginUniqueIp24h + 2 ? 'high' : 'medium';
    upsertComplianceIncident({
        ruleKey: 'login_ip_spread_24h',
        userId,
        severity,
        title: '短時間に複数IPからログイン成功',
        description: `24時間以内に ${uniqueIps.length} 種類のIPからログイン成功が記録されました。`,
        evidence: {
            windowHours: 24,
            loginCount: rows.length,
            uniqueIpCount: uniqueIps.length,
            ips: uniqueIps.slice(0, 12),
            lastLoginAt: rows[0]?.created_at || null
        }
    });
};

const evaluateContainerActionBurstRule = (userId) => {
    const targetActions = Array.from(COMPLIANCE_ACTION_BURST_ACTIONS);
    const placeholders = targetActions.map(() => '?').join(', ');
    const rows = db.prepare(`
        SELECT action, container_id, ip_address, created_at
        FROM activity_logs
        WHERE user_id = ?
          AND action IN (${placeholders})
          AND datetime(created_at) >= datetime('now', '-10 minutes')
        ORDER BY datetime(created_at) DESC
        LIMIT 300
    `).all(userId, ...targetActions);

    if (rows.length < COMPLIANCE_RULE_LIMITS.actionBurst10m) return;
    const uniqueContainers = Array.from(new Set(rows.map((row) => row.container_id).filter(Boolean)));
    const severity = rows.length >= COMPLIANCE_RULE_LIMITS.actionBurst10m + 15 ? 'high' : 'medium';

    upsertComplianceIncident({
        ruleKey: 'container_action_burst_10m',
        userId,
        severity,
        title: 'コンテナ操作の急増',
        description: `10分以内に ${rows.length} 件の起動/停止/再起動系操作が実行されました。`,
        evidence: {
            windowMinutes: 10,
            actionCount: rows.length,
            uniqueContainerCount: uniqueContainers.length,
            sampleActions: rows.slice(0, 20),
            lastActionAt: rows[0]?.created_at || null
        }
    });
};

const evaluateSupportSpamRules = (userId) => {
    const ticketCount = db.prepare(`
        SELECT COUNT(1) AS count
        FROM support_tickets
        WHERE user_id = ?
          AND datetime(created_at) >= datetime('now', '-30 minutes')
    `).get(userId)?.count || 0;

    if (ticketCount >= COMPLIANCE_RULE_LIMITS.supportTickets30m) {
        upsertComplianceIncident({
            ruleKey: 'support_ticket_spam_30m',
            userId,
            severity: ticketCount >= COMPLIANCE_RULE_LIMITS.supportTickets30m + 2 ? 'high' : 'medium',
            title: '問い合わせ作成数が閾値超過',
            description: `30分以内の新規問い合わせが ${ticketCount} 件に達しました。`,
            evidence: {
                windowMinutes: 30,
                ticketCount
            }
        });
    }

    const replyCount = db.prepare(`
        SELECT COUNT(1) AS count
        FROM support_messages
        WHERE user_id = ?
          AND sender_role = 'user'
          AND datetime(created_at) >= datetime('now', '-10 minutes')
    `).get(userId)?.count || 0;

    if (replyCount >= COMPLIANCE_RULE_LIMITS.supportReplies10m) {
        upsertComplianceIncident({
            ruleKey: 'support_reply_spam_10m',
            userId,
            severity: replyCount >= COMPLIANCE_RULE_LIMITS.supportReplies10m + 10 ? 'high' : 'medium',
            title: '問い合わせ返信数が閾値超過',
            description: `10分以内の返信が ${replyCount} 件に達しました。`,
            evidence: {
                windowMinutes: 10,
                replyCount
            }
        });
    }
};

const complianceEvaluationLocks = new Set();
const runComplianceEvaluationForUser = (userId) => {
    const normalizedUserId = String(userId || '').trim();
    if (!normalizedUserId) return;
    if (complianceEvaluationLocks.has(normalizedUserId)) return;

    complianceEvaluationLocks.add(normalizedUserId);
    try {
        const targetUser = db.prepare(`
            SELECT id, is_admin
            FROM users
            WHERE id = ?
        `).get(normalizedUserId);
        if (!targetUser) return;

        evaluateLoginIpSpreadRule(normalizedUserId);
        evaluateContainerActionBurstRule(normalizedUserId);

        if (!targetUser.is_admin) {
            evaluateSupportSpamRules(normalizedUserId);
        }
    } catch (error) {
        console.error('Compliance evaluation error:', error);
    } finally {
        complianceEvaluationLocks.delete(normalizedUserId);
    }
};

const scheduleComplianceEvaluation = ({ userId, action = null, event = null }) => {
    const normalizedUserId = String(userId || '').trim();
    if (!normalizedUserId) return;

    const shouldEvaluate = (
        (event && COMPLIANCE_LOGIN_EVENTS.has(String(event))) ||
        (action && (COMPLIANCE_ACTION_BURST_ACTIONS.has(String(action)) || COMPLIANCE_SUPPORT_ACTIVITY_ACTIONS.has(String(action))))
    );
    if (!shouldEvaluate) return;

    setTimeout(() => {
        runComplianceEvaluationForUser(normalizedUserId);
    }, 0);
};

const logActivity = (userId, containerId, action, details, ip) => {
    db.prepare(`
        INSERT INTO activity_logs (user_id, container_id, action, details, ip_address)
        VALUES (?, ?, ?, ?, ?)
    `).run(userId, containerId, action, JSON.stringify(details), ip);

    scheduleComplianceEvaluation({ userId, action });
};

const logAccessHistory = ({ userId, event, req, path = null, method = null }) => {
    if (!userId || !event) return;
    const requestPath = path || req?.originalUrl || req?.path || null;
    const requestMethod = method || req?.method || null;
    const userAgent = String(req?.headers?.['user-agent'] || '').slice(0, 512) || null;
    const clientIp = req ? getClientIp(req) : null;

    db.prepare(`
        INSERT INTO access_logs (user_id, event, path, method, ip_address, user_agent)
        VALUES (?, ?, ?, ?, ?, ?)
    `).run(userId, String(event), requestPath, requestMethod, clientIp, userAgent);

    scheduleComplianceEvaluation({ userId, event });
};

const shortenText = (text, maxLength = 240) => {
    const value = String(text ?? '').replace(/\s+/g, ' ').trim();
    if (value.length <= maxLength) return value;
    return `${value.slice(0, Math.max(1, maxLength - 1))}…`;
};

const sendSupportCreatedDiscordNotification = async ({
    ticketId,
    category,
    subject,
    message,
    userName,
    userEmail
}) => {
    const categoryLabel = SUPPORT_CATEGORY_LABELS[category] || category || '不明';
    const adminUrl = `${CONFIG.RP_ORIGIN}/chat/admin?ticket=${encodeURIComponent(ticketId)}`;
    return sendInternalNotificationEmail({
        subject: `${CONFIG.NOTIFY_SUBJECT_PREFIX} [SUPPORT] 新しいお問い合わせ ${ticketId}`,
        lines: [
            `分類: SUPPORT`,
            `時刻: ${new Date().toISOString()}`,
            `チケットID: ${ticketId}`,
            `カテゴリ: ${categoryLabel}`,
            `ユーザー: ${userName || '-'} (${userEmail || '-'})`,
            `件名: ${shortenText(subject, 160)}`,
            `本文プレビュー: ${shortenText(message, 320)}`,
            `管理画面: ${adminUrl}`
        ]
    });
};

const loadSupportTicketWithUser = (ticketId) => db.prepare(`
    SELECT t.*, u.name AS user_name, u.email AS user_email
    FROM support_tickets t
    JOIN users u ON u.id = t.user_id
    WHERE t.id = ?
`).get(ticketId);

const loadSupportMessages = (ticketId) => db.prepare(`
    SELECT m.id, m.ticket_id, m.user_id, m.sender_role, m.message, m.created_at,
           u.name AS user_name, u.email AS user_email
    FROM support_messages m
    JOIN users u ON u.id = m.user_id
    WHERE m.ticket_id = ?
    ORDER BY m.id ASC
`).all(ticketId);

const listSupportTickets = ({ includeAll = false, userId = null } = {}) => {
    const whereClause = includeAll ? '' : 'WHERE t.user_id = ?';
    const params = includeAll ? [] : [userId];
    return db.prepare(`
        SELECT
            t.id,
            t.user_id,
            t.category,
            t.subject,
            t.status,
            t.created_at,
            t.updated_at,
            t.closed_at,
            u.name AS user_name,
            u.email AS user_email,
            (
                SELECT m.sender_role
                FROM support_messages m
                WHERE m.ticket_id = t.id
                ORDER BY m.id DESC
                LIMIT 1
            ) AS last_sender_role,
            (
                SELECT m.message
                FROM support_messages m
                WHERE m.ticket_id = t.id
                ORDER BY m.id DESC
                LIMIT 1
            ) AS last_message,
            (
                SELECT m.created_at
                FROM support_messages m
                WHERE m.ticket_id = t.id
                ORDER BY m.id DESC
                LIMIT 1
            ) AS last_message_at,
            (
                SELECT COUNT(1)
                FROM support_messages m
                WHERE m.ticket_id = t.id
            ) AS message_count
        FROM support_tickets t
        JOIN users u ON u.id = t.user_id
        ${whereClause}
        ORDER BY datetime(COALESCE((
            SELECT m.created_at
            FROM support_messages m
            WHERE m.ticket_id = t.id
            ORDER BY m.id DESC
            LIMIT 1
        ), t.updated_at)) DESC
    `).all(...params);
};

const ensureSupportTicketAccess = (ticketId, viewerUser) => {
    const ticket = loadSupportTicketWithUser(ticketId);
    if (!ticket) {
        return { ok: false, status: 404, error: '問い合わせが見つかりません' };
    }
    if (!viewerUser?.is_admin && ticket.user_id !== viewerUser?.id) {
        return { ok: false, status: 403, error: 'この問い合わせにはアクセスできません' };
    }
    return { ok: true, ticket };
};

const generatePassword = () => {
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let password = '';
    for (let i = 0; i < 16; i++) {
        password += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return password;
};

const upsertVmTtydStableLink = ({
    containerId,
    vmName,
    targetUrl,
    username,
    password,
    ttydPort
}) => {
    const existing = db.prepare(`
        SELECT link_token FROM vm_ttyd_links WHERE container_id = ?
    `).get(containerId);
    const linkToken = existing?.link_token || generatePublicLinkToken();

    if (existing) {
        db.prepare(`
            UPDATE vm_ttyd_links
            SET vm_name = ?, target_url = ?, username = ?, password = ?, ttyd_port = ?, updated_at = CURRENT_TIMESTAMP
            WHERE container_id = ?
        `).run(vmName, targetUrl, username, password, ttydPort, containerId);
    } else {
        db.prepare(`
            INSERT INTO vm_ttyd_links (container_id, vm_name, link_token, target_url, username, password, ttyd_port)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        `).run(containerId, vmName, linkToken, targetUrl, username, password, ttydPort);
    }

    return linkToken;
};

const loadVmTtydLinkByContainerId = (containerId) => db.prepare(`
    SELECT id, container_id, vm_name, link_token, target_url, username, password, ttyd_port
    FROM vm_ttyd_links
    WHERE container_id = ?
`).get(containerId);

const ensureVmTtydStableLinkSeed = ({
    containerId,
    vmName,
    username,
    password,
    ttydPort
}) => {
    const safePort = normalizeVmTtydPort(ttydPort, normalizeVmTtydPort(CONFIG.VM_TTYD_PORT, 7681));
    const fallbackTargetUrl = `http://127.0.0.1:${safePort}`;
    const fallbackUser = sanitizeTtydAuthUser(username || 'admin', 'admin');
    const fallbackPassword = String(password || '').trim() || generateTunnelPassword();
    const existing = loadVmTtydLinkByContainerId(containerId);

    if (!existing) {
        const linkToken = generatePublicLinkToken();
        db.prepare(`
            INSERT INTO vm_ttyd_links (container_id, vm_name, link_token, target_url, username, password, ttyd_port)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        `).run(containerId, vmName, linkToken, fallbackTargetUrl, fallbackUser, fallbackPassword, safePort);

        return {
            link_token: linkToken,
            target_url: fallbackTargetUrl,
            username: fallbackUser,
            password: fallbackPassword,
            ttyd_port: safePort
        };
    }

    const nextVmName = String(existing.vm_name || '').trim() || vmName;
    const nextTargetUrl = String(existing.target_url || '').trim() || fallbackTargetUrl;
    const nextUser = sanitizeTtydAuthUser(existing.username || fallbackUser, fallbackUser);
    const nextPassword = String(existing.password || '').trim() || fallbackPassword;
    const nextPort = normalizeVmTtydPort(existing.ttyd_port, safePort);

    if (
        nextVmName !== existing.vm_name ||
        nextTargetUrl !== existing.target_url ||
        nextUser !== (existing.username || '') ||
        nextPassword !== (existing.password || '') ||
        nextPort !== normalizeVmTtydPort(existing.ttyd_port, safePort)
    ) {
        db.prepare(`
            UPDATE vm_ttyd_links
            SET vm_name = ?, target_url = ?, username = ?, password = ?, ttyd_port = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        `).run(nextVmName, nextTargetUrl, nextUser, nextPassword, nextPort, existing.id);
    }

    return {
        ...existing,
        vm_name: nextVmName,
        target_url: nextTargetUrl,
        username: nextUser,
        password: nextPassword,
        ttyd_port: nextPort
    };
};

const VM_TTYD_TOKEN_PATTERN = /^[a-zA-Z0-9_-]{16,128}$/;
const HOP_BY_HOP_HEADERS = new Set([
    'connection',
    'keep-alive',
    'proxy-authenticate',
    'proxy-authorization',
    'te',
    'trailers',
    'transfer-encoding',
    'upgrade'
]);
const vmTtydRefreshJobs = new Map();

const loadVmTtydLinkByToken = (token) => db.prepare(`
    SELECT id, vm_name, target_url, username, password, ttyd_port
    FROM vm_ttyd_links
    WHERE link_token = ?
`).get(token);

const parseVmTtydOriginalUrl = (token, rawUrl) => {
    const parsed = new URL(rawUrl, 'http://localhost');
    const prefix = `/vm-ttyd/${token}`;
    if (!parsed.pathname.startsWith(prefix)) {
        return null;
    }

    const suffixPath = parsed.pathname.slice(prefix.length) || '/';
    return {
        path: suffixPath.startsWith('/') ? suffixPath : `/${suffixPath}`,
        search: parsed.search || ''
    };
};

const normalizeVmTtydTargetUrl = (targetUrl) => {
    const raw = String(targetUrl || '').trim();
    if (!raw) return null;
    try {
        const parsed = new URL(raw);
        if (!['https:', 'http:'].includes(parsed.protocol)) return null;
        return parsed;
    } catch {
        return null;
    }
};

const buildVmTtydProxyContext = async (token, rawUrl) => {
    const record = loadVmTtydLinkByToken(token);
    if (!record?.target_url) {
        return { ok: false, statusCode: 404, error: 'Link not found' };
    }

    const targetBase = normalizeVmTtydTargetUrl(record.target_url);
    if (!targetBase) {
        return { ok: false, statusCode: 500, error: 'Invalid target URL' };
    }

    const pathInfo = parseVmTtydOriginalUrl(token, rawUrl);
    if (!pathInfo) {
        return { ok: false, statusCode: 404, error: 'Not found' };
    }

    const upstreamUrl = new URL(targetBase.toString());
    upstreamUrl.pathname = pathInfo.path;
    upstreamUrl.search = pathInfo.search;

    return {
        ok: true,
        record,
        upstreamUrl
    };
};

const readRequestBody = async (req) => {
    const chunks = [];
    for await (const chunk of req) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return chunks.length > 0 ? Buffer.concat(chunks) : Buffer.alloc(0);
};

const buildVmTtydProxyHeaders = (incomingHeaders, username, password) => {
    const headers = {};
    for (const [key, value] of Object.entries(incomingHeaders || {})) {
        if (!value) continue;
        const lowerKey = key.toLowerCase();
        if (
            HOP_BY_HOP_HEADERS.has(lowerKey) ||
            lowerKey === 'host' ||
            lowerKey === 'content-length' ||
            lowerKey === 'accept-encoding'
        ) {
            continue;
        }
        headers[key] = value;
    }
    headers['accept-encoding'] = 'identity';

    if (username && password) {
        const basic = Buffer.from(`${username}:${password}`).toString('base64');
        headers.Authorization = `Basic ${basic}`;
    }

    return headers;
};

const fetchVmTtydHttp = async (upstreamUrl, {
    method,
    headers,
    body,
    username,
    password
}) => {
    try {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), 8000);
        const response = await fetch(upstreamUrl, {
            method,
            redirect: 'manual',
            headers: buildVmTtydProxyHeaders(headers, username, password),
            body,
            signal: controller.signal
        });
        clearTimeout(timer);
        if (response.status >= 500) {
            return { ok: false, response, canRetry: true };
        }
        return { ok: true, response, canRetry: false };
    } catch (error) {
        return { ok: false, error, canRetry: true };
    }
};

const pipeVmTtydResponse = (response, res) => {
    res.status(response.status);
    response.headers.forEach((value, key) => {
        const lowerKey = key.toLowerCase();
        if (HOP_BY_HOP_HEADERS.has(lowerKey)) return;
        if (lowerKey === 'content-encoding' || lowerKey === 'content-length') return;
        res.setHeader(key, value);
    });

    if (!response.body) {
        res.end();
        return;
    }

    Readable.fromWeb(response.body).pipe(res);
};

const sendVmTtydUnavailable = (res) => {
    res.status(503);
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.setHeader('Cache-Control', 'no-store');
    res.send(`<!doctype html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Webターミナル準備中</title>
  <style>
    body { font-family: sans-serif; background:#0f172a; color:#e2e8f0; margin:0; display:grid; place-items:center; min-height:100vh; }
    .box { background:#111827; border:1px solid #334155; border-radius:12px; padding:24px; max-width:560px; width:92%; }
    h1 { margin:0 0 12px; font-size:20px; }
    p { margin:6px 0; line-height:1.6; }
    code { background:#1f2937; padding:2px 6px; border-radius:6px; }
  </style>
</head>
<body>
  <div class="box">
    <h1>Webターミナルを準備中です</h1>
    <p>一時リンクを再発行しています。数秒後に自動で再読み込みします。</p>
    <p>進まない場合は <code>5-10秒</code> 後にページを再読み込みしてください。</p>
  </div>
  <script>setTimeout(() => location.reload(), 5000);</script>
</body>
</html>`);
};

const isUrlReachable = async (url, timeoutMs = 6000) => {
    try {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        const response = await fetch(url, { method: 'GET', redirect: 'manual', signal: controller.signal });
        clearTimeout(timer);
        return Number.isFinite(response.status) && response.status > 0;
    } catch {
        return false;
    }
};

const refreshVmTtydStableTarget = async (record) => {
    if (!record?.vm_name) return null;

    const vmInfo = await getVmBackendInfo(record.vm_name);
    if (!vmInfo?.available) return null;

    if (vmInfo.status !== 'running') {
        const startResult = await runCommand('virsh', ['start', record.vm_name], 10000);
        const alreadyRunning = /already active|already running/i.test(startResult.stderr || '');
        if (!startResult.ok && !alreadyRunning) {
            return null;
        }
        await sleep(1500);
    }

    const preferredPort = normalizeVmTtydPort(record.ttyd_port, normalizeVmTtydPort(CONFIG.VM_TTYD_PORT, 7681));
    const authUser = sanitizeTtydAuthUser(record.username || 'admin', 'admin');
    const authPass = String(record.password || '').trim() || generateTunnelPassword();

    const endpoint = await ensureVmTtydDirectEndpoint(record.vm_name, {
        port: preferredPort,
        authUser,
        authPass
    });
    if (!endpoint.ok) {
        console.error('VM ttyd direct refresh failed:', {
            id: record.id,
            vm: record.vm_name,
            error: endpoint.error
        });
        return null;
    }

    db.prepare(`
        UPDATE vm_ttyd_links
        SET target_url = ?, username = ?, password = ?, ttyd_port = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    `).run(endpoint.url, endpoint.username, endpoint.password, endpoint.port, record.id);

    return {
        url: endpoint.url
    };
};

const scheduleVmTtydRefresh = (record, reason = 'unknown') => {
    if (!record?.id) return;
    if (vmTtydRefreshJobs.has(record.id)) return;

    const job = refreshVmTtydStableTarget(record)
        .catch((error) => {
            console.error('VM ttyd refresh failed:', {
                id: record.id,
                vm: record.vm_name,
                reason,
                error: error?.message || String(error)
            });
        })
        .finally(() => {
            vmTtydRefreshJobs.delete(record.id);
        });

    vmTtydRefreshJobs.set(record.id, {
        startedAt: Date.now(),
        reason,
        job
    });
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

const toFileTimestamp = (date = new Date()) => {
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, '0');
    const d = String(date.getDate()).padStart(2, '0');
    const hh = String(date.getHours()).padStart(2, '0');
    const mm = String(date.getMinutes()).padStart(2, '0');
    const ss = String(date.getSeconds()).padStart(2, '0');
    return `${y}${m}${d}-${hh}${mm}${ss}`;
};

const sendOpsDiscordNotification = async ({
    title,
    description,
    color = 5814783,
    fields = []
}) => {
    const normalizedFields = Array.isArray(fields) ? fields : [];
    const fieldLines = normalizedFields.map((field) => {
        const name = String(field?.name || '-');
        const value = String(field?.value ?? '-').replace(/\r\n/g, '\n');
        return `${name}: ${value}`;
    });
    return sendInternalNotificationEmail({
        subject: `${CONFIG.NOTIFY_SUBJECT_PREFIX} [OPS] ${title || '運用アラート'}`,
        lines: [
            `分類: OPS`,
            `時刻: ${new Date().toISOString()}`,
            `概要: ${description || '-'}`,
            ...fieldLines
        ]
    });
};

const rateLimitBuckets = new Map();
const createRateLimiter = ({
    id,
    windowMs,
    max,
    keyFn,
    message
}) => (req, res, next) => {
    if (!RATE_LIMIT_CONFIG.enabled) return next();

    const identifier = String(keyFn?.(req) || getClientIp(req) || 'unknown');
    const bucketKey = `${id}:${identifier}`;
    const now = Date.now();
    let bucket = rateLimitBuckets.get(bucketKey);

    if (!bucket || bucket.expiresAt <= now) {
        bucket = { count: 0, expiresAt: now + windowMs };
    }

    if (bucket.count >= max) {
        const retryAfterSec = Math.max(1, Math.ceil((bucket.expiresAt - now) / 1000));
        res.setHeader('Retry-After', String(retryAfterSec));
        return res.status(429).json({
            error: message || 'アクセスが集中しています。しばらくしてから再試行してください。',
            retry_after_sec: retryAfterSec
        });
    }

    bucket.count += 1;
    rateLimitBuckets.set(bucketKey, bucket);
    next();
};

setInterval(() => {
    const now = Date.now();
    for (const [key, bucket] of rateLimitBuckets.entries()) {
        if (!bucket || bucket.expiresAt <= now) {
            rateLimitBuckets.delete(key);
        }
    }
}, 60 * 1000);

const authRateLimit = createRateLimiter({
    id: 'auth',
    windowMs: RATE_LIMIT_CONFIG.authWindowMs,
    max: RATE_LIMIT_CONFIG.authMax,
    keyFn: (req) => getClientIp(req),
    message: '認証試行が多すぎます。時間を置いて再試行してください。'
});

const supportCreateRateLimit = createRateLimiter({
    id: 'support-create',
    windowMs: RATE_LIMIT_CONFIG.supportCreateWindowMs,
    max: RATE_LIMIT_CONFIG.supportCreateMax,
    keyFn: (req) => req.user?.id || getClientIp(req),
    message: '問い合わせ作成の上限に達しました。時間を置いて再試行してください。'
});

const supportReplyRateLimit = createRateLimiter({
    id: 'support-reply',
    windowMs: RATE_LIMIT_CONFIG.supportReplyWindowMs,
    max: RATE_LIMIT_CONFIG.supportReplyMax,
    keyFn: (req) => req.user?.id || getClientIp(req),
    message: '返信送信が多すぎます。時間を置いて再試行してください。'
});

const billingRateLimit = createRateLimiter({
    id: 'billing',
    windowMs: RATE_LIMIT_CONFIG.billingWindowMs,
    max: RATE_LIMIT_CONFIG.billingMax,
    keyFn: (req) => req.user?.id || getClientIp(req),
    message: '決済操作が多すぎます。時間を置いて再試行してください。'
});

const adminRateLimit = createRateLimiter({
    id: 'admin',
    windowMs: RATE_LIMIT_CONFIG.adminWindowMs,
    max: RATE_LIMIT_CONFIG.adminMax,
    keyFn: (req) => req.user?.id || getClientIp(req),
    message: '管理APIの呼び出しが多すぎます。時間を置いて再試行してください。'
});

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

let autoBackupRunning = false;
let autoBackupTimer = null;
let systemMonitorTimer = null;
let lastAutoBackupAt = null;
const MONITOR_RUNTIME_EVENT_TTL_MS = 24 * 60 * 60 * 1000;
const MONITOR_RUNTIME_EVENT_LIMIT = 2000;
const monitorRuntimeEvents = {
    api5xx: [],
    backupFailures: [],
    stripeWebhookFailures: []
};
let lastSystemMonitorSummary = {
    updated_at: null,
    api_5xx_recent_count: 0,
    backup_failure_recent_count: 0,
    stripe_failure_recent_count: 0,
    vm_mismatch_count: 0,
    backup_stale: false
};
const AUTO_BACKUP_FILE_PREFIX = 'auto-system-';
const AUTO_BACKUP_FILE_SUFFIX = '.tar.gz';

const pruneMonitorRuntimeEvents = (bucketName) => {
    const bucket = monitorRuntimeEvents[bucketName];
    if (!Array.isArray(bucket) || bucket.length === 0) return;
    const cutoff = Date.now() - MONITOR_RUNTIME_EVENT_TTL_MS;
    while (bucket.length && bucket[0].atMs < cutoff) {
        bucket.shift();
    }
    if (bucket.length > MONITOR_RUNTIME_EVENT_LIMIT) {
        bucket.splice(0, bucket.length - MONITOR_RUNTIME_EVENT_LIMIT);
    }
};

const pushSystemMonitorEvent = (bucketName, payload = {}) => {
    const bucket = monitorRuntimeEvents[bucketName];
    if (!Array.isArray(bucket)) return;
    const now = Date.now();
    bucket.push({
        atMs: now,
        atIso: new Date(now).toISOString(),
        ...payload
    });
    pruneMonitorRuntimeEvents(bucketName);
};

const getRecentSystemMonitorEvents = (bucketName, windowMs) => {
    const bucket = monitorRuntimeEvents[bucketName];
    if (!Array.isArray(bucket) || bucket.length === 0) return [];
    pruneMonitorRuntimeEvents(bucketName);
    if (!Number.isFinite(windowMs) || windowMs <= 0) {
        return bucket.slice();
    }
    const cutoff = Date.now() - windowMs;
    return bucket.filter((event) => event.atMs >= cutoff);
};

const summarizeRecentApi5xxEvents = (events = [], limit = 5) => {
    const counters = new Map();
    for (const event of events) {
        const method = String(event.method || 'GET').toUpperCase();
        const pathOnly = String(event.path || '/').split('?')[0] || '/';
        const key = `${method} ${pathOnly}`;
        counters.set(key, (counters.get(key) || 0) + 1);
    }
    return Array.from(counters.entries())
        .sort((a, b) => b[1] - a[1])
        .slice(0, Math.max(1, limit))
        .map(([key, count]) => `${key} (${count})`);
};

const listAutoBackupFiles = () => {
    const files = [];
    try {
        for (const name of fs.readdirSync(CONFIG.BACKUPS_DIR)) {
            if (!name.startsWith(AUTO_BACKUP_FILE_PREFIX) || !name.endsWith(AUTO_BACKUP_FILE_SUFFIX)) continue;
            const fullPath = path.join(CONFIG.BACKUPS_DIR, name);
            const stat = fs.statSync(fullPath);
            if (!stat.isFile()) continue;
            files.push({
                name,
                path: fullPath,
                mtimeMs: stat.mtimeMs,
                size: stat.size
            });
        }
    } catch (e) {
        console.error('List auto backup files error:', e);
    }
    return files.sort((a, b) => b.mtimeMs - a.mtimeMs);
};

const pruneAutoBackups = () => {
    const retentionMs = AUTO_BACKUP_CONFIG.retentionDays * 24 * 60 * 60 * 1000;
    const cutoff = Date.now() - retentionMs;
    const files = listAutoBackupFiles();
    let removed = 0;

    for (const file of files) {
        if (file.mtimeMs >= cutoff) continue;
        try {
            fs.unlinkSync(file.path);
            removed += 1;
        } catch (e) {
            console.error('Auto backup prune failed:', file.path, e.message);
        }
    }
    return removed;
};

const addTarSource = (sources, baseDir, entries = []) => {
    if (!baseDir || !Array.isArray(entries) || entries.length === 0) return;
    const normalizedEntries = entries
        .map((entry) => String(entry || '').trim())
        .filter(Boolean);
    if (!normalizedEntries.length) return;
    sources.push({
        baseDir,
        entries: normalizedEntries
    });
};

const summarizeDirectoryFiles = (dirPath) => {
    const summary = {
        fileCount: 0,
        totalSizeBytes: 0
    };
    try {
        const rootStat = fs.statSync(dirPath);
        if (!rootStat.isDirectory()) return summary;
    } catch (e) {
        return summary;
    }

    const queue = [dirPath];
    while (queue.length > 0) {
        const current = queue.pop();
        let entries = [];
        try {
            entries = fs.readdirSync(current, { withFileTypes: true });
        } catch (e) {
            continue;
        }
        for (const entry of entries) {
            const fullPath = path.join(current, entry.name);
            if (entry.isDirectory()) {
                queue.push(fullPath);
                continue;
            }
            if (!entry.isFile()) continue;
            summary.fileCount += 1;
            try {
                summary.totalSizeBytes += fs.statSync(fullPath).size;
            } catch (e) {}
        }
    }
    return summary;
};

const getFilesystemAvailableBytes = async (targetPath) => {
    const result = await runCommand('df', ['-Pk', targetPath], 5000);
    if (!result.ok) return null;
    const lines = String(result.stdout || '').trim().split('\n').filter(Boolean);
    if (lines.length < 2) return null;
    const cols = lines[lines.length - 1].trim().split(/\s+/);
    if (!cols[3]) return null;
    const availableKb = Number.parseInt(cols[3], 10);
    if (!Number.isFinite(availableKb) || availableKb < 0) return null;
    return availableKb * 1024;
};

const sumFileSizes = (baseDir, names = []) => {
    let total = 0;
    for (const name of names) {
        try {
            total += fs.statSync(path.join(baseDir, name)).size;
        } catch (e) {}
    }
    return total;
};

const collectVmDomainMetadata = async (metadataDirPath) => {
    const vmDefinitionsDir = path.join(metadataDirPath, 'vm-definitions');
    fs.mkdirSync(vmDefinitionsDir, { recursive: true });
    const info = {
        domainCount: 0,
        dumpedCount: 0,
        failedCount: 0,
        failedDomains: []
    };

    const listResult = await runCommand('virsh', ['list', '--all', '--name'], 15000);
    if (!listResult.ok) {
        info.failedCount = 1;
        info.failedDomains.push(`virsh-list: ${listResult.stderr || 'list_failed'}`);
        return info;
    }

    const domains = String(listResult.stdout || '')
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean);
    info.domainCount = domains.length;

    for (const domain of domains) {
        const safeName = String(domain).replace(/[^a-zA-Z0-9_.-]/g, '_') || 'unknown-domain';
        const xmlResult = await runCommand('virsh', ['dumpxml', domain], 20000);
        if (!xmlResult.ok || !xmlResult.stdout) {
            info.failedCount += 1;
            info.failedDomains.push(`${domain}: ${xmlResult.stderr || 'dumpxml_failed'}`);
            continue;
        }
        try {
            fs.writeFileSync(path.join(vmDefinitionsDir, `${safeName}.xml`), `${xmlResult.stdout}\n`, 'utf8');
            info.dumpedCount += 1;
        } catch (e) {
            info.failedCount += 1;
            info.failedDomains.push(`${domain}: ${e.message || 'write_failed'}`);
        }
    }

    if (info.failedDomains.length > 20) {
        info.failedDomains = info.failedDomains.slice(0, 20);
    }
    return info;
};

const createSystemBackupSnapshot = async ({
    reason = 'scheduled',
    actorUserId = null,
    force = false,
    modeOverride = null
} = {}) => {
    if (!AUTO_BACKUP_CONFIG.enabled && !force) {
        return { ok: false, skipped: true, reason: 'disabled' };
    }
    if (autoBackupRunning) {
        return { ok: false, skipped: true, reason: 'in_progress' };
    }

    autoBackupRunning = true;
    const startedAt = Date.now();
    const mode = normalizeAutoBackupMode(modeOverride, AUTO_BACKUP_CONFIG.mode);
    const backupTimeoutMs = Math.max(60 * 1000, AUTO_BACKUP_CONFIG.timeoutMinutes * 60 * 1000);
    const fileTimestamp = toFileTimestamp(new Date(startedAt));
    const safeReason = String(reason || 'manual').replace(/[^a-zA-Z0-9_-]/g, '-').slice(0, 24) || 'manual';
    const backupName = `${AUTO_BACKUP_FILE_PREFIX}${mode}-${safeReason}-${fileTimestamp}${AUTO_BACKUP_FILE_SUFFIX}`;
    const backupPath = path.join(CONFIG.BACKUPS_DIR, backupName);
    let tempRoot = null;

    try {
        const tarSources = [];
        const componentNames = [];

        const dbDir = path.dirname(CONFIG.DB_PATH);
        const dbBase = path.basename(CONFIG.DB_PATH);
        const targets = [dbBase, `${dbBase}-wal`, `${dbBase}-shm`]
            .filter((name) => fs.existsSync(path.join(dbDir, name)));
        if (targets.length === 0) {
            throw new Error('backup source db file not found');
        }
        addTarSource(tarSources, dbDir, targets);
        componentNames.push('db');

        const dbSizeBytes = sumFileSizes(dbDir, targets);
        const vmPoolPath = path.resolve(CONFIG.VM_POOL_DIR || '/tmp/libvirt-images');
        let containersSummary = {
            path: CONFIG.CONTAINERS_DIR,
            fileCount: 0,
            totalSizeBytes: 0,
            included: false
        };
        let vmPoolSummary = {
            path: vmPoolPath,
            fileCount: 0,
            totalSizeBytes: 0,
            included: false
        };

        tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'shironeko-backup-meta-'));
        const metadataDir = path.join(tempRoot, 'system-backup-meta');
        fs.mkdirSync(metadataDir, { recursive: true });
        const vmMetadata = await collectVmDomainMetadata(metadataDir);
        const containersMeta = db.prepare(`
            SELECT id, user_id, name, type, status, docker_id, created_at, updated_at
            FROM containers
            ORDER BY datetime(updated_at) DESC
        `).all();
        fs.writeFileSync(path.join(metadataDir, 'containers-index.json'), JSON.stringify(containersMeta, null, 2), 'utf8');

        if (mode === 'full') {
            try {
                const containersStat = fs.statSync(CONFIG.CONTAINERS_DIR);
                if (containersStat.isDirectory()) {
                    addTarSource(tarSources, path.dirname(CONFIG.CONTAINERS_DIR), [path.basename(CONFIG.CONTAINERS_DIR)]);
                    containersSummary = {
                        path: CONFIG.CONTAINERS_DIR,
                        ...summarizeDirectoryFiles(CONFIG.CONTAINERS_DIR),
                        included: true
                    };
                    componentNames.push('containers');
                }
            } catch (e) {}

            try {
                const vmPoolStat = fs.statSync(vmPoolPath);
                if (vmPoolStat.isDirectory()) {
                    addTarSource(tarSources, path.dirname(vmPoolPath), [path.basename(vmPoolPath)]);
                    vmPoolSummary = {
                        path: vmPoolPath,
                        ...summarizeDirectoryFiles(vmPoolPath),
                        included: true
                    };
                    componentNames.push('vm-images');
                }
            } catch (e) {}
        }

        const metadataSummary = summarizeDirectoryFiles(metadataDir);
        const estimatedSizeBytes = dbSizeBytes
            + (containersSummary.included ? containersSummary.totalSizeBytes : 0)
            + (vmPoolSummary.included ? vmPoolSummary.totalSizeBytes : 0)
            + metadataSummary.totalSizeBytes;
        const availableBytes = await getFilesystemAvailableBytes(CONFIG.BACKUPS_DIR);
        const reserveBytes = 512 * 1024 * 1024;
        if (availableBytes != null && availableBytes < (estimatedSizeBytes + reserveBytes)) {
            throw new Error(
                `insufficient free space for backup (required~${formatBytes(estimatedSizeBytes + reserveBytes)}, available=${formatBytes(availableBytes)})`
            );
        }

        componentNames.push('metadata');
        const uniqueComponents = Array.from(new Set(componentNames));
        const manifest = {
            backup_name: backupName,
            mode,
            reason: safeReason,
            started_at: new Date(startedAt).toISOString(),
            created_at: new Date().toISOString(),
            db_path: CONFIG.DB_PATH,
            containers_dir: CONFIG.CONTAINERS_DIR,
            vm_pool_dir: vmPoolPath,
            estimated_size_bytes: estimatedSizeBytes,
            available_bytes_before_backup: availableBytes,
            components: uniqueComponents,
            vm_metadata: vmMetadata,
            db_summary: {
                files: targets,
                totalSizeBytes: dbSizeBytes
            },
            containers_summary: containersSummary,
            vm_pool_summary: vmPoolSummary
        };
        fs.writeFileSync(path.join(metadataDir, 'backup-manifest.json'), JSON.stringify(manifest, null, 2), 'utf8');
        addTarSource(tarSources, tempRoot, ['system-backup-meta']);

        const tarArgs = ['-czf', backupPath];
        for (const source of tarSources) {
            tarArgs.push('-C', source.baseDir, ...source.entries);
        }

        const backupResult = await runCommand('tar', tarArgs, backupTimeoutMs);
        if (!backupResult.ok) {
            throw new Error(backupResult.stderr || 'tar backup failed');
        }

        if (AUTO_BACKUP_CONFIG.restoreCheckEnabled) {
            const verifyTimeoutMs = mode === 'full'
                ? Math.max(120000, Math.min(backupTimeoutMs, 30 * 60 * 1000))
                : 60000;
            const verifyResult = await runCommand('tar', ['-tzf', backupPath], verifyTimeoutMs);
            if (!verifyResult.ok) {
                throw new Error(`restore check failed: ${verifyResult.stderr || 'tar -tzf failed'}`);
            }
        }

        const removedCount = pruneAutoBackups();
        const stat = fs.statSync(backupPath);
        lastAutoBackupAt = new Date().toISOString();

        if (AUTO_BACKUP_CONFIG.notifyEnabled) {
            sendOpsDiscordNotification({
                title: '自動バックアップ完了',
                description: `バックアップを作成しました: ${backupName}`,
                color: 3066993,
                fields: [
                    { name: 'モード', value: mode.toUpperCase(), inline: true },
                    { name: '理由', value: safeReason, inline: true },
                    { name: 'サイズ', value: formatBytes(stat.size), inline: true },
                    { name: '対象', value: Array.from(new Set(componentNames)).join(', ') || 'db', inline: false },
                    { name: '削除件数', value: String(removedCount), inline: true }
                ]
            }).catch((notifyError) => {
                console.error('Auto backup notify error:', notifyError);
            });
        }

        if (actorUserId) {
            db.prepare(`
                INSERT INTO activity_logs (user_id, container_id, action, details, ip_address)
                VALUES (?, ?, ?, ?, ?)
            `).run(actorUserId, null, 'system_backup_manual', JSON.stringify({
                backup_name: backupName,
                size: stat.size,
                mode,
                components: Array.from(new Set(componentNames))
            }), '-');
        }

        return {
            ok: true,
            backupName,
            backupPath,
            mode,
            components: Array.from(new Set(componentNames)),
            sizeBytes: stat.size,
            removedCount,
            elapsedMs: Date.now() - startedAt
        };
    } catch (e) {
        pushSystemMonitorEvent('backupFailures', {
            reason: `${mode}:${safeReason}`,
            message: String(e?.message || e || 'backup_failed')
        });
        if (AUTO_BACKUP_CONFIG.notifyEnabled) {
            sendOpsDiscordNotification({
                title: '自動バックアップ失敗',
                description: String(e.message || e),
                color: 15158332
            }).catch((notifyError) => {
                console.error('Auto backup error notify failed:', notifyError);
            });
        }
        return { ok: false, error: String(e.message || e) };
    } finally {
        if (tempRoot) {
            try {
                fs.rmSync(tempRoot, { recursive: true, force: true });
            } catch (e) {}
        }
        autoBackupRunning = false;
    }
};

const parseDiskUsagePercent = (dfOutput = '') => {
    const lines = String(dfOutput || '').trim().split('\n').filter(Boolean);
    if (lines.length < 2) return null;
    const targetLine = lines[lines.length - 1];
    const cols = targetLine.trim().split(/\s+/);
    if (!cols[4]) return null;
    const match = cols[4].match(/(\d+)%/);
    if (!match) return null;
    const value = Number.parseInt(match[1], 10);
    return Number.isFinite(value) ? value : null;
};

const collectSystemHealthSnapshot = async () => {
    const mem = process.memoryUsage();
    const rssMb = mem.rss / (1024 * 1024);
    const heapUsedMb = mem.heapUsed / (1024 * 1024);

    const dbFiles = [CONFIG.DB_PATH, `${CONFIG.DB_PATH}-wal`, `${CONFIG.DB_PATH}-shm`];
    let dbSizeBytes = 0;
    for (const filePath of dbFiles) {
        if (!fs.existsSync(filePath)) continue;
        try {
            dbSizeBytes += fs.statSync(filePath).size;
        } catch (e) {}
    }

    let diskUsagePercent = null;
    const dfResult = await runCommand('df', ['-Pk', CONFIG.DATA_DIR], 5000);
    if (dfResult.ok) {
        diskUsagePercent = parseDiskUsagePercent(dfResult.stdout);
    }

    const api5xxRecentCount = getRecentSystemMonitorEvents(
        'api5xx',
        SYSTEM_MONITOR_CONFIG.api5xxWindowSeconds * 1000
    ).length;
    const backupFailureRecentCount = getRecentSystemMonitorEvents(
        'backupFailures',
        SYSTEM_MONITOR_CONFIG.backupFailureWindowMinutes * 60 * 1000
    ).length;
    const stripeFailureRecentCount = getRecentSystemMonitorEvents(
        'stripeWebhookFailures',
        SYSTEM_MONITOR_CONFIG.stripeFailureWindowMinutes * 60 * 1000
    ).length;

    return {
        timestamp: new Date().toISOString(),
        uptimeSec: Math.floor(process.uptime()),
        rssMb: Number(rssMb.toFixed(1)),
        heapUsedMb: Number(heapUsedMb.toFixed(1)),
        dbSizeBytes,
        dbSizeMb: Number((dbSizeBytes / (1024 * 1024)).toFixed(2)),
        diskUsagePercent,
        wsClients: wss.clients.size,
        autoBackup: {
            enabled: AUTO_BACKUP_CONFIG.enabled,
            mode: AUTO_BACKUP_CONFIG.mode,
            startupMode: AUTO_BACKUP_CONFIG.startupMode,
            timeoutMinutes: AUTO_BACKUP_CONFIG.timeoutMinutes,
            running: autoBackupRunning,
            lastRunAt: lastAutoBackupAt
        },
        monitor: {
            api5xx_recent_count: api5xxRecentCount,
            backup_failure_recent_count: backupFailureRecentCount,
            stripe_failure_recent_count: stripeFailureRecentCount,
            last_summary: lastSystemMonitorSummary
        }
    };
};

const monitorAlertState = new Map();
const shouldEmitMonitorAlert = (key) => {
    const cooldownMs = SYSTEM_MONITOR_CONFIG.alertCooldownMinutes * 60 * 1000;
    const now = Date.now();
    const lastAt = monitorAlertState.get(key) || 0;
    if (now - lastAt < cooldownMs) return false;
    monitorAlertState.set(key, now);
    return true;
};

const runSystemMonitorTick = async () => {
    if (!SYSTEM_MONITOR_CONFIG.enabled) return null;

    const snapshot = await collectSystemHealthSnapshot();
    const alerts = [];
    const nowMs = Date.now();
    const api5xxEvents = getRecentSystemMonitorEvents(
        'api5xx',
        SYSTEM_MONITOR_CONFIG.api5xxWindowSeconds * 1000
    );
    const backupFailureEvents = getRecentSystemMonitorEvents(
        'backupFailures',
        SYSTEM_MONITOR_CONFIG.backupFailureWindowMinutes * 60 * 1000
    );
    const runtimeStripeFailureEvents = getRecentSystemMonitorEvents(
        'stripeWebhookFailures',
        SYSTEM_MONITOR_CONFIG.stripeFailureWindowMinutes * 60 * 1000
    );
    const stripeWindowExpr = `-${SYSTEM_MONITOR_CONFIG.stripeFailureWindowMinutes} minutes`;
    const stripeFailureCountRow = db.prepare(`
        SELECT COUNT(*) AS count
        FROM stripe_webhook_events
        WHERE status = 'failed'
          AND datetime(updated_at) >= datetime('now', ?)
    `).get(stripeWindowExpr);
    const stripeFailureCount = Number(stripeFailureCountRow?.count || 0);
    const stripeLatestFailure = db.prepare(`
        SELECT event_id, event_type, attempts, last_error, updated_at
        FROM stripe_webhook_events
        WHERE status = 'failed'
        ORDER BY datetime(updated_at) DESC
        LIMIT 1
    `).get();
    const combinedStripeFailureCount = Math.max(stripeFailureCount, runtimeStripeFailureEvents.length);

    let backupStale = false;
    let backupStaleMinutes = null;
    if (AUTO_BACKUP_CONFIG.enabled && !autoBackupRunning) {
        if (!lastAutoBackupAt) {
            backupStale = true;
        } else {
            const lastRunMs = Date.parse(lastAutoBackupAt);
            if (Number.isFinite(lastRunMs)) {
                backupStaleMinutes = Math.floor((nowMs - lastRunMs) / 60000);
                backupStale = backupStaleMinutes >= SYSTEM_MONITOR_CONFIG.backupStaleMinutes;
            } else {
                backupStale = true;
            }
        }
    }

    let vmMismatchRows = [];
    const vmExpectedRunningRows = db.prepare(`
        SELECT c.id, c.name, c.user_id, c.docker_id, c.updated_at, u.email AS user_email
        FROM containers c
        LEFT JOIN users u ON u.id = c.user_id
        WHERE c.docker_id LIKE 'vm:%'
          AND c.status = 'running'
        ORDER BY datetime(c.updated_at) DESC
        LIMIT ?
    `).all(SYSTEM_MONITOR_CONFIG.vmStoppedCheckLimit);

    if (vmExpectedRunningRows.length > 0) {
        const graceMs = SYSTEM_MONITOR_CONFIG.vmStoppedGraceMinutes * 60 * 1000;
        for (const row of vmExpectedRunningRows) {
            const vmName = String(row.docker_id || '').replace(/^vm:/, '');
            if (!vmName) continue;

            const updatedAtMs = Date.parse(String(row.updated_at || ''));
            if (Number.isFinite(updatedAtMs) && (nowMs - updatedAtMs) < graceMs) {
                continue;
            }

            try {
                const vmInfo = await getVmBackendInfo(vmName);
                if (!vmInfo || vmInfo.status !== 'running') {
                    vmMismatchRows.push({
                        id: row.id,
                        name: row.name,
                        vm_name: vmName,
                        user_email: row.user_email || '-',
                        db_status: 'running',
                        vm_status: vmInfo?.status || 'unknown'
                    });
                }
            } catch {}
        }
    }

    lastSystemMonitorSummary = {
        updated_at: snapshot.timestamp,
        api_5xx_recent_count: api5xxEvents.length,
        backup_failure_recent_count: backupFailureEvents.length,
        stripe_failure_recent_count: combinedStripeFailureCount,
        vm_mismatch_count: vmMismatchRows.length,
        backup_stale: backupStale
    };

    if (snapshot.diskUsagePercent != null && snapshot.diskUsagePercent >= SYSTEM_MONITOR_CONFIG.diskUsageWarnPercent) {
        alerts.push({
            key: 'disk',
            title: 'ディスク使用率アラート',
            description: `ディスク使用率が ${snapshot.diskUsagePercent}% です`,
            fields: [
                { name: 'data_dir', value: CONFIG.DATA_DIR, inline: false },
                { name: '閾値', value: `${SYSTEM_MONITOR_CONFIG.diskUsageWarnPercent}%`, inline: true },
                { name: '現在値', value: `${snapshot.diskUsagePercent}%`, inline: true }
            ]
        });
    }

    if (snapshot.dbSizeMb >= SYSTEM_MONITOR_CONFIG.dbSizeWarnMb) {
        alerts.push({
            key: 'db-size',
            title: 'DBサイズアラート',
            description: `DBサイズが ${snapshot.dbSizeMb} MB です`,
            fields: [
                { name: 'DBファイル', value: CONFIG.DB_PATH, inline: false },
                { name: '閾値', value: `${SYSTEM_MONITOR_CONFIG.dbSizeWarnMb} MB`, inline: true },
                { name: '現在値', value: `${snapshot.dbSizeMb} MB`, inline: true }
            ]
        });
    }

    if (snapshot.rssMb >= SYSTEM_MONITOR_CONFIG.rssWarnMb) {
        alerts.push({
            key: 'rss',
            title: 'メモリ使用量アラート',
            description: `プロセスRSSが ${snapshot.rssMb} MB です`,
            fields: [
                { name: '閾値', value: `${SYSTEM_MONITOR_CONFIG.rssWarnMb} MB`, inline: true },
                { name: '現在値', value: `${snapshot.rssMb} MB`, inline: true }
            ]
        });
    }

    if (api5xxEvents.length >= SYSTEM_MONITOR_CONFIG.api5xxWarnCount) {
        const topPaths = summarizeRecentApi5xxEvents(api5xxEvents, 5);
        const lastEvent = api5xxEvents[api5xxEvents.length - 1] || null;
        alerts.push({
            key: 'api-5xx',
            title: 'API 5xx 多発アラート',
            description: `直近${SYSTEM_MONITOR_CONFIG.api5xxWindowSeconds}秒で API 5xx が ${api5xxEvents.length} 件発生しました`,
            fields: [
                { name: '閾値', value: `${SYSTEM_MONITOR_CONFIG.api5xxWarnCount} 件`, inline: true },
                { name: '現在値', value: `${api5xxEvents.length} 件`, inline: true },
                {
                    name: '主要エンドポイント',
                    value: topPaths.length ? topPaths.join('\n').slice(0, 1000) : '-',
                    inline: false
                },
                ...(lastEvent
                    ? [{
                        name: '最新5xx',
                        value: `${lastEvent.method || 'GET'} ${lastEvent.path || '-'} (${lastEvent.status || 500})`,
                        inline: false
                    }]
                    : [])
            ]
        });
    }

    if (backupStale) {
        alerts.push({
            key: 'backup-stale',
            title: 'バックアップ遅延アラート',
            description: '自動バックアップの最終実行時刻が古いか未実行です',
            fields: [
                { name: '閾値', value: `${SYSTEM_MONITOR_CONFIG.backupStaleMinutes} 分`, inline: true },
                {
                    name: '最終実行',
                    value: lastAutoBackupAt || '未実行',
                    inline: false
                },
                {
                    name: '経過',
                    value: backupStaleMinutes == null ? '-' : `${backupStaleMinutes} 分`,
                    inline: true
                }
            ]
        });
    }

    if (backupFailureEvents.length >= SYSTEM_MONITOR_CONFIG.backupFailureWarnCount) {
        const latest = backupFailureEvents[backupFailureEvents.length - 1] || {};
        alerts.push({
            key: 'backup-failed',
            title: 'バックアップ失敗アラート',
            description: `直近${SYSTEM_MONITOR_CONFIG.backupFailureWindowMinutes}分でバックアップ失敗が ${backupFailureEvents.length} 件発生しました`,
            fields: [
                { name: '閾値', value: `${SYSTEM_MONITOR_CONFIG.backupFailureWarnCount} 件`, inline: true },
                { name: '現在値', value: `${backupFailureEvents.length} 件`, inline: true },
                { name: '最新理由', value: String(latest.reason || '-'), inline: true },
                { name: '最新エラー', value: String(latest.message || '-').slice(0, 900), inline: false }
            ]
        });
    }

    if (combinedStripeFailureCount >= SYSTEM_MONITOR_CONFIG.stripeFailureWarnCount) {
        const runtimeLatest = runtimeStripeFailureEvents[runtimeStripeFailureEvents.length - 1] || null;
        const dbLatestAtMs = Date.parse(String(stripeLatestFailure?.updated_at || ''));
        const runtimeLatestAtMs = Number(runtimeLatest?.atMs || 0);
        const latestSource = runtimeLatestAtMs >= (Number.isFinite(dbLatestAtMs) ? dbLatestAtMs : 0)
            ? 'runtime'
            : 'db';
        const latestMessage = latestSource === 'runtime'
            ? String(runtimeLatest?.message || '-')
            : String(stripeLatestFailure?.last_error || '-');
        const latestEventType = latestSource === 'runtime'
            ? String(runtimeLatest?.event_type || runtimeLatest?.stage || '-')
            : String(stripeLatestFailure?.event_type || '-');

        alerts.push({
            key: 'stripe-webhook-failed',
            title: 'Stripe Webhook 失敗アラート',
            description: `直近${SYSTEM_MONITOR_CONFIG.stripeFailureWindowMinutes}分で Stripe webhook 失敗が ${combinedStripeFailureCount} 件検出されました`,
            fields: [
                { name: '閾値', value: `${SYSTEM_MONITOR_CONFIG.stripeFailureWarnCount} 件`, inline: true },
                { name: '現在値', value: `${combinedStripeFailureCount} 件`, inline: true },
                { name: '最新イベント', value: latestEventType || '-', inline: true },
                { name: '最新エラー', value: latestMessage.slice(0, 900), inline: false }
            ]
        });
    }

    if (vmMismatchRows.length > 0) {
        const lines = vmMismatchRows.slice(0, 10).map((item) => (
            `${item.name} (${item.vm_name}) / ${item.vm_status} / ${item.user_email}`
        ));
        alerts.push({
            key: 'vm-stopped',
            title: 'VM 停止アラート',
            description: 'DB上は running ですが、実体VMが停止/不明の可能性があります',
            fields: [
                { name: '検出件数', value: `${vmMismatchRows.length} 件`, inline: true },
                { name: '確認上限', value: `${SYSTEM_MONITOR_CONFIG.vmStoppedCheckLimit} 件`, inline: true },
                { name: 'グレース期間', value: `${SYSTEM_MONITOR_CONFIG.vmStoppedGraceMinutes} 分`, inline: true },
                { name: '対象', value: lines.join('\n').slice(0, 1000), inline: false }
            ]
        });
    }

    for (const alert of alerts) {
        if (!shouldEmitMonitorAlert(alert.key)) continue;
        sendOpsDiscordNotification({
            title: alert.title,
            description: alert.description,
            color: 15158332,
            fields: alert.fields
        }).catch((notifyError) => {
            console.error('System monitor alert notify failed:', notifyError);
        });
    }

    return snapshot;
};

const startAutoBackupScheduler = () => {
    if (!AUTO_BACKUP_CONFIG.enabled) {
        console.log('ℹ️ Auto backup scheduler disabled');
        return;
    }

    if (autoBackupTimer) {
        clearInterval(autoBackupTimer);
        autoBackupTimer = null;
    }

    const intervalMs = Math.max(60 * 1000, AUTO_BACKUP_CONFIG.intervalMinutes * 60 * 1000);
    autoBackupTimer = setInterval(() => {
        createSystemBackupSnapshot({ reason: 'scheduled' })
            .then((result) => {
                if (result?.ok || result?.skipped) return;
                console.error('Scheduled backup tick failed:', result?.error || result?.reason || 'unknown_error');
            })
            .catch((error) => {
                console.error('Scheduled backup tick failed:', error);
            });
    }, intervalMs);

    setTimeout(() => {
        createSystemBackupSnapshot({
            reason: 'startup',
            force: true,
            modeOverride: AUTO_BACKUP_CONFIG.startupMode
        })
            .then((result) => {
                if (result?.ok || result?.skipped) return;
                console.error('Startup backup failed:', result?.error || result?.reason || 'unknown_error');
            })
            .catch((error) => {
                console.error('Startup backup failed:', error);
            });
    }, 3000);

    console.log(
        `✅ Auto backup scheduler started (interval=${AUTO_BACKUP_CONFIG.intervalMinutes}m, mode=${AUTO_BACKUP_CONFIG.mode}, startup_mode=${AUTO_BACKUP_CONFIG.startupMode})`
    );
};

const startSystemMonitorScheduler = () => {
    if (!SYSTEM_MONITOR_CONFIG.enabled) {
        console.log('ℹ️ System monitor scheduler disabled');
        return;
    }

    if (systemMonitorTimer) {
        clearInterval(systemMonitorTimer);
        systemMonitorTimer = null;
    }

    const intervalMs = Math.max(30 * 1000, SYSTEM_MONITOR_CONFIG.intervalSeconds * 1000);
    systemMonitorTimer = setInterval(() => {
        runSystemMonitorTick().catch((error) => {
            console.error('System monitor tick failed:', error);
        });
    }, intervalMs);

    runSystemMonitorTick().catch((error) => {
        console.error('Initial system monitor tick failed:', error);
    });

    console.log(`✅ System monitor scheduler started (interval=${SYSTEM_MONITOR_CONFIG.intervalSeconds}s)`);
};

// ============ Cloudflare Turnstile ============

// Turnstile検証関数
const verifyTurnstile = async (token, ip) => {
    // 認証を無効化している場合はスキップ
    if (!TURNSTILE_AUTH_ENABLED || !CONFIG.TURNSTILE_SECRET_KEY) {
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
    if (!TURNSTILE_AUTH_ENABLED) {
        return res.json({
            enabled: false,
            siteKey: null
        });
    }
    res.json({
        enabled: !!CONFIG.TURNSTILE_SITE_KEY,
        siteKey: CONFIG.TURNSTILE_SITE_KEY || null
    });
});

// Turnstile検証API（認証開始前に呼び出される）
app.post('/api/turnstile/verify', async (req, res) => {
    const { token } = req.body;
    
    if (!TURNSTILE_AUTH_ENABLED || !CONFIG.TURNSTILE_SECRET_KEY) {
        // Turnstileが無効な場合は成功として処理
        return res.json({ success: true, skipped: true });
    }
    
    if (!token) {
        return res.status(400).json({ success: false, error: 'トークンが必要です' });
    }
    
    const result = await verifyTurnstile(token, getClientIp(req));
    
    if (result.success) {
        // 検証成功時、一時的なセッションIDを発行
        const sessionId = uuidv4();
        // セッションを5分間有効に（メモリに保存）
        turnstileSessions.set(sessionId, {
            verified: true,
            ip: getClientIp(req),
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

const normalizeEmail = (value = '') => String(value || '').trim().toLowerCase();
const isValidEmailFormat = (value = '') => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || '').trim());
const MAILER_RECIPIENT_MODES = new Set(['all_users', 'manual']);

const escapeHtmlForMail = (value = '') => String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');

const formatMailTextToHtml = (value = '') => {
    const escaped = escapeHtmlForMail(String(value || '').trim());
    if (!escaped) return '';
    return escaped
        .split('\n')
        .map((line) => line.trim() ? line : '&nbsp;')
        .join('<br>\n');
};

const isMailerConfigured = () => (
    !!CONFIG.MAILER_ENABLED &&
    !!String(CONFIG.MAILER_SMTP_HOST || '').trim() &&
    !!String(CONFIG.MAILER_FROM_EMAIL || '').trim()
);

let mailTransporter = null;
const getMailTransporter = () => {
    if (!isMailerConfigured()) {
        return null;
    }
    if (mailTransporter) return mailTransporter;

    const smtpUser = String(CONFIG.MAILER_SMTP_USER || '').trim();
    const smtpPass = String(CONFIG.MAILER_SMTP_PASS || '').trim();
    const auth = smtpUser && smtpPass ? { user: smtpUser, pass: smtpPass } : undefined;
    mailTransporter = nodemailer.createTransport({
        host: CONFIG.MAILER_SMTP_HOST,
        port: CONFIG.MAILER_SMTP_PORT,
        secure: !!CONFIG.MAILER_SMTP_SECURE,
        auth,
        tls: {
            rejectUnauthorized: !!CONFIG.MAILER_SMTP_REJECT_UNAUTHORIZED
        }
    });
    return mailTransporter;
};

const parseManualRecipientLine = (line = '') => {
    const raw = String(line || '').trim();
    if (!raw) return null;

    // Name <email@example.com>
    const angleMatch = raw.match(/^(.*?)<([^<>]+)>$/);
    if (angleMatch) {
        const maybeEmail = normalizeEmail(angleMatch[2]);
        if (!isValidEmailFormat(maybeEmail)) return null;
        const name = String(angleMatch[1] || '').replace(/^["'\s]+|["'\s]+$/g, '').trim();
        return { email: maybeEmail, name: name || null };
    }

    // email,name
    if (raw.includes(',')) {
        const [emailPart, ...nameParts] = raw.split(',');
        const maybeEmail = normalizeEmail(emailPart);
        if (!isValidEmailFormat(maybeEmail)) return null;
        const name = nameParts.join(',').trim();
        return { email: maybeEmail, name: name || null };
    }

    const maybeEmail = normalizeEmail(raw);
    if (!isValidEmailFormat(maybeEmail)) return null;
    return { email: maybeEmail, name: null };
};

const parseManualRecipients = (rawText = '') => {
    const lines = String(rawText || '')
        .split(/\r?\n|;/g)
        .map((line) => line.trim())
        .filter(Boolean);
    const recipients = [];
    const seen = new Set();
    for (const line of lines) {
        const parsed = parseManualRecipientLine(line);
        if (!parsed) continue;
        const key = parsed.email;
        if (seen.has(key)) continue;
        seen.add(key);
        recipients.push(parsed);
    }
    return recipients;
};

const normalizeMailerPlanFilters = (planFiltersRaw) => {
    const allowed = new Set(Object.keys(CONFIG.PLANS || {}));
    const input = Array.isArray(planFiltersRaw) ? planFiltersRaw : [];
    const normalized = input
        .map((value) => String(value || '').trim().toLowerCase())
        .filter((value) => allowed.has(value));
    return Array.from(new Set(normalized));
};

const collectRecipientsFromUsers = (planFilters = []) => {
    const plans = normalizeMailerPlanFilters(planFilters);
    const rows = plans.length
        ? db.prepare(`
            SELECT id, email, name, plan
            FROM users
            WHERE lower(email) <> '' AND plan IN (${plans.map(() => '?').join(',')})
            ORDER BY datetime(created_at) DESC
        `).all(...plans)
        : db.prepare(`
            SELECT id, email, name, plan
            FROM users
            WHERE lower(email) <> ''
            ORDER BY datetime(created_at) DESC
        `).all();

    const recipients = [];
    const seen = new Set();
    for (const row of rows) {
        const email = normalizeEmail(row.email);
        if (!isValidEmailFormat(email) || seen.has(email)) continue;
        seen.add(email);
        recipients.push({
            email,
            name: String(row.name || '').trim() || null,
            user_id: row.id,
            plan: row.plan
        });
    }
    return recipients;
};

const waitMs = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const resolveMailerFromField = () => {
    const fromName = String(CONFIG.MAILER_FROM_NAME || '').trim();
    const fromEmail = normalizeEmail(CONFIG.MAILER_FROM_EMAIL);
    if (!isValidEmailFormat(fromEmail)) return '';
    return fromName ? `"${fromName.replace(/"/g, '\\"')}" <${fromEmail}>` : fromEmail;
};

const sanitizeMailBodyText = (value = '') => String(value || '')
    .replace(/\r\n/g, '\n')
    .replace(/\u0000/g, '')
    .trim();

const resolveNotificationRecipients = () => {
    const fallback = ['koutarou114@outlook.jp'];
    const raw = String(CONFIG.NOTIFY_EMAIL_TO || '').trim();
    const source = raw || fallback.join(',');
    const candidates = source
        .split(/[,\n;]+/g)
        .map((item) => String(item || '').trim().toLowerCase())
        .filter(Boolean);
    const recipients = [];
    const seen = new Set();
    const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    for (const email of candidates) {
        if (!emailPattern.test(email) || seen.has(email)) continue;
        seen.add(email);
        recipients.push(email);
    }
    return recipients.length ? recipients : fallback;
};

const sendInternalNotificationEmail = async ({
    subject,
    lines = []
}) => {
    const transporter = getMailTransporter();
    if (!transporter) {
        console.warn('Notification mail skipped: mailer is not configured');
        return false;
    }
    const from = resolveMailerFromField();
    if (!from) {
        console.warn('Notification mail skipped: MAILER_FROM_EMAIL is invalid');
        return false;
    }
    const recipients = resolveNotificationRecipients();
    if (!recipients.length) {
        console.warn('Notification mail skipped: recipient list is empty');
        return false;
    }

    const text = sanitizeMailBodyText(Array.isArray(lines) ? lines.filter(Boolean).join('\n') : String(lines || ''));
    const html = formatMailTextToHtml(text);
    const replyTo = normalizeEmail(CONFIG.MAILER_REPLY_TO || '');
    const message = {
        from,
        to: recipients.join(', '),
        subject: String(subject || `${CONFIG.NOTIFY_SUBJECT_PREFIX} Notification`).slice(0, 200),
        text: text || '-',
        html: html || '-'
    };
    if (replyTo && isValidEmailFormat(replyTo)) {
        message.replyTo = replyTo;
    }

    const result = await transporter.sendMail(message);
    return !!result?.messageId;
};

const consumeTurnstileSession = (sessionId) => {
    if (!TURNSTILE_AUTH_ENABLED || !CONFIG.TURNSTILE_SECRET_KEY) {
        return { ok: true, skipped: true };
    }

    if (!sessionId) {
        return { ok: false, error: 'turnstile_required' };
    }

    const session = turnstileSessions.get(sessionId);
    if (!session || session.expires < Date.now()) {
        turnstileSessions.delete(sessionId);
        return { ok: false, error: 'turnstile_expired' };
    }

    turnstileSessions.delete(sessionId);
    return { ok: true };
};

const makeAuthUserPayload = (user) => ({
    id: user.id,
    discord_id: user.discord_id || null,
    email: user.email,
    name: user.name,
    avatar: user.avatar || null,
    plan: user.plan,
    is_admin: user.is_admin,
    max_containers: user.max_containers || 1,
    max_cpu: user.max_cpu || 0.5,
    max_memory: user.max_memory || 512,
    max_storage: user.max_storage || 1024,
    stripe_customer_id: user.stripe_customer_id || null,
    stripe_subscription_id: user.stripe_subscription_id || null,
    two_factor_enabled: !!user.two_factor_enabled,
    passkey_enabled: !!user.passkey_enabled,
    created_at: user.created_at
});

// メール登録
app.post('/api/auth/register', authRateLimit, async (req, res) => {
    try {
        const { email, password, name, sessionId, termsAccepted } = req.body || {};
        const normalizedEmail = normalizeEmail(email);
        const displayName = String(name || '').trim() || normalizedEmail.split('@')[0] || 'user';
        const passwordText = String(password || '');

        if (!normalizedEmail || !isValidEmailFormat(normalizedEmail)) {
            return res.status(400).json({ error: '有効なメールアドレスを入力してください' });
        }
        if (passwordText.length < 8) {
            return res.status(400).json({ error: 'パスワードは8文字以上で入力してください' });
        }
        if (displayName.length > 80) {
            return res.status(400).json({ error: 'ユーザー名は80文字以内で入力してください' });
        }
        if (termsAccepted !== true) {
            return res.status(400).json({ error: '利用規約・プライバシーポリシーへの同意が必要です' });
        }

        const turnstileCheck = consumeTurnstileSession(sessionId);
        if (!turnstileCheck.ok) {
            const message = turnstileCheck.error === 'turnstile_expired'
                ? 'セキュリティ認証の有効期限が切れました。再度認証してください'
                : 'セキュリティ認証を完了してください';
            return res.status(400).json({ error: message, code: turnstileCheck.error });
        }

        const existing = db.prepare('SELECT id FROM users WHERE email = ?').get(normalizedEmail);
        if (existing) {
            return res.status(409).json({ error: 'このメールアドレスは既に登録されています' });
        }

        const passwordHash = await bcrypt.hash(passwordText, 10);
        const userId = `user-${uuidv4()}`;

        db.prepare(`
            INSERT INTO users (id, email, password, name, plan, max_containers, max_cpu, max_memory, max_storage)
            VALUES (?, ?, ?, ?, 'free', 1, 0.5, 512, 1024)
        `).run(userId, normalizedEmail, passwordHash, displayName);

        const user = db.prepare('SELECT * FROM users WHERE id = ?').get(userId);
        const token = jwt.sign({ userId: user.id, discordId: user.discord_id || null }, CONFIG.JWT_SECRET, { expiresIn: '90d' });

        logActivity(user.id, null, 'register_email', { email: normalizedEmail }, getClientIp(req));
        logAccessHistory({ userId: user.id, event: 'register_email', req });
        res.json({ success: true, token, user: makeAuthUserPayload(user) });
    } catch (e) {
        console.error('Email register error:', e);
        res.status(500).json({ error: 'メール登録に失敗しました' });
    }
});

// メールログイン
app.post('/api/auth/login', authRateLimit, async (req, res) => {
    try {
        const { email, password, sessionId } = req.body || {};
        const normalizedEmail = normalizeEmail(email);
        const passwordText = String(password || '');

        if (!normalizedEmail || !passwordText) {
            return res.status(400).json({ error: 'メールアドレスとパスワードを入力してください' });
        }

        const turnstileCheck = consumeTurnstileSession(sessionId);
        if (!turnstileCheck.ok) {
            const message = turnstileCheck.error === 'turnstile_expired'
                ? 'セキュリティ認証の有効期限が切れました。再度認証してください'
                : 'セキュリティ認証を完了してください';
            return res.status(400).json({ error: message, code: turnstileCheck.error });
        }

        const user = db.prepare('SELECT * FROM users WHERE email = ?').get(normalizedEmail);
        if (!user) {
            return res.status(401).json({ error: 'メールアドレスまたはパスワードが正しくありません' });
        }

        const passwordHash = String(user.password || '');
        if (passwordHash.length < 20) {
            return res.status(400).json({ error: 'このアカウントはメールログイン未設定です。Discordログインをご利用ください。' });
        }

        const passwordMatched = await bcrypt.compare(passwordText, passwordHash);
        if (!passwordMatched) {
            return res.status(401).json({ error: 'メールアドレスまたはパスワードが正しくありません' });
        }

        if (user.two_factor_enabled || user.passkey_enabled) {
            const pendingToken = jwt.sign(
                { userId: user.id, pending2fa: true },
                CONFIG.JWT_SECRET,
                { expiresIn: '5m' }
            );
            return res.json({
                success: false,
                require2fa: true,
                authType: user.passkey_enabled ? 'passkey' : 'totp',
                pendingToken
            });
        }

        const token = jwt.sign({ userId: user.id, discordId: user.discord_id || null }, CONFIG.JWT_SECRET, { expiresIn: '90d' });
        logActivity(user.id, null, 'login_email', { email: normalizedEmail }, getClientIp(req));
        logAccessHistory({ userId: user.id, event: 'login_email', req });
        res.json({ success: true, token, user: makeAuthUserPayload(user) });
    } catch (e) {
        console.error('Email login error:', e);
        res.status(500).json({ error: 'メールログインに失敗しました' });
    }
});

// ============ Discord OAuth認証API ============

// Discord認証開始
app.get('/api/auth/discord', (req, res) => {
    if (!CONFIG.DISCORD_CLIENT_ID) {
        return res.status(500).json({ error: 'Discord認証が設定されていません' });
    }
    
    // Turnstileセッション検証（設定されている場合）
    const sessionId = req.query.session;
    if (TURNSTILE_AUTH_ENABLED && CONFIG.TURNSTILE_SECRET_KEY) {
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
    const requestHost = getRequestHost(req);
    const requestedRedirect = sanitizeOAuthRedirectUrl(req.query.redirect || '', requestHost);
    const redirectUrl = requestedRedirect || getDefaultOAuthRedirectUrl(requestHost);
    const stateData = encodeOAuthState({
        redirect: redirectUrl,
        nonce: crypto.randomUUID()
    });
    
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
            const stateData = decodeOAuthState(state);
            if (stateData?.redirect) {
                redirectUrl = sanitizeOAuthRedirectUrl(stateData.redirect, CONFIG.PUBLIC_HOST);
            }
        }
        if (!redirectUrl) {
            redirectUrl = getDefaultOAuthRedirectUrl(CONFIG.PUBLIC_HOST);
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
            
            logActivity(userId, null, 'register', { discord_id: discordUser.id }, getClientIp(req));
            logAccessHistory({ userId, event: 'register_discord', req });
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
        
        logActivity(user.id, null, 'login', { discord_id: discordUser.id }, getClientIp(req));
        logAccessHistory({ userId: user.id, event: 'login_discord', req });
        
        // リダイレクト先を決定
        if (redirectUrl && /^https?:\/\//i.test(redirectUrl)) {
            // 外部リダイレクト（manage.lesve.tech など）
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
    logActivity(req.user.id, null, 'logout', {}, getClientIp(req));
    logAccessHistory({ userId: req.user.id, event: 'logout', req });
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
        
        logActivity(req.user.id, null, 'passkey_register', { name }, getClientIp(req));
        
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
        
        logActivity(user.id, null, 'login_2fa', { method: 'passkey' }, getClientIp(req));
        logAccessHistory({ userId: user.id, event: 'login_2fa_passkey', req });
        
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
        
        logActivity(req.user.id, null, 'passkey_delete', { name: passkey.name }, getClientIp(req));
        
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
        
        logActivity(req.user.id, null, 'totp_enable', {}, getClientIp(req));
        
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
        
        logActivity(user.id, null, 'login_2fa', { method: usedBackupCode ? 'backup_code' : 'totp' }, getClientIp(req));
        logAccessHistory({ userId: user.id, event: usedBackupCode ? 'login_2fa_backup_code' : 'login_2fa_totp', req });
        
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
        
        logActivity(req.user.id, null, 'totp_disable', {}, getClientIp(req));
        
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
        
        logActivity(req.user.id, null, 'ssh_key_add', { name, fingerprint }, getClientIp(req));
        
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
        
        logActivity(req.user.id, null, 'ssh_key_delete', { name: key.name, fingerprint: key.fingerprint }, getClientIp(req));
        
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
            const vmTtydLink = isVmBackend
                ? ensureVmTtydStableLinkSeed({
                    containerId: c.id,
                    vmName,
                    username: c.ssh_user,
                    password: c.ssh_password,
                    ttydPort: CONFIG.VM_TTYD_PORT
                })
                : null;
            
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
                vm_ttyd_url: vmTtydLink ? buildVmTtydStableUrl(vmTtydLink.link_token) : null,
                vm_ttyd_username: vmTtydLink?.username || null,
                vm_ttyd_password: vmTtydLink?.password || null,
                vm_ttyd_port: vmTtydLink?.ttyd_port || null,
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
        const { name, type, env_vars, auto_restart, startup_command, backend } = req.body;
        
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
        const backendType = resolveContainerCreateBackend(backend);
        
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

        let dockerId = null;
        let createdStatus = 'stopped';
        if (backendType === 'vm') {
            const vmProvision = await provisionVmForContainer({
                containerId,
                sshUser,
                sshPassword,
                planConfig,
                autoRestart: !!auto_restart
            });
            if (!vmProvision.ok) {
                return res.status(500).json({
                    error: `VMの作成に失敗しました: ${vmProvision.error}`
                });
            }

            dockerId = vmProvision.dockerId;
            createdStatus = vmProvision.status || 'running';
            console.log(`✅ VM ${vmProvision.vmName} created for ${containerId} (SSH user: ${sshUser})`);
        } else if (dockerAvailable) {
            try {
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
                createdStatus = 'running';
                await container.start();

                const allPackages = [...CONFIG.BASE_PACKAGES, ...serverType.packages];
                const setupCmd = buildAptSetupScript(allPackages);
                const exec = await container.exec({
                    Cmd: ['bash', '-lc', setupCmd],
                    AttachStdout: true,
                    AttachStderr: true
                });
                exec.start({ Detach: true }).catch(() => console.log('Package setup started in background'));

                console.log(`✅ Container ${containerId} created (SSH user: ${sshUser})`);
            } catch (e) {
                console.error('Docker container creation error:', e);
            }
        } else {
            return res.status(500).json({ error: 'Dockerが利用できないため、サーバーを作成できません' });
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
            dockerId ? createdStatus : 'stopped',
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
        
        logActivity(req.user.id, containerId, 'container_create', { name, type, backend: backendType }, getClientIp(req));
        
        const containerData = db.prepare('SELECT * FROM containers WHERE id = ?').get(containerId);
        res.json({
            ...containerData,
            backend_type: isVmBackendRef(containerData.docker_id) ? 'vm' : 'docker',
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
        const vmTtydLink = isVmBackend
            ? ensureVmTtydStableLinkSeed({
                containerId: container.id,
                vmName,
                username: container.ssh_user,
                password: container.ssh_password,
                ttydPort: CONFIG.VM_TTYD_PORT
            })
            : null;
        
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
            vm_ttyd_url: vmTtydLink ? buildVmTtydStableUrl(vmTtydLink.link_token) : null,
            vm_ttyd_username: vmTtydLink?.username || null,
            vm_ttyd_password: vmTtydLink?.password || null,
            vm_ttyd_port: vmTtydLink?.ttyd_port || null,
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

            logActivity(req.user.id, req.params.id, 'container_start', { backend: 'vm', vm: vmName }, getClientIp(req));
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
                        const startupScript = buildDetachedContainerStartupScript(container.startup_command);
                        const exec = await dockerContainer.exec({
                            Cmd: ['bash', '-lc', startupScript],
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
                    const setupCmd = buildAptSetupScript(allPackages);
                    const exec2 = await newContainer.exec({
                        Cmd: ['bash', '-lc', setupCmd],
                        AttachStdout: true,
                        AttachStderr: true
                    });
                    exec2.start({ Detach: true }).catch(() => {});
                    
                    // 起動コマンドがあれば実行
                    if (container.startup_command) {
                        const startupScript = buildDetachedContainerStartupScript(container.startup_command);
                        const exec = await newContainer.exec({
                            Cmd: ['bash', '-lc', startupScript],
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
        
        logActivity(req.user.id, req.params.id, 'container_start', {}, getClientIp(req));
        
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

            logActivity(req.user.id, req.params.id, 'container_stop', { backend: 'vm', vm: vmName }, getClientIp(req));
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
        
        logActivity(req.user.id, req.params.id, 'container_stop', {}, getClientIp(req));
        
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

            logActivity(req.user.id, req.params.id, 'container_restart', { backend: 'vm', vm: vmName }, getClientIp(req));
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
                            const startupScript = buildDetachedContainerStartupScript(container.startup_command);
                            const exec = await dockerContainer.exec({
                                Cmd: ['bash', '-lc', startupScript],
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
        
        logActivity(req.user.id, req.params.id, 'container_restart', {}, getClientIp(req));
        
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
        
        logActivity(req.user.id, req.params.id, 'container_delete', {}, getClientIp(req));
        
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
        const normalizedCommand = normalizeShellScript(command);
        
        if (!normalizedCommand) {
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

            const cmd = `cd /app 2>/dev/null || cd / && /bin/bash -lc ${shellSingleQuote(normalizedCommand)}`;
            const vmExecResult = await vmExecBash(vmName, cmd, { timeoutMs: 30000 });
            if (!vmExecResult.ok && vmExecResult.stderr) {
                return res.json({ output: vmExecResult.stdout + vmExecResult.stderr });
            }
            return res.json({ output: vmExecResult.stdout + vmExecResult.stderr });
        }

        if (!dockerAvailable || !isDockerBackendRef(container.docker_id)) {
            return res.json({
                output: `[シミュレーション] $ ${normalizedCommand}\nDocker環境では実際に実行されます。`
            });
        }

        const execScript = buildContainerExecScript(normalizedCommand);
        const dockerContainer = docker.getContainer(container.docker_id);
        const exec = await dockerContainer.exec({
            Cmd: ['bash', '-lc', execScript],
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

// VM ttyd 固定リンク更新（固定URLのみ）
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

        const endpoint = await ensureVmTtydDirectEndpoint(vmName, {
            port: tunnelPort,
            authUser: tunnelUser,
            authPass: tunnelPassword
        });

        if (!endpoint.ok) {
            return res.status(500).json({ error: `固定リンク作成に失敗しました: ${endpoint.error}` });
        }

        const directUrl = endpoint.url;
        const linkToken = upsertVmTtydStableLink({
            containerId: req.params.id,
            vmName,
            targetUrl: directUrl,
            username: endpoint.username,
            password: endpoint.password,
            ttydPort: endpoint.port
        });
        const publicUrl = buildVmTtydStableUrl(linkToken);

        logActivity(req.user.id, req.params.id, 'vm_ttyd_tunnel_create', {
            backend: 'vm',
            vm: vmName,
            port: endpoint.port,
            mode: 'subdomain',
            transport: 'vm_direct',
            url: publicUrl,
            direct_url: directUrl
        }, getClientIp(req));

        res.json({
            success: true,
            message: '固定URLを更新しました',
            mode: 'subdomain',
            backend: 'vm',
            vm_name: vmName,
            url: publicUrl,
            direct_url: directUrl,
            ttyd_port: endpoint.port,
            username: endpoint.username,
            password: endpoint.password
        });
    } catch (e) {
        console.error('Create VM ttyd tunnel error:', e);
        res.status(500).json({ error: 'VM固定URLの更新に失敗しました' });
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
        
        logActivity(req.user.id, req.params.id, 'env_update', {}, getClientIp(req));
        
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
                const listScript = `ls -la --time-style=+%Y-%m-%dT%H:%M:%S -- ${shellSingleQuote(targetPath)} 2>/dev/null || echo "DIR_NOT_FOUND"`;
                const exec = await dockerContainer.exec({
                    Cmd: ['bash', '-lc', listScript],
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

            logActivity(req.user.id, req.params.id, 'file_save', { path: filePath, backend: 'vm' }, getClientIp(req));
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
                const writeScript = `base64 -d > ${shellSingleQuote(filePath)} <<'__SHIRONEKO_B64__'
${base64Content}
__SHIRONEKO_B64__`;
                const exec = await dockerContainer.exec({
                    Cmd: ['bash', '-lc', writeScript],
                    AttachStdout: true,
                    AttachStderr: true
                });
                
                await exec.start();
                
                logActivity(req.user.id, req.params.id, 'file_save', { path: filePath }, getClientIp(req));
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
        logActivity(req.user.id, req.params.id, 'file_save', { path: filePath }, getClientIp(req));
        
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
            }, getClientIp(req));
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
                    const writeScript = `base64 -d > ${shellSingleQuote(destPath)} <<'__SHIRONEKO_B64__'
${base64Content}
__SHIRONEKO_B64__`;
                    
                    // コンテナ内でBase64デコードしてファイルを作成
                    const exec = await dockerContainer.exec({
                        Cmd: ['bash', '-lc', writeScript],
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
        }, getClientIp(req));
        
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

            logActivity(req.user.id, req.params.id, 'file_delete', { path: targetPath, backend: 'vm' }, getClientIp(req));
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
                
                logActivity(req.user.id, req.params.id, 'file_delete', { path: targetPath }, getClientIp(req));
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
        
        logActivity(req.user.id, req.params.id, 'file_delete', { path: targetPath }, getClientIp(req));
        
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
        
        logActivity(req.user.id, req.params.id, 'backup_create', { backupName }, getClientIp(req));
        
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
const parseStripePaymentMethodTypes = (raw) => {
    const text = String(raw || '').trim();
    if (!text) return [];

    const unique = new Set();
    for (const token of text.split(',')) {
        const method = token.trim();
        if (method) unique.add(method);
    }
    return Array.from(unique);
};
const STRIPE_PAYMENT_METHOD_TYPES = parseStripePaymentMethodTypes(CONFIG.STRIPE_PAYMENT_METHOD_TYPES);

const getStripePriceIdForPlan = (planId) => {
    if (planId === 'standard') return CONFIG.STRIPE_PRICE_STANDARD;
    if (planId === 'pro') return CONFIG.STRIPE_PRICE_PRO;
    return '';
};

const normalizeKnownPlanId = (value) => {
    const planId = String(value || '').trim().toLowerCase();
    return CONFIG.PLANS[planId] ? planId : null;
};

const getStripeObjectId = (value) => {
    if (!value) return '';
    if (typeof value === 'string') return value.trim();
    if (typeof value?.id === 'string') return value.id.trim();
    return '';
};

const getPlanIdFromStripePriceId = (priceId) => {
    const target = String(priceId || '').trim();
    if (!target) return null;
    if (target === CONFIG.STRIPE_PRICE_STANDARD) return 'standard';
    if (target === CONFIG.STRIPE_PRICE_PRO) return 'pro';
    return null;
};

const resolveStripeSubscriptionPlan = (subscription) => {
    const planFromMeta = normalizeKnownPlanId(subscription?.metadata?.target_plan || subscription?.metadata?.plan);
    if (planFromMeta) return planFromMeta;

    const items = Array.isArray(subscription?.items?.data) ? subscription.items.data : [];
    for (const item of items) {
        const planFromPrice = getPlanIdFromStripePriceId(item?.price?.id);
        if (planFromPrice) return planFromPrice;
    }
    return null;
};

const findUserForStripeWebhook = ({ userId = '', subscriptionId = '', customerId = '' } = {}) => {
    const normalizedUserId = String(userId || '').trim();
    if (normalizedUserId) {
        const user = db.prepare('SELECT * FROM users WHERE id = ?').get(normalizedUserId);
        if (user) return user;
    }

    const normalizedSubscriptionId = String(subscriptionId || '').trim();
    if (normalizedSubscriptionId) {
        const user = db.prepare('SELECT * FROM users WHERE stripe_subscription_id = ?').get(normalizedSubscriptionId);
        if (user) return user;
    }

    const normalizedCustomerId = String(customerId || '').trim();
    if (normalizedCustomerId) {
        const user = db.prepare('SELECT * FROM users WHERE stripe_customer_id = ?').get(normalizedCustomerId);
        if (user) return user;
    }

    return null;
};

const stripeWebhookGetEventStmt = db.prepare(`
    SELECT event_id, event_type, status, attempts
    FROM stripe_webhook_events
    WHERE event_id = ?
    LIMIT 1
`);
const stripeWebhookInsertEventStmt = db.prepare(`
    INSERT INTO stripe_webhook_events (event_id, event_type, status, attempts, payload_hash, processed_at, updated_at)
    VALUES (?, ?, 'processing', 1, ?, NULL, CURRENT_TIMESTAMP)
`);
const stripeWebhookMarkProcessingStmt = db.prepare(`
    UPDATE stripe_webhook_events
    SET event_type = ?,
        status = 'processing',
        attempts = attempts + 1,
        payload_hash = COALESCE(?, payload_hash),
        last_error = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE event_id = ?
`);
const stripeWebhookMarkProcessedStmt = db.prepare(`
    UPDATE stripe_webhook_events
    SET status = 'processed',
        last_error = NULL,
        processed_at = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
    WHERE event_id = ?
`);
const stripeWebhookMarkFailedStmt = db.prepare(`
    UPDATE stripe_webhook_events
    SET status = 'failed',
        last_error = ?,
        updated_at = CURRENT_TIMESTAMP
    WHERE event_id = ?
`);

const truncateErrorText = (value, maxLength = 1000) => {
    const text = String(value || '').trim();
    if (text.length <= maxLength) return text;
    return text.slice(0, maxLength);
};

const handleStripeCheckoutSessionCompleted = async (sessionObject) => {
    const sessionId = String(sessionObject?.id || '').trim();
    const ownerUserId = String(sessionObject?.client_reference_id || sessionObject?.metadata?.user_id || '').trim();
    const targetPlan = normalizeKnownPlanId(sessionObject?.metadata?.target_plan);
    const stripeCustomerId = getStripeObjectId(sessionObject?.customer);
    const stripeSubscriptionId = getStripeObjectId(sessionObject?.subscription);
    const user = findUserForStripeWebhook({
        userId: ownerUserId,
        subscriptionId: stripeSubscriptionId,
        customerId: stripeCustomerId
    });

    if (!user) {
        return {
            handled: false,
            reason: 'user_not_found',
            session_id: sessionId
        };
    }

    db.prepare(`
        UPDATE users
        SET stripe_customer_id = COALESCE(NULLIF(?, ''), stripe_customer_id),
            stripe_subscription_id = COALESCE(NULLIF(?, ''), stripe_subscription_id),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    `).run(stripeCustomerId || null, stripeSubscriptionId || null, user.id);

    let appliedPlan = user.plan || 'free';
    if (targetPlan && targetPlan !== user.plan) {
        const limits = await applyUserPlanAndLimits({
            userId: user.id,
            planId: targetPlan,
            actorUserId: user.id,
            ip: 'stripe-webhook',
            activity: 'billing_webhook_checkout_completed'
        });
        appliedPlan = limits.planId;
    }

    logActivity(user.id, null, 'billing_webhook_checkout', {
        session_id: sessionId,
        target_plan: targetPlan,
        applied_plan: appliedPlan
    }, 'stripe-webhook');

    return {
        handled: true,
        user_id: user.id,
        plan: appliedPlan,
        session_id: sessionId
    };
};

const handleStripeSubscriptionLifecycleEvent = async (eventType, subscriptionObject) => {
    const subscriptionId = getStripeObjectId(subscriptionObject?.id || subscriptionObject);
    const customerId = getStripeObjectId(subscriptionObject?.customer);
    const subscriptionStatus = String(subscriptionObject?.status || '').trim().toLowerCase();
    const user = findUserForStripeWebhook({
        subscriptionId,
        customerId
    });

    if (!user) {
        return {
            handled: false,
            reason: 'user_not_found',
            subscription_id: subscriptionId
        };
    }

    if (eventType === 'customer.subscription.deleted') {
        db.prepare(`
            UPDATE users
            SET stripe_customer_id = COALESCE(NULLIF(?, ''), stripe_customer_id),
                stripe_subscription_id = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        `).run(customerId || null, user.id);
    } else {
        db.prepare(`
            UPDATE users
            SET stripe_customer_id = COALESCE(NULLIF(?, ''), stripe_customer_id),
                stripe_subscription_id = COALESCE(NULLIF(?, ''), stripe_subscription_id),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        `).run(customerId || null, subscriptionId || null, user.id);
    }

    let nextPlan = null;
    if (eventType === 'customer.subscription.deleted') {
        nextPlan = 'free';
    } else {
        if (['canceled', 'unpaid', 'incomplete_expired'].includes(subscriptionStatus)) {
            nextPlan = 'free';
        } else {
            nextPlan = resolveStripeSubscriptionPlan(subscriptionObject);
        }
    }

    let appliedPlan = user.plan || 'free';
    if (nextPlan && nextPlan !== user.plan) {
        const limits = await applyUserPlanAndLimits({
            userId: user.id,
            planId: nextPlan,
            actorUserId: user.id,
            ip: 'stripe-webhook',
            activity: 'billing_webhook_subscription_sync'
        });
        appliedPlan = limits.planId;
    }

    logActivity(user.id, null, 'billing_webhook_subscription', {
        event_type: eventType,
        subscription_id: subscriptionId || null,
        status: subscriptionStatus || null,
        plan: appliedPlan
    }, 'stripe-webhook');

    return {
        handled: true,
        user_id: user.id,
        plan: appliedPlan,
        subscription_id: subscriptionId || null,
        status: subscriptionStatus || null
    };
};

const processStripeWebhookEvent = async (event) => {
    const eventType = String(event?.type || '').trim();
    if (!eventType) return { handled: false, reason: 'event_type_missing' };

    if (eventType === 'checkout.session.completed') {
        return handleStripeCheckoutSessionCompleted(event.data?.object || {});
    }

    if (eventType === 'customer.subscription.updated' || eventType === 'customer.subscription.deleted') {
        return handleStripeSubscriptionLifecycleEvent(eventType, event.data?.object || {});
    }

    return {
        handled: false,
        reason: 'ignored_event_type',
        event_type: eventType
    };
};

app.post('/api/billing/webhook', billingRateLimit, async (req, res) => {
    if (!stripe) {
        return res.status(503).json({ error: 'Stripeが設定されていません' });
    }
    if (!CONFIG.STRIPE_WEBHOOK_SECRET) {
        return res.status(503).json({ error: 'STRIPE_WEBHOOK_SECRET が未設定です' });
    }

    const signature = String(req.headers['stripe-signature'] || '').trim();
    if (!signature) {
        return res.status(400).json({ error: 'Stripe署名ヘッダが不足しています' });
    }
    if (!Buffer.isBuffer(req.rawBody)) {
        return res.status(400).json({ error: 'Webhookリクエストボディが不正です' });
    }

    let event;
    try {
        event = stripe.webhooks.constructEvent(req.rawBody, signature, CONFIG.STRIPE_WEBHOOK_SECRET);
    } catch (e) {
        pushSystemMonitorEvent('stripeWebhookFailures', {
            stage: 'signature_verification',
            message: String(e?.message || e || 'signature_verification_failed')
        });
        console.error('Stripe webhook signature verification failed:', e.message);
        return res.status(400).json({ error: 'Webhook署名検証に失敗しました' });
    }

    const eventId = String(event.id || '').trim();
    if (!eventId) {
        return res.status(400).json({ error: 'Webhook event_id が不正です' });
    }

    const payloadHash = crypto.createHash('sha256').update(req.rawBody).digest('hex');
    const existing = stripeWebhookGetEventStmt.get(eventId);
    if (existing?.status === 'processed') {
        return res.json({ received: true, duplicate: true, status: 'processed' });
    }
    if (existing?.status === 'processing') {
        return res.status(202).json({ received: true, duplicate: true, status: 'processing' });
    }

    if (existing) {
        stripeWebhookMarkProcessingStmt.run(event.type, payloadHash, eventId);
    } else {
        stripeWebhookInsertEventStmt.run(eventId, event.type, payloadHash);
    }

    try {
        const result = await processStripeWebhookEvent(event);
        stripeWebhookMarkProcessedStmt.run(eventId);
        return res.json({ received: true, processed: true, event: event.type, result });
    } catch (e) {
        const errorText = truncateErrorText(e?.message || e);
        pushSystemMonitorEvent('stripeWebhookFailures', {
            stage: 'processing',
            event_id: eventId,
            event_type: String(event?.type || ''),
            message: errorText
        });
        stripeWebhookMarkFailedStmt.run(errorText, eventId);
        console.error('Stripe webhook processing failed:', e);
        return res.status(500).json({ error: 'Webhook処理に失敗しました' });
    }
});

app.post('/api/billing/checkout-session', authMiddleware, billingRateLimit, async (req, res) => {
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
        const baseSessionParams = {
            ...(hasStandardCoupon ? { discounts: [{ coupon: CONFIG.STRIPE_STANDARD_COUPON_ID }] } : {}),
            mode: 'subscription',
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
        };

        let session;
        let usedPaymentMethodTypes = 'dynamic';
        if (STRIPE_PAYMENT_METHOD_TYPES.length > 0) {
            try {
                session = await stripe.checkout.sessions.create({
                    ...baseSessionParams,
                    payment_method_types: STRIPE_PAYMENT_METHOD_TYPES
                });
                usedPaymentMethodTypes = STRIPE_PAYMENT_METHOD_TYPES;
            } catch (e) {
                const message = String(e?.message || '');
                const isPaymentMethodError = e?.type === 'StripeInvalidRequestError'
                    && (/payment_method_types/i.test(message) || /payment method/i.test(message));
                if (!isPaymentMethodError) throw e;

                console.warn('Invalid STRIPE_PAYMENT_METHOD_TYPES, fallback to dynamic:', {
                    configured: STRIPE_PAYMENT_METHOD_TYPES,
                    error: message
                });
                session = await stripe.checkout.sessions.create(baseSessionParams);
            }
        } else {
            session = await stripe.checkout.sessions.create(baseSessionParams);
        }

        logActivity(req.user.id, null, 'billing_checkout_create', {
            plan: requestedPlan,
            session_id: session.id,
            payment_method_types: usedPaymentMethodTypes
        }, getClientIp(req));

        res.json({ success: true, url: session.url, session_id: session.id });
    } catch (e) {
        console.error('Create checkout session error:', e);
        res.status(500).json({ error: 'チェックアウトの作成に失敗しました' });
    }
});

app.post('/api/billing/portal-session', authMiddleware, billingRateLimit, async (req, res) => {
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

        logActivity(req.user.id, null, 'billing_portal_open', { customer_id: customerId }, getClientIp(req));

        res.json({ success: true, url: portalSession.url });
    } catch (e) {
        console.error('Create billing portal session error:', e);
        res.status(500).json({ error: 'サブスク管理ページの作成に失敗しました' });
    }
});

app.post('/api/billing/confirm', authMiddleware, billingRateLimit, async (req, res) => {
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
            ip: getClientIp(req),
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

app.post('/api/billing/reapply-limits', authMiddleware, billingRateLimit, async (req, res) => {
    try {
        const limits = getEffectiveUserLimits(req.user);
        await syncContainersToPlanLimits(req.user.id, {
            planId: req.user.plan || 'free',
            ...limits
        });

        logActivity(req.user.id, null, 'plan_limits_reapply', {
            plan: req.user.plan || 'free',
            limits
        }, getClientIp(req));

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

// ============ 問い合わせAPI ============
app.get('/api/support/tickets', authMiddleware, (req, res) => {
    try {
        const includeAll = req.user.is_admin && String(req.query.scope || '').toLowerCase() === 'all';
        const tickets = listSupportTickets({ includeAll, userId: req.user.id });
        res.json(tickets);
    } catch (e) {
        console.error('Support ticket list error:', e);
        res.status(500).json({ error: '問い合わせ一覧の取得に失敗しました' });
    }
});

app.post('/api/support/tickets', authMiddleware, supportCreateRateLimit, (req, res) => {
    try {
        const category = normalizeSupportCategory(req.body?.category);
        const subject = sanitizeSupportText(req.body?.subject);
        const message = sanitizeSupportText(req.body?.message);

        if (!category || !SUPPORT_CATEGORIES.has(category)) {
            return res.status(400).json({ error: 'カテゴリが不正です' });
        }
        if (subject.length < 2 || subject.length > 120) {
            return res.status(400).json({ error: '件名は2〜120文字で入力してください' });
        }
        if (message.length < 2 || message.length > 4000) {
            return res.status(400).json({ error: '本文は2〜4000文字で入力してください' });
        }

        // ユーザーは終了していない問い合わせがある場合、同じチケットへ引き継ぐ
        if (!req.user.is_admin) {
            const activeTicket = db.prepare(`
                SELECT id
                FROM support_tickets
                WHERE user_id = ? AND status != 'closed'
                ORDER BY datetime(updated_at) DESC, datetime(created_at) DESC
                LIMIT 1
            `).get(req.user.id);

            if (activeTicket?.id) {
                const continueTx = db.transaction(() => {
                    db.prepare(`
                        INSERT INTO support_messages (ticket_id, user_id, sender_role, message)
                        VALUES (?, ?, 'user', ?)
                    `).run(activeTicket.id, req.user.id, message);

                    db.prepare(`
                        UPDATE support_tickets
                        SET status = 'waiting_admin', updated_at = CURRENT_TIMESTAMP, closed_at = NULL
                        WHERE id = ?
                    `).run(activeTicket.id);
                });
                continueTx();

                logActivity(req.user.id, null, 'support_ticket_continued', {
                    ticket_id: activeTicket.id,
                    category
                }, getClientIp(req));

                return res.json({
                    id: activeTicket.id,
                    reused: true,
                    message: '進行中のチャットに送信しました'
                });
            }
        }

        const ticketId = `ticket-${uuidv4().replace(/-/g, '').slice(0, 12)}`;
        const createTicketTx = db.transaction(() => {
            db.prepare(`
                INSERT INTO support_tickets (id, user_id, category, subject, status)
                VALUES (?, ?, ?, ?, 'waiting_admin')
            `).run(ticketId, req.user.id, category, subject);

            db.prepare(`
                INSERT INTO support_messages (ticket_id, user_id, sender_role, message)
                VALUES (?, ?, 'user', ?)
            `).run(ticketId, req.user.id, message);

            db.prepare(`
                UPDATE support_tickets
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            `).run(ticketId);
        });
        createTicketTx();

        logActivity(req.user.id, null, 'support_ticket_created', {
            ticket_id: ticketId,
            category
        }, getClientIp(req));

        sendSupportCreatedDiscordNotification({
            ticketId,
            category,
            subject,
            message,
            userName: req.user.name,
            userEmail: req.user.email
        }).catch((notifyError) => {
            console.error('Support Discord notification error:', notifyError);
        });

        res.status(201).json({
            id: ticketId,
            message: 'お問い合わせを受け付けました'
        });
    } catch (e) {
        console.error('Support ticket create error:', e);
        res.status(500).json({ error: 'お問い合わせの作成に失敗しました' });
    }
});

app.get('/api/support/tickets/:id/messages', authMiddleware, (req, res) => {
    try {
        const ticketId = sanitizeSupportText(req.params.id);
        if (!ticketId) {
            return res.status(400).json({ error: '問い合わせIDが必要です' });
        }

        const access = ensureSupportTicketAccess(ticketId, req.user);
        if (!access.ok) {
            return res.status(access.status).json({ error: access.error });
        }

        const messages = loadSupportMessages(ticketId);
        res.json({
            ticket: access.ticket,
            messages
        });
    } catch (e) {
        console.error('Support ticket messages error:', e);
        res.status(500).json({ error: '問い合わせメッセージの取得に失敗しました' });
    }
});

app.post('/api/support/tickets/:id/replies', authMiddleware, supportReplyRateLimit, (req, res) => {
    try {
        const ticketId = sanitizeSupportText(req.params.id);
        const message = sanitizeSupportText(req.body?.message);
        if (!ticketId) {
            return res.status(400).json({ error: '問い合わせIDが必要です' });
        }
        if (message.length < 1 || message.length > 4000) {
            return res.status(400).json({ error: '返信本文は1〜4000文字で入力してください' });
        }

        const access = ensureSupportTicketAccess(ticketId, req.user);
        if (!access.ok) {
            return res.status(access.status).json({ error: access.error });
        }

        if (!req.user.is_admin && access.ticket.status === 'closed') {
            return res.status(403).json({ error: 'このチャットは終了しています。新しい問い合わせを開始してください' });
        }

        const senderRole = req.user.is_admin ? 'admin' : 'user';
        const nextStatus = senderRole === 'admin' ? 'waiting_user' : 'waiting_admin';
        const replyTx = db.transaction(() => {
            db.prepare(`
                INSERT INTO support_messages (ticket_id, user_id, sender_role, message)
                VALUES (?, ?, ?, ?)
            `).run(ticketId, req.user.id, senderRole, message);

            db.prepare(`
                UPDATE support_tickets
                SET status = ?, updated_at = CURRENT_TIMESTAMP, closed_at = NULL
                WHERE id = ?
            `).run(nextStatus, ticketId);
        });
        replyTx();

        logActivity(req.user.id, null, 'support_ticket_replied', {
            ticket_id: ticketId,
            sender_role: senderRole
        }, getClientIp(req));

        res.json({
            success: true,
            status: nextStatus,
            message: '返信を送信しました'
        });
    } catch (e) {
        console.error('Support ticket reply error:', e);
        res.status(500).json({ error: '返信の送信に失敗しました' });
    }
});

app.put('/api/support/tickets/:id/status', authMiddleware, (req, res) => {
    try {
        const ticketId = sanitizeSupportText(req.params.id);
        const status = normalizeSupportStatus(req.body?.status);
        if (!ticketId) {
            return res.status(400).json({ error: '問い合わせIDが必要です' });
        }
        if (!status) {
            return res.status(400).json({ error: 'ステータスが不正です' });
        }

        const access = ensureSupportTicketAccess(ticketId, req.user);
        if (!access.ok) {
            return res.status(access.status).json({ error: access.error });
        }

        // 終了扱いは管理者のみ。ユーザーは「運営対応待ち」への変更のみ許可
        if (!req.user.is_admin && status !== 'waiting_admin') {
            return res.status(403).json({ error: 'このステータスには変更できません' });
        }

        db.prepare(`
            UPDATE support_tickets
            SET status = ?, updated_at = CURRENT_TIMESTAMP, closed_at = CASE WHEN ? = 'closed' THEN CURRENT_TIMESTAMP ELSE NULL END
            WHERE id = ?
        `).run(status, status, ticketId);

        logActivity(req.user.id, null, 'support_ticket_status_update', {
            ticket_id: ticketId,
            status
        }, getClientIp(req));

        res.json({
            success: true,
            status
        });
    } catch (e) {
        console.error('Support ticket status update error:', e);
        res.status(500).json({ error: 'ステータス更新に失敗しました' });
    }
});

app.delete('/api/support/tickets/:id', authMiddleware, (req, res) => {
    try {
        const ticketId = sanitizeSupportText(req.params.id);
        if (!ticketId) {
            return res.status(400).json({ error: '問い合わせIDが必要です' });
        }

        const access = ensureSupportTicketAccess(ticketId, req.user);
        if (!access.ok) {
            return res.status(access.status).json({ error: access.error });
        }

        if (!req.user.is_admin) {
            return res.status(403).json({ error: '削除は管理者のみ実行できます' });
        }

        const deleteTx = db.transaction(() => {
            db.prepare(`
                DELETE FROM support_messages
                WHERE ticket_id = ?
            `).run(ticketId);

            return db.prepare(`
                DELETE FROM support_tickets
                WHERE id = ?
            `).run(ticketId);
        });

        const ticketResult = deleteTx();
        if (!ticketResult || !ticketResult.changes) {
            return res.status(404).json({ error: '問い合わせが見つかりません' });
        }

        logActivity(req.user.id, null, 'support_ticket_deleted', {
            ticket_id: ticketId,
            owner_user_id: access.ticket.user_id
        }, getClientIp(req));

        res.json({
            success: true,
            message: 'お問い合わせを削除しました'
        });
    } catch (e) {
        console.error('Support ticket delete error:', e);
        res.status(500).json({ error: 'お問い合わせの削除に失敗しました' });
    }
});

app.get('/api/access-history', authMiddleware, (req, res) => {
    try {
        const requestedLimit = Number.parseInt(req.query.limit, 10);
        const limit = Number.isFinite(requestedLimit)
            ? Math.min(Math.max(requestedLimit, 1), 300)
            : 100;

        const rows = db.prepare(`
            SELECT id, event, path, method, ip_address, user_agent, created_at
            FROM access_logs
            WHERE user_id = ?
            ORDER BY datetime(created_at) DESC
            LIMIT ?
        `).all(req.user.id, limit);

        res.json(rows);
    } catch (e) {
        console.error('Access history error:', e);
        res.status(500).json({ error: 'アクセス履歴の取得に失敗しました' });
    }
});

// ============ 管理者API ============
app.get('/api/admin/mailer/config', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
    try {
        const verifyRequested = ['1', 'true', 'yes'].includes(String(req.query.verify || '').toLowerCase());
        const configured = isMailerConfigured();
        let verified = false;
        let verifyError = null;

        if (configured && verifyRequested) {
            try {
                const transporter = getMailTransporter();
                if (!transporter) {
                    verifyError = 'mailer_not_configured';
                } else {
                    await transporter.verify();
                    verified = true;
                }
            } catch (error) {
                verifyError = String(error?.message || error || 'smtp_verify_failed').slice(0, 500);
                mailTransporter = null;
            }
        }

        res.json({
            enabled: !!CONFIG.MAILER_ENABLED,
            configured,
            smtp_host: CONFIG.MAILER_SMTP_HOST || '',
            smtp_port: CONFIG.MAILER_SMTP_PORT,
            smtp_secure: !!CONFIG.MAILER_SMTP_SECURE,
            from_email: normalizeEmail(CONFIG.MAILER_FROM_EMAIL),
            from_name: CONFIG.MAILER_FROM_NAME || '',
            reply_to: normalizeEmail(CONFIG.MAILER_REPLY_TO || ''),
            max_recipients: CONFIG.MAILER_MAX_RECIPIENTS,
            send_delay_ms: CONFIG.MAILER_SEND_DELAY_MS,
            verified,
            verify_error: verifyError
        });
    } catch (e) {
        console.error('Admin mailer config error:', e);
        res.status(500).json({ error: 'メール設定の取得に失敗しました' });
    }
});

app.post('/api/admin/mailer/send', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
    try {
        const dryRun = !!req.body?.dry_run;

        const subject = String(req.body?.subject || '').trim();
        const bodyText = sanitizeMailBodyText(req.body?.body_text || req.body?.body || '');
        const bodyHtmlRaw = String(req.body?.body_html || '').trim();
        const recipientMode = String(req.body?.recipient_mode || 'all_users').trim().toLowerCase();

        if (!subject || subject.length > 200) {
            return res.status(400).json({ error: '件名は1〜200文字で入力してください' });
        }
        if (!bodyText) {
            return res.status(400).json({ error: '本文(テキスト)を入力してください' });
        }
        if (!MAILER_RECIPIENT_MODES.has(recipientMode)) {
            return res.status(400).json({ error: '送信対象モードが不正です' });
        }

        const recipients = recipientMode === 'all_users'
            ? collectRecipientsFromUsers(req.body?.plan_filters || [])
            : parseManualRecipients(req.body?.manual_recipients || '');

        if (!recipients.length) {
            return res.status(400).json({ error: '有効な宛先がありません' });
        }
        if (recipients.length > CONFIG.MAILER_MAX_RECIPIENTS) {
            return res.status(400).json({
                error: `宛先数が上限を超えています (max=${CONFIG.MAILER_MAX_RECIPIENTS})`
            });
        }

        const recipientFilter = recipientMode === 'all_users'
            ? { plan_filters: normalizeMailerPlanFilters(req.body?.plan_filters || []) }
            : { manual_count: recipients.length };

        if (dryRun) {
            return res.json({
                success: true,
                dry_run: true,
                recipient_mode: recipientMode,
                total_recipients: recipients.length,
                recipients_preview: recipients
            });
        }

        if (!isMailerConfigured()) {
            return res.status(400).json({ error: 'メール送信設定が未構成です (MAILER_* を設定してください)' });
        }

        const fromField = resolveMailerFromField();
        if (!fromField) {
            return res.status(400).json({ error: 'MAILER_FROM_EMAIL が不正です' });
        }

        const transporter = getMailTransporter();
        if (!transporter) {
            return res.status(500).json({ error: 'SMTPトランスポートの初期化に失敗しました' });
        }

        const campaignId = `mail-${uuidv4()}`;
        const bodyHtml = bodyHtmlRaw || formatMailTextToHtml(bodyText);
        const nowIso = new Date().toISOString();
        db.prepare(`
            INSERT INTO bulk_mail_campaigns (
                id, created_by, recipient_mode, recipient_filter_json,
                subject, body_text, body_html, total_recipients,
                sent_count, failed_count, status, started_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 'sending', ?)
        `).run(
            campaignId,
            req.user.id,
            recipientMode,
            JSON.stringify(recipientFilter),
            subject,
            bodyText,
            bodyHtml || null,
            recipients.length,
            nowIso
        );

        const insertDeliveryStmt = db.prepare(`
            INSERT INTO bulk_mail_deliveries (
                campaign_id, recipient_email, recipient_name, status
            )
            VALUES (?, ?, ?, 'queued')
        `);
        const updateDeliveryStmt = db.prepare(`
            UPDATE bulk_mail_deliveries
            SET status = ?, provider_message_id = ?, error_message = ?, sent_at = ?
            WHERE id = ?
        `);

        const deliveryRows = recipients.map((recipient) => {
            const result = insertDeliveryStmt.run(
                campaignId,
                recipient.email,
                recipient.name || null
            );
            return {
                id: Number(result.lastInsertRowid),
                email: recipient.email,
                name: recipient.name || null
            };
        });

        const replyTo = normalizeEmail(CONFIG.MAILER_REPLY_TO || '');
        let sentCount = 0;
        let failedCount = 0;
        const delayMs = Math.max(0, CONFIG.MAILER_SEND_DELAY_MS);

        for (const delivery of deliveryRows) {
            try {
                const sendResult = await transporter.sendMail({
                    from: fromField,
                    to: delivery.name
                        ? `"${String(delivery.name).replace(/"/g, '\\"')}" <${delivery.email}>`
                        : delivery.email,
                    replyTo: isValidEmailFormat(replyTo) ? replyTo : undefined,
                    subject,
                    text: bodyText,
                    html: bodyHtml || undefined
                });

                sentCount += 1;
                updateDeliveryStmt.run(
                    'sent',
                    String(sendResult?.messageId || '').slice(0, 255) || null,
                    null,
                    new Date().toISOString(),
                    delivery.id
                );
            } catch (error) {
                failedCount += 1;
                updateDeliveryStmt.run(
                    'failed',
                    null,
                    String(error?.message || error || 'send_failed').slice(0, 500),
                    null,
                    delivery.id
                );
            }

            if (delayMs > 0) {
                await waitMs(delayMs);
            }
        }

        const status = failedCount === 0 ? 'completed' : (sentCount > 0 ? 'partial' : 'failed');
        db.prepare(`
            UPDATE bulk_mail_campaigns
            SET sent_count = ?, failed_count = ?, status = ?, completed_at = ?
            WHERE id = ?
        `).run(sentCount, failedCount, status, new Date().toISOString(), campaignId);

        logActivity(req.user.id, null, 'admin_bulk_mail_send', {
            campaign_id: campaignId,
            recipient_mode: recipientMode,
            total_recipients: recipients.length,
            sent_count: sentCount,
            failed_count: failedCount,
            subject
        }, getClientIp(req));

        res.json({
            success: true,
            campaign_id: campaignId,
            status,
            total_recipients: recipients.length,
            sent_count: sentCount,
            failed_count: failedCount
        });
    } catch (e) {
        console.error('Admin bulk mail send error:', e);
        res.status(500).json({ error: '一括メール送信に失敗しました' });
    }
});

app.get('/api/admin/mailer/history', authMiddleware, adminMiddleware, adminRateLimit, (req, res) => {
    try {
        const requestedLimit = Number.parseInt(req.query.limit, 10);
        const limit = Number.isFinite(requestedLimit)
            ? Math.min(Math.max(requestedLimit, 1), 500)
            : 100;

        const rows = db.prepare(`
            SELECT c.*,
                   u.email AS created_by_email,
                   u.name AS created_by_name
            FROM bulk_mail_campaigns c
            LEFT JOIN users u ON u.id = c.created_by
            ORDER BY datetime(c.created_at) DESC
            LIMIT ?
        `).all(limit);

        const campaigns = rows.map((row) => {
            let recipientFilter = null;
            try {
                recipientFilter = row.recipient_filter_json ? JSON.parse(row.recipient_filter_json) : null;
            } catch {
                recipientFilter = null;
            }
            return {
                ...row,
                recipient_filter: recipientFilter
            };
        });

        res.json(campaigns);
    } catch (e) {
        console.error('Admin mailer history error:', e);
        res.status(500).json({ error: 'メール配信履歴の取得に失敗しました' });
    }
});

app.get('/api/admin/mailer/history/:id', authMiddleware, adminMiddleware, adminRateLimit, (req, res) => {
    try {
        const campaignId = String(req.params.id || '').trim();
        if (!campaignId) {
            return res.status(400).json({ error: 'キャンペーンIDが必要です' });
        }

        const campaign = db.prepare(`
            SELECT c.*,
                   u.email AS created_by_email,
                   u.name AS created_by_name
            FROM bulk_mail_campaigns c
            LEFT JOIN users u ON u.id = c.created_by
            WHERE c.id = ?
            LIMIT 1
        `).get(campaignId);
        if (!campaign) {
            return res.status(404).json({ error: 'キャンペーンが見つかりません' });
        }

        const requestedLimit = Number.parseInt(req.query.limit, 10);
        const limit = Number.isFinite(requestedLimit)
            ? Math.min(Math.max(requestedLimit, 1), 10000)
            : 2000;
        const deliveries = db.prepare(`
            SELECT id, campaign_id, recipient_email, recipient_name, status,
                   provider_message_id, error_message, created_at, sent_at
            FROM bulk_mail_deliveries
            WHERE campaign_id = ?
            ORDER BY id ASC
            LIMIT ?
        `).all(campaignId, limit);

        let recipientFilter = null;
        try {
            recipientFilter = campaign.recipient_filter_json ? JSON.parse(campaign.recipient_filter_json) : null;
        } catch {
            recipientFilter = null;
        }

        res.json({
            campaign: {
                ...campaign,
                recipient_filter: recipientFilter
            },
            deliveries
        });
    } catch (e) {
        console.error('Admin mailer history detail error:', e);
        res.status(500).json({ error: 'メール配信詳細の取得に失敗しました' });
    }
});

app.get('/api/admin/users', authMiddleware, adminMiddleware, adminRateLimit, (req, res) => {
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

app.get('/api/admin/containers', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
    const containers = db.prepare(`
        SELECT c.*, u.email as user_email, u.name as user_name, u.discord_id as user_discord_id
        FROM containers c
        JOIN users u ON c.user_id = u.id
        ORDER BY c.created_at DESC
    `).all();

    const containersWithResources = await Promise.all(containers.map(async (c) => {
        let realStatus = c.status;
        const isVmBackend = isVmBackendRef(c.docker_id);
        const vmName = isVmBackend ? c.docker_id.replace(/^vm:/, '') : null;
        const vmInfo = isVmBackend ? await getVmBackendInfo(vmName) : null;

        if (dockerAvailable && isDockerBackendRef(c.docker_id)) {
            try {
                const dockerContainer = docker.getContainer(c.docker_id);
                const inspect = await dockerContainer.inspect();
                realStatus = inspect.State.Running ? 'running' : 'stopped';

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

        const dockerStats = realStatus === 'running' && isDockerBackendRef(c.docker_id)
            ? await getContainerStats(c.docker_id, c.id)
            : { cpuUsage: 0, memoryUsage: 0, networkIn: 0, networkOut: 0, diskUsageBytes: 0 };
        const hasDockerStats = realStatus === 'running' && isDockerBackendRef(c.docker_id);
        const hasVmStats = realStatus === 'running' && isVmBackend && !!vmInfo?.stats;

        const configuredCpuLimit = c.cpu_limit || 0.5;
        const configuredMemoryLimit = c.memory_limit || 512;
        const configuredStorageLimit = c.storage_limit || 1024;
        const cpuLimit = vmInfo?.cpu_limit || configuredCpuLimit;
        const memoryLimit = vmInfo?.memory_limit || configuredMemoryLimit;
        const storageLimit = vmInfo?.storage_limit || configuredStorageLimit;
        const storageLimitBytes = storageLimit * 1024 * 1024;
        const diskUsagePercent = storageLimitBytes > 0
            ? ((dockerStats.diskUsageBytes / storageLimitBytes) * 100).toFixed(1)
            : 0;

        const cpuUsage = hasDockerStats
            ? (dockerStats.cpuUsage || 0)
            : (hasVmStats && Number.isFinite(vmInfo.stats.cpuUsage) ? vmInfo.stats.cpuUsage : 0);
        const memoryUsage = hasDockerStats
            ? (dockerStats.memoryUsage || 0)
            : (hasVmStats && Number.isFinite(vmInfo.stats.memoryUsage) ? vmInfo.stats.memoryUsage : 0);
        const networkIn = hasDockerStats
            ? formatBytes(dockerStats.networkIn || 0)
            : (hasVmStats && Number.isFinite(vmInfo.stats.networkInBytes) ? formatBytes(vmInfo.stats.networkInBytes) : '0 B');
        const networkOut = hasDockerStats
            ? formatBytes(dockerStats.networkOut || 0)
            : (hasVmStats && Number.isFinite(vmInfo.stats.networkOutBytes) ? formatBytes(vmInfo.stats.networkOutBytes) : '0 B');
        const vmTtydLink = isVmBackend
            ? ensureVmTtydStableLinkSeed({
                containerId: c.id,
                vmName,
                username: c.ssh_user,
                password: c.ssh_password,
                ttydPort: CONFIG.VM_TTYD_PORT
            })
            : null;

        return {
            ...c,
            status: realStatus,
            backend_type: isVmBackend ? 'vm' : 'docker',
            backend_target: isVmBackend ? vmName : c.docker_id,
            resource_source: vmInfo?.available ? 'vm' : 'configured',
            ssh_host: CONFIG.SSH_HOST,
            ssh_port: CONFIG.SSH_PORT,
            ssh_command: c.ssh_user ? `ssh ${c.ssh_user}@${CONFIG.SSH_HOST} -p ${CONFIG.SSH_PORT}` : null,
            vm_ttyd_url: vmTtydLink ? buildVmTtydStableUrl(vmTtydLink.link_token) : null,
            vm_ttyd_username: vmTtydLink?.username || null,
            vm_ttyd_password: vmTtydLink?.password || null,
            vm_ttyd_port: vmTtydLink?.ttyd_port || null,
            cpu_limit: cpuLimit,
            memory_limit: memoryLimit,
            storage_limit: storageLimit,
            configured_cpu_limit: configuredCpuLimit,
            configured_memory_limit: configuredMemoryLimit,
            configured_storage_limit: configuredStorageLimit,
            cpu_usage: cpuUsage,
            memory_usage: memoryUsage,
            network_in: networkIn,
            network_out: networkOut,
            stats: {
                available: hasDockerStats || hasVmStats,
                cpuUsage: hasDockerStats ? dockerStats.cpuUsage : (hasVmStats ? vmInfo.stats.cpuUsage : null),
                memoryUsage: hasDockerStats ? dockerStats.memoryUsage : (hasVmStats ? vmInfo.stats.memoryUsage : null),
                networkIn: hasDockerStats
                    ? formatBytes(dockerStats.networkIn)
                    : (hasVmStats && Number.isFinite(vmInfo.stats.networkInBytes) ? formatBytes(vmInfo.stats.networkInBytes) : null),
                networkOut: hasDockerStats
                    ? formatBytes(dockerStats.networkOut)
                    : (hasVmStats && Number.isFinite(vmInfo.stats.networkOutBytes) ? formatBytes(vmInfo.stats.networkOutBytes) : null),
                diskUsage: hasDockerStats
                    ? formatBytes(dockerStats.diskUsageBytes)
                    : (hasVmStats && Number.isFinite(vmInfo.stats.diskUsageBytes) ? formatBytes(vmInfo.stats.diskUsageBytes) : null),
                diskUsageBytes: hasDockerStats
                    ? dockerStats.diskUsageBytes
                    : (hasVmStats && Number.isFinite(vmInfo.stats.diskUsageBytes) ? vmInfo.stats.diskUsageBytes : null),
                diskUsagePercent: hasDockerStats ? diskUsagePercent : (hasVmStats ? vmInfo.stats.diskUsagePercent : null)
            }
        };
    }));

    res.json(containersWithResources);
});

app.get('/api/admin/logs', authMiddleware, adminMiddleware, adminRateLimit, (req, res) => {
    const requestedLimit = Number.parseInt(req.query.limit, 10);
    const limit = Number.isFinite(requestedLimit)
        ? Math.min(Math.max(requestedLimit, 1), 1000)
        : 300;

    const logs = db.prepare(`
        SELECT l.*, u.email AS user_email, u.name AS user_name
        FROM activity_logs l
        LEFT JOIN users u ON u.id = l.user_id
        ORDER BY datetime(l.created_at) DESC
        LIMIT ?
    `).all(limit);
    res.json(logs);
});

app.get('/api/admin/access-logs', authMiddleware, adminMiddleware, adminRateLimit, (req, res) => {
    try {
        const requestedLimit = Number.parseInt(req.query.limit, 10);
        const limit = Number.isFinite(requestedLimit)
            ? Math.min(Math.max(requestedLimit, 1), 1000)
            : 500;

        const logs = db.prepare(`
            SELECT a.*, u.email AS user_email, u.name AS user_name
            FROM access_logs a
            LEFT JOIN users u ON u.id = a.user_id
            ORDER BY datetime(a.created_at) DESC
            LIMIT ?
        `).all(limit);

        res.json(logs);
    } catch (e) {
        console.error('Admin access logs error:', e);
        res.status(500).json({ error: 'アクセス履歴の取得に失敗しました' });
    }
});

app.get('/api/admin/compliance/summary', authMiddleware, adminMiddleware, adminRateLimit, (req, res) => {
    try {
        const summary = db.prepare(`
            SELECT
                COALESCE(SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END), 0) AS open_count,
                COALESCE(SUM(CASE WHEN status = 'investigating' THEN 1 ELSE 0 END), 0) AS investigating_count,
                COALESCE(SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END), 0) AS resolved_count,
                COALESCE(SUM(CASE WHEN status = 'ignored' THEN 1 ELSE 0 END), 0) AS ignored_count,
                COALESCE(SUM(CASE WHEN datetime(last_detected_at) >= datetime('now', '-24 hours') THEN 1 ELSE 0 END), 0) AS detected_24h_count,
                COALESCE(SUM(CASE WHEN status IN ('open', 'investigating') AND severity IN ('high', 'critical') THEN 1 ELSE 0 END), 0) AS high_open_count
            FROM compliance_incidents
        `).get();

        res.json({
            open: summary.open_count || 0,
            investigating: summary.investigating_count || 0,
            resolved: summary.resolved_count || 0,
            ignored: summary.ignored_count || 0,
            detected_24h: summary.detected_24h_count || 0,
            high_open: summary.high_open_count || 0
        });
    } catch (e) {
        console.error('Admin compliance summary error:', e);
        res.status(500).json({ error: '規約監視サマリーの取得に失敗しました' });
    }
});

app.get('/api/admin/compliance/incidents', authMiddleware, adminMiddleware, adminRateLimit, (req, res) => {
    try {
        const statusRaw = String(req.query.status || 'open').trim().toLowerCase();
        const status = statusRaw === 'all' ? 'all' : statusRaw;
        if (status !== 'all' && !COMPLIANCE_STATUSES.has(status)) {
            return res.status(400).json({ error: 'ステータスが不正です' });
        }

        const requestedLimit = Number.parseInt(req.query.limit, 10);
        const limit = Number.isFinite(requestedLimit)
            ? Math.min(Math.max(requestedLimit, 1), 1000)
            : 300;

        const whereClause = status === 'all' ? '' : 'WHERE i.status = ?';
        const params = status === 'all' ? [limit] : [status, limit];
        const incidents = db.prepare(`
            SELECT i.*,
                   u.email AS user_email,
                   u.name AS user_name,
                   ru.email AS resolved_by_email,
                   ru.name AS resolved_by_name
            FROM compliance_incidents i
            LEFT JOIN users u ON u.id = i.user_id
            LEFT JOIN users ru ON ru.id = i.resolved_by
            ${whereClause}
            ORDER BY
                CASE i.status
                    WHEN 'open' THEN 0
                    WHEN 'investigating' THEN 1
                    WHEN 'resolved' THEN 2
                    ELSE 3
                END,
                datetime(i.last_detected_at) DESC
            LIMIT ?
        `).all(...params);

        res.json(incidents);
    } catch (e) {
        console.error('Admin compliance incidents error:', e);
        res.status(500).json({ error: '規約監視インシデントの取得に失敗しました' });
    }
});

app.put('/api/admin/compliance/incidents/:id/status', authMiddleware, adminMiddleware, adminRateLimit, (req, res) => {
    try {
        const incidentId = String(req.params.id || '').trim();
        const nextStatus = String(req.body?.status || '').trim().toLowerCase();
        const resolutionNote = sanitizeSupportText(req.body?.note || '');

        if (!incidentId) {
            return res.status(400).json({ error: 'インシデントIDが必要です' });
        }
        if (!COMPLIANCE_STATUSES.has(nextStatus)) {
            return res.status(400).json({ error: 'ステータスが不正です' });
        }

        const incident = db.prepare(`
            SELECT id, user_id, container_id, rule_key, status
            FROM compliance_incidents
            WHERE id = ?
        `).get(incidentId);
        if (!incident) {
            return res.status(404).json({ error: 'インシデントが見つかりません' });
        }

        if (nextStatus === 'resolved' || nextStatus === 'ignored') {
            db.prepare(`
                UPDATE compliance_incidents
                SET status = ?,
                    resolved_by = ?,
                    resolved_at = CURRENT_TIMESTAMP,
                    resolution_note = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            `).run(nextStatus, req.user.id, resolutionNote || null, incidentId);
        } else {
            db.prepare(`
                UPDATE compliance_incidents
                SET status = ?,
                    resolved_by = NULL,
                    resolved_at = NULL,
                    resolution_note = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            `).run(nextStatus, incidentId);
        }

        logActivity(req.user.id, incident.container_id || null, 'admin_compliance_status_update', {
            incident_id: incidentId,
            previous_status: incident.status,
            status: nextStatus,
            rule_key: incident.rule_key,
            target_user: incident.user_id
        }, getClientIp(req));

        const updated = db.prepare(`
            SELECT i.*,
                   u.email AS user_email,
                   u.name AS user_name,
                   ru.email AS resolved_by_email,
                   ru.name AS resolved_by_name
            FROM compliance_incidents i
            LEFT JOIN users u ON u.id = i.user_id
            LEFT JOIN users ru ON ru.id = i.resolved_by
            WHERE i.id = ?
            LIMIT 1
        `).get(incidentId);

        res.json({
            success: true,
            incident: updated
        });
    } catch (e) {
        console.error('Admin compliance incident status update error:', e);
        res.status(500).json({ error: 'インシデント更新に失敗しました' });
    }
});

app.post('/api/admin/compliance/scan', authMiddleware, adminMiddleware, adminRateLimit, (req, res) => {
    try {
        const users = db.prepare('SELECT id FROM users').all();
        let scannedUsers = 0;
        for (const row of users) {
            const userId = String(row.id || '').trim();
            if (!userId) continue;
            runComplianceEvaluationForUser(userId);
            scannedUsers += 1;
        }

        const openCount = db.prepare(`
            SELECT COUNT(1) AS count
            FROM compliance_incidents
            WHERE status IN ('open', 'investigating')
        `).get()?.count || 0;

        logActivity(req.user.id, null, 'admin_compliance_scan', {
            scanned_users: scannedUsers,
            open_incidents: openCount
        }, getClientIp(req));

        res.json({
            success: true,
            scanned_users: scannedUsers,
            open_incidents: openCount
        });
    } catch (e) {
        console.error('Admin compliance scan error:', e);
        res.status(500).json({ error: '規約監視スキャンに失敗しました' });
    }
});

// ユーザー制限更新API
app.put('/api/admin/users/:id/limits', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
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
    }, getClientIp(req));
    
    res.json({
        success: true,
        message: 'ユーザー設定を更新しました',
        limits: updatedLimits
    });
});

// ユーザー削除API
app.delete('/api/admin/users/:id', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
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
        
        logActivity(req.user.id, null, 'admin_delete_user', { deleted_user: user.id, email: user.email }, getClientIp(req));
        
        res.json({ success: true, message: 'ユーザーを削除しました' });
    } catch (e) {
        console.error('Delete user error:', e);
        res.status(500).json({ error: 'ユーザーの削除に失敗しました' });
    }
});

// 統計情報API
app.get('/api/admin/stats', authMiddleware, adminMiddleware, adminRateLimit, (req, res) => {
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

app.get('/api/admin/system/health', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
    try {
        const snapshot = await collectSystemHealthSnapshot();
        res.json({
            success: true,
            snapshot,
            monitor: {
                enabled: SYSTEM_MONITOR_CONFIG.enabled,
                interval_seconds: SYSTEM_MONITOR_CONFIG.intervalSeconds,
                alert_cooldown_minutes: SYSTEM_MONITOR_CONFIG.alertCooldownMinutes,
                api_5xx_window_seconds: SYSTEM_MONITOR_CONFIG.api5xxWindowSeconds,
                api_5xx_warn_count: SYSTEM_MONITOR_CONFIG.api5xxWarnCount,
                backup_stale_minutes: SYSTEM_MONITOR_CONFIG.backupStaleMinutes,
                backup_failure_window_minutes: SYSTEM_MONITOR_CONFIG.backupFailureWindowMinutes,
                backup_failure_warn_count: SYSTEM_MONITOR_CONFIG.backupFailureWarnCount,
                stripe_failure_window_minutes: SYSTEM_MONITOR_CONFIG.stripeFailureWindowMinutes,
                stripe_failure_warn_count: SYSTEM_MONITOR_CONFIG.stripeFailureWarnCount,
                vm_stopped_grace_minutes: SYSTEM_MONITOR_CONFIG.vmStoppedGraceMinutes,
                vm_stopped_check_limit: SYSTEM_MONITOR_CONFIG.vmStoppedCheckLimit
            },
            backup: {
                enabled: AUTO_BACKUP_CONFIG.enabled,
                mode: AUTO_BACKUP_CONFIG.mode,
                startup_mode: AUTO_BACKUP_CONFIG.startupMode,
                interval_minutes: AUTO_BACKUP_CONFIG.intervalMinutes,
                timeout_minutes: AUTO_BACKUP_CONFIG.timeoutMinutes,
                retention_days: AUTO_BACKUP_CONFIG.retentionDays,
                running: autoBackupRunning,
                last_run_at: lastAutoBackupAt
            }
        });
    } catch (e) {
        console.error('Admin system health error:', e);
        res.status(500).json({ error: 'システム状態の取得に失敗しました' });
    }
});

app.get('/api/admin/system/backups', authMiddleware, adminMiddleware, adminRateLimit, (req, res) => {
    try {
        const requestedLimit = Number.parseInt(req.query.limit, 10);
        const limit = Number.isFinite(requestedLimit)
            ? Math.min(Math.max(requestedLimit, 1), 500)
            : 100;

        const files = listAutoBackupFiles().slice(0, limit).map((item) => ({
            name: item.name,
            size: item.size,
            modified_at: new Date(item.mtimeMs).toISOString()
        }));

        res.json({
            success: true,
            files
        });
    } catch (e) {
        console.error('Admin list backups error:', e);
        res.status(500).json({ error: 'バックアップ一覧の取得に失敗しました' });
    }
});

app.post('/api/admin/system/backup', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
    try {
        const reasonRaw = String(req.body?.reason || 'admin-manual').trim();
        const reason = reasonRaw ? reasonRaw.slice(0, 48) : 'admin-manual';
        const mode = normalizeAutoBackupMode(req.body?.mode, AUTO_BACKUP_CONFIG.mode);
        const result = await createSystemBackupSnapshot({
            reason,
            actorUserId: req.user.id,
            force: true,
            modeOverride: mode
        });

        if (!result.ok) {
            return res.status(500).json({
                error: 'バックアップの作成に失敗しました',
                reason: result.error || result.reason || 'unknown'
            });
        }

        res.json({
            success: true,
            backup_name: result.backupName,
            mode: result.mode || mode,
            components: result.components || ['db'],
            size_bytes: result.sizeBytes,
            elapsed_ms: result.elapsedMs,
            removed_count: result.removedCount
        });
    } catch (e) {
        console.error('Admin manual backup error:', e);
        res.status(500).json({ error: 'バックアップの作成に失敗しました' });
    }
});

// ============ 管理者用サーバー操作API ============

// 管理者用: サーバー起動
app.post('/api/admin/containers/:id/start', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
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
                            const startupScript = buildDetachedContainerStartupScript(container.startup_command);
                            const exec = await dockerContainer.exec({
                                Cmd: ['bash', '-lc', startupScript],
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
        
        logActivity(req.user.id, req.params.id, 'admin_container_start', { container_name: container.name }, getClientIp(req));
        
        res.json({ success: true, message: 'サーバーを起動しました' });
    } catch (e) {
        console.error('Admin start container error:', e);
        res.status(500).json({ error: 'サーバーの起動に失敗しました' });
    }
});

// 管理者用: サーバー停止
app.post('/api/admin/containers/:id/stop', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
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
        
        logActivity(req.user.id, req.params.id, 'admin_container_stop', { container_name: container.name }, getClientIp(req));
        
        res.json({ success: true, message: 'サーバーを停止しました' });
    } catch (e) {
        console.error('Admin stop container error:', e);
        res.status(500).json({ error: 'サーバーの停止に失敗しました' });
    }
});

// 管理者用: サーバー再起動
app.post('/api/admin/containers/:id/restart', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
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
                            const startupScript = buildDetachedContainerStartupScript(container.startup_command);
                            const exec = await dockerContainer.exec({
                                Cmd: ['bash', '-lc', startupScript],
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
        
        logActivity(req.user.id, req.params.id, 'admin_container_restart', { container_name: container.name }, getClientIp(req));
        
        res.json({ success: true, message: 'サーバーを再起動しました' });
    } catch (e) {
        console.error('Admin restart container error:', e);
        res.status(500).json({ error: 'サーバーの再起動に失敗しました' });
    }
});

// 管理者用: サーバー削除
app.delete('/api/admin/containers/:id', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
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
        
        logActivity(req.user.id, req.params.id, 'admin_container_delete', { container_name: container.name }, getClientIp(req));
        
        res.json({ success: true, message: 'サーバーを削除しました' });
    } catch (e) {
        console.error('Admin delete container error:', e);
        res.status(500).json({ error: 'サーバーの削除に失敗しました' });
    }
});

// 管理者用: サーバーのリソース制限を更新
app.put('/api/admin/containers/:id/limits', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
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
        }, getClientIp(req));
        
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
app.put('/api/admin/users/:id/plan', authMiddleware, adminMiddleware, adminRateLimit, async (req, res) => {
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
            ip: getClientIp(req),
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
const vmTtydProxyWss = new WebSocketServer({ noServer: true });

const parseVmTtydWsRequest = (rawUrl = '') => {
    const parsed = new URL(rawUrl, 'http://localhost');
    const match = parsed.pathname.match(/^\/vm-ttyd\/([a-zA-Z0-9_-]{16,128})(\/.*)?$/);
    if (!match) return null;
    const token = match[1];
    const upstreamPath = match[2] || '/';
    return {
        token,
        upstreamPath: upstreamPath.startsWith('/') ? upstreamPath : `/${upstreamPath}`,
        search: parsed.search || ''
    };
};

const parseVmTtydTokenFromReferer = (rawReferer = '') => {
    const referer = String(rawReferer || '').trim();
    if (!referer) return null;
    try {
        const parsed = new URL(referer);
        const match = parsed.pathname.match(/^\/vm-ttyd\/([a-zA-Z0-9_-]{16,128})(\/.*)?$/);
        return match ? match[1] : null;
    } catch {
        return null;
    }
};

const closeWsSafely = (ws, code = 1011, reason = 'proxy error') => {
    try {
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
            ws.close(code, reason);
        }
    } catch {}
};

const proxyVmTtydWs = async (clientWs, request, parsedRequest) => {
    const record = loadVmTtydLinkByToken(parsedRequest.token);
    if (!record?.target_url) {
        closeWsSafely(clientWs, 1008, 'link not found');
        return;
    }

    let targetBase = normalizeVmTtydTargetUrl(record.target_url);
    if (!targetBase) {
        closeWsSafely(clientWs, 1011, 'invalid target');
        return;
    }

    const connectUpstream = () => {
        const wsProtocol = targetBase.protocol === 'https:' ? 'wss:' : 'ws:';
        const upstreamUrl = `${wsProtocol}//${targetBase.host}${parsedRequest.upstreamPath}${parsedRequest.search}`;
        const protocolHeader = request.headers['sec-websocket-protocol'];
        const protocols = typeof protocolHeader === 'string'
            ? protocolHeader.split(',').map((part) => part.trim()).filter(Boolean)
            : [];
        const headers = {
            Origin: `${targetBase.protocol}//${targetBase.host}`
        };
        if (record.username && record.password) {
            headers.Authorization = `Basic ${Buffer.from(`${record.username}:${record.password}`).toString('base64')}`;
        }
        return new WebSocket(upstreamUrl, protocols, { headers });
    };

    let upstreamWs = connectUpstream();

    const attachBridge = () => {
        upstreamWs.on('open', () => {});
        upstreamWs.on('message', (data, isBinary) => {
            if (clientWs.readyState !== WebSocket.OPEN) return;
            clientWs.send(data, { binary: isBinary });
        });
        upstreamWs.on('close', (code, reason) => {
            closeWsSafely(clientWs, Number.isInteger(code) ? code : 1000, String(reason || 'closed'));
        });
        upstreamWs.on('error', () => {
            scheduleVmTtydRefresh(record, 'ws-upstream-error');
            closeWsSafely(clientWs, 1013, 'terminal preparing');
        });
    };

    attachBridge();

    clientWs.on('message', (data, isBinary) => {
        if (upstreamWs.readyState === WebSocket.OPEN) {
            upstreamWs.send(data, { binary: isBinary });
        }
    });
    clientWs.on('close', () => {
        closeWsSafely(upstreamWs, 1000, 'client closed');
    });
    clientWs.on('error', () => {
        closeWsSafely(upstreamWs, 1011, 'client error');
    });
};

server.on('upgrade', (req, socket, head) => {
    const parsedUrl = new URL(req.url || '/', 'http://localhost');
    if (parsedUrl.pathname === '/ws') {
        const refererToken = parseVmTtydTokenFromReferer(req.headers.referer || '');
        if (refererToken) {
            const parsedRequest = {
                token: refererToken,
                upstreamPath: '/ws',
                search: parsedUrl.search || ''
            };
            vmTtydProxyWss.handleUpgrade(req, socket, head, (clientWs) => {
                proxyVmTtydWs(clientWs, req, parsedRequest).catch(() => {
                    closeWsSafely(clientWs, 1011, 'proxy error');
                });
            });
            return;
        }
        wss.handleUpgrade(req, socket, head, (ws) => {
            wss.emit('connection', ws, req);
        });
        return;
    }

    const parsedRequest = parseVmTtydWsRequest(req.url || '');
    if (parsedRequest) {
        vmTtydProxyWss.handleUpgrade(req, socket, head, (clientWs) => {
            proxyVmTtydWs(clientWs, req, parsedRequest).catch(() => {
                closeWsSafely(clientWs, 1011, 'proxy error');
            });
        });
        return;
    }

    socket.write('HTTP/1.1 400 Bad Request\r\n\r\n');
    socket.destroy();
});

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

app.get('/api/health', async (_req, res) => {
    try {
        const snapshot = await collectSystemHealthSnapshot();
        res.json({
            ok: true,
            timestamp: new Date().toISOString(),
            service: 'shironeko-server',
            docker_available: dockerAvailable,
            uptime_sec: snapshot.uptimeSec,
            rss_mb: snapshot.rssMb,
            db_size_mb: snapshot.dbSizeMb,
            disk_usage_percent: snapshot.diskUsagePercent
        });
    } catch (e) {
        res.status(500).json({
            ok: false,
            error: 'health_check_failed'
        });
    }
});

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
    startAutoBackupScheduler();
    startSystemMonitorScheduler();
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
    if (autoBackupTimer) clearInterval(autoBackupTimer);
    if (systemMonitorTimer) clearInterval(systemMonitorTimer);
    db.close();
    server.close();
    process.exit(0);
});
