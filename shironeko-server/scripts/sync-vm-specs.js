#!/usr/bin/env node

const { execFileSync } = require('child_process');
const Database = require('better-sqlite3');

const DB_PATH = process.env.DB_PATH || '/app/data/shironeko.db';
const LIBVIRT_DEFAULT_URI = process.env.LIBVIRT_DEFAULT_URI || 'qemu:///system';
const APPLY = process.argv.includes('--apply');
const DISK_TARGETS = ['vda', 'sda', 'vdb'];

function runVirsh(args) {
    try {
        return execFileSync('virsh', args, {
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'pipe'],
            env: { ...process.env, LIBVIRT_DEFAULT_URI }
        });
    } catch {
        return null;
    }
}

function parseDomInfo(output) {
    const cpuMatch = output.match(/^\s*CPU\(s\):\s+(\d+)/mi);
    const memoryMatch = output.match(/^\s*Max memory:\s+(\d+)\s+KiB/mi);
    if (!cpuMatch || !memoryMatch) return null;

    const cpu = parseInt(cpuMatch[1], 10);
    const memoryKiB = parseInt(memoryMatch[1], 10);
    if (!Number.isFinite(cpu) || !Number.isFinite(memoryKiB) || cpu <= 0 || memoryKiB <= 0) {
        return null;
    }

    return {
        cpuLimit: cpu,
        memoryLimitMb: Math.floor(memoryKiB / 1024)
    };
}

function getDiskLimitMb(vmName) {
    for (const target of DISK_TARGETS) {
        const blkInfo = runVirsh(['domblkinfo', vmName, target]);
        if (!blkInfo) continue;
        const capacityMatch = blkInfo.match(/^\s*Capacity:\s+(\d+)/mi);
        if (!capacityMatch) continue;
        const capacityBytes = parseInt(capacityMatch[1], 10);
        if (Number.isFinite(capacityBytes) && capacityBytes > 0) {
            return Math.floor(capacityBytes / (1024 * 1024));
        }
    }
    return null;
}

function getVmSpec(vmName) {
    const domInfo = runVirsh(['dominfo', vmName]);
    if (!domInfo) return null;

    const parsed = parseDomInfo(domInfo);
    if (!parsed) return null;

    const storageLimitMb = getDiskLimitMb(vmName);
    if (!storageLimitMb) return null;

    return {
        cpuLimit: parsed.cpuLimit,
        memoryLimitMb: parsed.memoryLimitMb,
        storageLimitMb
    };
}

function main() {
    const db = new Database(DB_PATH);
    const containers = db.prepare(`
        SELECT id, name, docker_id, cpu_limit, memory_limit, storage_limit
        FROM containers
        WHERE docker_id LIKE 'vm:%'
        ORDER BY created_at ASC
    `).all();

    if (containers.length === 0) {
        console.log('No VM-backed containers found.');
        db.close();
        return;
    }

    const updateStmt = db.prepare(`
        UPDATE containers
        SET cpu_limit = ?, memory_limit = ?, storage_limit = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    `);

    let updated = 0;
    let skipped = 0;

    for (const container of containers) {
        const vmName = String(container.docker_id || '').replace(/^vm:/, '');
        const spec = getVmSpec(vmName);
        if (!spec) {
            skipped += 1;
            console.log(`[skip] ${container.id} (${vmName}) -> spec not available`);
            continue;
        }

        const same =
            Number(container.cpu_limit) === Number(spec.cpuLimit) &&
            Number(container.memory_limit) === Number(spec.memoryLimitMb) &&
            Number(container.storage_limit) === Number(spec.storageLimitMb);

        if (same) {
            console.log(`[ok]   ${container.id} (${vmName}) already synced`);
            continue;
        }

        if (APPLY) {
            updateStmt.run(spec.cpuLimit, spec.memoryLimitMb, spec.storageLimitMb, container.id);
        }

        updated += 1;
        console.log(
            `[${APPLY ? 'sync' : 'plan'}] ${container.id} (${vmName}) ` +
            `${container.cpu_limit}/${container.memory_limit}/${container.storage_limit} -> ` +
            `${spec.cpuLimit}/${spec.memoryLimitMb}/${spec.storageLimitMb}`
        );
    }

    db.close();

    console.log(`done: updated=${updated}, skipped=${skipped}, mode=${APPLY ? 'apply' : 'dry-run'}`);
    if (!APPLY) {
        console.log('run with --apply to write changes');
    }
}

main();
