# SSD Migration Runbook (No Host Reboot)

## Goal
- Keep the host Ubuntu and VM runtime stable.
- Move VM disk storage off `/tmp/libvirt-images` to the new SSD mount.
- Do not delete existing data during migration.

## Current Risk
- Root filesystem was near full.
- VM disk files (`*.qcow2`) currently live under `/tmp/libvirt-images` (not ideal for long-term operation).

## Preconditions
- New SSD is attached and visible from host.
- Windows partition/data on SSD must remain untouched.
- You have enough unallocated SSD space for Linux partition.

## Step 1: Identify target disk and partition safely
```bash
lsblk -f
sudo fdisk -l
```
- Confirm which device is the new SSD.
- Do not modify existing Windows NTFS partitions.

## Step 2: Create Linux partition in free space
- Use `gdisk`/`fdisk` only on free/unallocated area.
- Create one Linux partition and format as `ext4`.
```bash
sudo mkfs.ext4 /dev/<NEW_PARTITION>
```

## Step 3: Mount new SSD path
```bash
sudo mkdir -p /mnt/vmdata
sudo mount /dev/<NEW_PARTITION> /mnt/vmdata
df -h /mnt/vmdata
```

## Step 4: Prepare destination for VM images
```bash
sudo mkdir -p /mnt/vmdata/libvirt-images
sudo chown root:root /mnt/vmdata/libvirt-images
sudo chmod 755 /mnt/vmdata/libvirt-images
```

## Step 5: Stop affected VMs before file copy
```bash
virsh list --all
virsh shutdown vm-server-97161dd4-9eb
virsh shutdown vm-server-fdc43086-702
```
- Wait until both are `shut off`.

## Step 6: Copy VM disk files (no delete)
```bash
sudo rsync -aH --info=progress2 /tmp/libvirt-images/ /mnt/vmdata/libvirt-images/
```

## Step 7: Repoint libvirt domain disk paths
- Dump, edit, and redefine XML per VM.
```bash
virsh dumpxml vm-server-97161dd4-9eb > /tmp/vm1.xml
virsh dumpxml vm-server-fdc43086-702 > /tmp/vm2.xml
```
- Replace disk source path from:
  - `/tmp/libvirt-images/<name>.qcow2`
  to
  - `/mnt/vmdata/libvirt-images/<name>.qcow2`
```bash
virsh define /tmp/vm1.xml
virsh define /tmp/vm2.xml
```

## Step 8: Start VMs and verify
```bash
virsh start vm-server-97161dd4-9eb
virsh start vm-server-fdc43086-702
virsh domblklist vm-server-97161dd4-9eb
virsh domblklist vm-server-fdc43086-702
```
- Confirm each VM now points to `/mnt/vmdata/libvirt-images/...`.

## Step 9: Persist mount (`/etc/fstab`)
```bash
sudo blkid /dev/<NEW_PARTITION>
```
- Add UUID entry to `/etc/fstab`:
```text
UUID=<UUID> /mnt/vmdata ext4 defaults,nofail 0 2
```
- Test:
```bash
sudo mount -a
df -h /mnt/vmdata
```

## Step 10: Keep old files as rollback buffer first
- Do not immediately delete `/tmp/libvirt-images/*`.
- Keep for one verification window (for example 24-48h).
- After stable operation, remove old copies manually.

## Validation Checklist
- `virsh list --all` shows expected VM states.
- `virsh domblklist <vm>` points to `/mnt/vmdata/libvirt-images`.
- SSH to VM works.
- App health check works:
```bash
curl -sS http://127.0.0.1:4103/api/health
```

## Notes
- This runbook intentionally avoids host reboot.
- If SSD partitioning requires OS-level disk table refresh in your environment, schedule a short maintenance window.
