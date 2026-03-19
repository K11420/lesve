#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Provision one tenant VM for lenovo-server on KVM/libvirt.

Usage:
  provision-lenovo-vm.sh --tenant-id <id> --ssh-key-file <pubkey> [options]

Required:
  --tenant-id <id>         Tenant/container id (e.g. server-abc123)
  --ssh-key-file <path>    Public key for admin login

Options:
  --name <vm-name>         VM name (default: lenovo-<tenant-id>)
  --admin-user <user>      Admin username in VM (default: admin)
  --memory-mb <mb>         RAM in MB (default: 2048)
  --vcpus <count>          vCPU count (default: 2)
  --disk-gb <gb>           Disk size in GB (default: 20)
  --bridge <bridge>        Linux bridge name (default: br0)
  --pool-dir <dir>         Disk storage dir (default: /var/lib/libvirt/images)
  --image-url <url>        Cloud image URL
  --help                   Show help
USAGE
}

TENANT_ID=""
VM_NAME=""
ADMIN_USER="admin"
MEMORY_MB=2048
VCPUS=2
DISK_GB=20
BRIDGE="br0"
POOL_DIR="/var/lib/libvirt/images"
SSH_KEY_FILE=""
IMAGE_URL="https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tenant-id)
      TENANT_ID="$2"
      shift 2
      ;;
    --name)
      VM_NAME="$2"
      shift 2
      ;;
    --admin-user)
      ADMIN_USER="$2"
      shift 2
      ;;
    --memory-mb)
      MEMORY_MB="$2"
      shift 2
      ;;
    --vcpus)
      VCPUS="$2"
      shift 2
      ;;
    --disk-gb)
      DISK_GB="$2"
      shift 2
      ;;
    --bridge)
      BRIDGE="$2"
      shift 2
      ;;
    --pool-dir)
      POOL_DIR="$2"
      shift 2
      ;;
    --ssh-key-file)
      SSH_KEY_FILE="$2"
      shift 2
      ;;
    --image-url)
      IMAGE_URL="$2"
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

if [[ -z "$TENANT_ID" || -z "$SSH_KEY_FILE" ]]; then
  usage >&2
  exit 1
fi

if [[ -z "$VM_NAME" ]]; then
  VM_NAME="lenovo-${TENANT_ID}"
fi

if [[ ! -f "$SSH_KEY_FILE" ]]; then
  echo "SSH public key not found: $SSH_KEY_FILE" >&2
  exit 1
fi

for cmd in curl qemu-img virt-install virsh sed; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Required command not found: $cmd" >&2
    exit 1
  fi
done

mkdir -p "$POOL_DIR"
BASE_IMAGE="$POOL_DIR/base-noble-cloudimg-amd64.img"
VM_DISK="$POOL_DIR/${VM_NAME}.qcow2"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

if [[ -e "$VM_DISK" ]]; then
  echo "VM disk already exists: $VM_DISK" >&2
  exit 1
fi

if virsh dominfo "$VM_NAME" >/dev/null 2>&1; then
  echo "VM already exists in libvirt: $VM_NAME" >&2
  exit 1
fi

TEMPLATE_DIR="$(cd "$(dirname "$0")" && pwd)/../vps/templates"
USER_DATA_TEMPLATE="$TEMPLATE_DIR/cloud-init-user-data.tpl"
if [[ ! -f "$USER_DATA_TEMPLATE" ]]; then
  echo "Template not found: $USER_DATA_TEMPLATE" >&2
  exit 1
fi

echo "[1/5] Preparing base image..."
if [[ ! -f "$BASE_IMAGE" ]]; then
  curl -fL "$IMAGE_URL" -o "$BASE_IMAGE"
fi

echo "[2/5] Creating vm disk..."
qemu-img create -f qcow2 -F qcow2 -b "$BASE_IMAGE" "$VM_DISK" "${DISK_GB}G"

echo "[3/5] Rendering cloud-init..."
USER_DATA="$WORK_DIR/user-data"
META_DATA="$WORK_DIR/meta-data"
SSH_PUBLIC_KEY="$(tr -d '\n' < "$SSH_KEY_FILE")"

sed \
  -e "s|__HOSTNAME__|$VM_NAME|g" \
  -e "s|__ADMIN_USER__|$ADMIN_USER|g" \
  -e "s|__SSH_PUBLIC_KEY__|$SSH_PUBLIC_KEY|g" \
  -e "s|__TENANT_ID__|$TENANT_ID|g" \
  "$USER_DATA_TEMPLATE" > "$USER_DATA"

cat > "$META_DATA" <<META
instance-id: ${VM_NAME}-$(date +%s)
local-hostname: $VM_NAME
META

echo "[4/5] Defining VM..."
virt-install \
  --name "$VM_NAME" \
  --memory "$MEMORY_MB" \
  --vcpus "$VCPUS" \
  --disk "path=$VM_DISK,format=qcow2,bus=virtio" \
  --cloud-init "user-data=$USER_DATA,meta-data=$META_DATA,clouduser-ssh-key=$SSH_KEY_FILE,root-ssh-key=$SSH_KEY_FILE,disable=on" \
  --os-variant ubuntu24.04 \
  --network "bridge=$BRIDGE,model=virtio" \
  --graphics none \
  --console pty,target_type=serial \
  --import \
  --noautoconsole

echo "[5/5] VM created"
virsh dominfo "$VM_NAME"

echo
echo "Tenant VM ready: $VM_NAME"
echo "Check IP: virsh domifaddr $VM_NAME"
echo "After first boot, you can open web terminal tunnel in VM:"
echo "  sudo TTYD_USER=$ADMIN_USER TTYD_PASS='<password>' /usr/local/bin/start-ttyd-trycloudflare.sh"
