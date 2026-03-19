#!/bin/bash
set -e

echo "🔐 Starting SSH Gateway for LenovoServer..."

# Create docker group with same GID as host's docker socket
if [ -S /var/run/docker.sock ]; then
    DOCKER_GID=$(stat -c '%g' /var/run/docker.sock)
    if ! getent group docker > /dev/null; then
        groupadd -g "$DOCKER_GID" docker
    else
        groupmod -g "$DOCKER_GID" docker 2>/dev/null || true
    fi
    echo "Docker group configured with GID: $DOCKER_GID"
fi

# Create libvirt group with same GID as host's libvirt socket
if [ -S /var/run/libvirt/libvirt-sock ]; then
    LIBVIRT_GID=$(stat -c '%g' /var/run/libvirt/libvirt-sock)
    if ! getent group libvirt > /dev/null; then
        groupadd -g "$LIBVIRT_GID" libvirt
    else
        groupmod -g "$LIBVIRT_GID" libvirt 2>/dev/null || true
    fi
    echo "libvirt group configured with GID: $LIBVIRT_GID"
fi

# Generate SSH host keys if not exist
if [ ! -f /etc/ssh/ssh_host_rsa_key ]; then
    echo "Generating SSH host keys..."
    ssh-keygen -t rsa -b 4096 -f /etc/ssh/ssh_host_rsa_key -N ""
    ssh-keygen -t ed25519 -f /etc/ssh/ssh_host_ed25519_key -N ""
fi

# Initial user sync
/sync_users.sh

# Start user sync in background (every 30 seconds)
while true; do
    sleep 30
    /sync_users.sh
done &

echo "✅ SSH Gateway ready on port 22"
echo "   Users will be synced every 30 seconds"

# Start SSH daemon
exec /usr/sbin/sshd -D -e
