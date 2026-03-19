#!/bin/bash
# Create system user for SSH access

USERNAME="$1"
PASSWORD="$2"

if [ -z "$USERNAME" ] || [ -z "$PASSWORD" ]; then
    echo "Usage: create_user.sh <username> <password>"
    exit 1
fi

# Check if user exists
if id "$USERNAME" &>/dev/null; then
    # Update password
    echo "$USERNAME:$PASSWORD" | chpasswd
else
    # Create user
    useradd -m -s /bin/bash "$USERNAME"
    echo "$USERNAME:$PASSWORD" | chpasswd
fi

# Ensure gateway users are not in generic sudo groups.
if getent group sudo >/dev/null 2>&1; then
    gpasswd -d "$USERNAME" sudo >/dev/null 2>&1 || true
fi
if getent group wheel >/dev/null 2>&1; then
    gpasswd -d "$USERNAME" wheel >/dev/null 2>&1 || true
fi

# Allow only constrained backend handoff commands from forced login shell.
if command -v sudo >/dev/null 2>&1 && [ -d /etc/sudoers.d ]; then
    SUDOERS_FILE="/etc/sudoers.d/90-shironeko-${USERNAME}"
    printf '%s ALL=(root) NOPASSWD: /usr/local/sbin/shironeko-backendctl *\n' "$USERNAME" > "$SUDOERS_FILE"
    chmod 0440 "$SUDOERS_FILE"
fi

echo "User $USERNAME ready"
