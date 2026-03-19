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

echo "User $USERNAME ready"
