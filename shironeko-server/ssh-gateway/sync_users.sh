#!/bin/bash
# Sync SSH users from LenovoServer API

API_URL="${API_URL:-http://shironeko-server:4103}"

# Fetch all containers with SSH credentials
CONTAINERS=$(curl -s "${API_URL}/api/internal/ssh-users" 2>/dev/null)

if [ -z "$CONTAINERS" ] || [ "$CONTAINERS" = "null" ]; then
    # API not ready yet, skip
    exit 0
fi

# Process each container
echo "$CONTAINERS" | jq -c '.[]' 2>/dev/null | while read -r container; do
    SSH_USER=$(echo "$container" | jq -r '.ssh_user')
    SSH_PASS=$(echo "$container" | jq -r '.ssh_password')
    BACKEND_TYPE=$(echo "$container" | jq -r '.backend_type // "docker"')
    BACKEND_TARGET=$(echo "$container" | jq -r '.backend_target // .docker_name')
    AUTHORIZED_KEYS=$(echo "$container" | jq -r '.authorized_keys // [] | .[]' 2>/dev/null)
    
    if [ -z "$SSH_USER" ] || [ "$SSH_USER" = "null" ]; then
        continue
    fi
    
    # Create or update user
    if ! id "$SSH_USER" &>/dev/null; then
        # Create user with shell that connects to container
        useradd -m -s /usr/local/bin/shironeko-connect "$SSH_USER" 2>/dev/null
        echo "Created SSH user: $SSH_USER"
    fi

    # Ensure user groups
    getent group docker >/dev/null && usermod -aG docker "$SSH_USER" 2>/dev/null || true
    getent group libvirt >/dev/null && usermod -aG libvirt "$SSH_USER" 2>/dev/null || true
    # SSH gateway users must not have generic sudo group access.
    if getent group sudo >/dev/null 2>&1; then
        gpasswd -d "$SSH_USER" sudo >/dev/null 2>&1 || true
    fi
    if getent group wheel >/dev/null 2>&1; then
        gpasswd -d "$SSH_USER" wheel >/dev/null 2>&1 || true
    fi

    # Allow only the constrained backend handoff wrapper via sudo.
    if command -v sudo >/dev/null 2>&1 && [ -d /etc/sudoers.d ]; then
        SUDOERS_FILE="/etc/sudoers.d/90-shironeko-${SSH_USER}"
        printf '%s ALL=(root) NOPASSWD: /usr/local/sbin/shironeko-backendctl *\n' "$SSH_USER" > "$SUDOERS_FILE" 2>/dev/null || true
        chmod 0440 "$SUDOERS_FILE" 2>/dev/null || true
    fi
    
    # Update password
    echo "$SSH_USER:$SSH_PASS" | chpasswd 2>/dev/null
    
    # Store backend settings for connect script
    echo "$BACKEND_TYPE" > "/home/$SSH_USER/.backend_type" 2>/dev/null
    echo "$BACKEND_TARGET" > "/home/$SSH_USER/.backend_target" 2>/dev/null
    chown "$SSH_USER:$SSH_USER" "/home/$SSH_USER/.backend_type" "/home/$SSH_USER/.backend_target" 2>/dev/null
    
    # Setup SSH public key authentication
    SSH_DIR="/home/$SSH_USER/.ssh"
    AUTHORIZED_KEYS_FILE="$SSH_DIR/authorized_keys"
    
    # Create .ssh directory if not exists
    mkdir -p "$SSH_DIR" 2>/dev/null
    chmod 700 "$SSH_DIR" 2>/dev/null
    chown "$SSH_USER:$SSH_USER" "$SSH_DIR" 2>/dev/null
    
    # Write authorized_keys file
    if [ -n "$AUTHORIZED_KEYS" ]; then
        echo "$AUTHORIZED_KEYS" > "$AUTHORIZED_KEYS_FILE"
        chmod 600 "$AUTHORIZED_KEYS_FILE"
        chown "$SSH_USER:$SSH_USER" "$AUTHORIZED_KEYS_FILE"
        echo "Updated authorized_keys for: $SSH_USER"
    else
        # No keys registered, remove authorized_keys file
        rm -f "$AUTHORIZED_KEYS_FILE" 2>/dev/null
    fi
done

# Remove users for deleted containers
for user_home in /home/srv-*; do
    if [ -d "$user_home" ]; then
        username=$(basename "$user_home")
        container_file="$user_home/.container_name"
        
        if [ -f "$container_file" ]; then
            container_name=$(cat "$container_file")
            # Check if container still exists in API response
            exists=$(echo "$CONTAINERS" | jq -r --arg u "$username" '.[] | select(.ssh_user == $u) | .ssh_user' 2>/dev/null)
            
            if [ -z "$exists" ]; then
                userdel -r "$username" 2>/dev/null
                echo "Removed SSH user: $username"
            fi
        fi
    fi
done
