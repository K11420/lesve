#!/bin/bash
# PAM authentication script - validates user/password against API

USERNAME="$PAM_USER"
PASSWORD="$(cat)"  # Read password from stdin

API_URL="${API_URL:-http://lenovo-server:4103}"

# Extract container ID from username
CONTAINER_ID=""
if [[ "$USERNAME" =~ ^(srv|server)-(.+)$ ]]; then
    CONTAINER_ID="${BASH_REMATCH[2]}"
else
    CONTAINER_ID="$USERNAME"
fi

# Call API to validate credentials
RESPONSE=$(curl -s -X POST "${API_URL}/api/ssh/auth" \
    -H "Content-Type: application/json" \
    -d "{\"container_id\": \"${CONTAINER_ID}\", \"password\": \"${PASSWORD}\"}" \
    2>/dev/null)

if [ $? -ne 0 ]; then
    exit 1
fi

# Check if authentication succeeded
SUCCESS=$(echo "$RESPONSE" | jq -r '.success // false')

if [ "$SUCCESS" = "true" ]; then
    exit 0
else
    exit 1
fi
