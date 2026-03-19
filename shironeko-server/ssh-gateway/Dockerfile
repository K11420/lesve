# SSH Gateway Container for LenovoServer
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install required packages including Docker CLI
RUN apt-get update && apt-get install -y \
    openssh-server \
    libvirt-clients \
    sudo \
    curl \
    jq \
    ca-certificates \
    gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y docker-ce-cli \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/run/sshd

# SSH configuration for password and public key authentication
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin no/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PubkeyAuthentication yes/PubkeyAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/UsePAM yes/UsePAM yes/' /etc/ssh/sshd_config && \
    echo "" >> /etc/ssh/sshd_config && \
    echo "# LenovoServer SSH Gateway" >> /etc/ssh/sshd_config && \
    echo "PermitEmptyPasswords no" >> /etc/ssh/sshd_config && \
    echo "ChallengeResponseAuthentication no" >> /etc/ssh/sshd_config && \
    echo "AuthorizedKeysFile .ssh/authorized_keys" >> /etc/ssh/sshd_config

# Copy scripts
COPY entrypoint.sh /entrypoint.sh
COPY sync_users.sh /sync_users.sh
COPY connect.sh /usr/local/bin/shironeko-connect
COPY backendctl.sh /usr/local/sbin/shironeko-backendctl

RUN chmod +x /entrypoint.sh /sync_users.sh /usr/local/bin/shironeko-connect /usr/local/sbin/shironeko-backendctl

EXPOSE 22

ENTRYPOINT ["/entrypoint.sh"]
