# ShironekoServer Production Dockerfile
FROM node:20-slim

# Docker CLI をインストール（Docker-in-Docker用）
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    libvirt-clients \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian bookworm stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y docker-ce-cli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 作業ディレクトリ
WORKDIR /app

# package.json をコピー
COPY backend/package*.json ./backend/

# 依存関係インストール（コンテナ内のNodeバージョンに合わせてビルド）
RUN cd backend && npm install --omit=dev

# アプリケーションコードをコピー（node_modules を除く）
COPY backend/server.js ./backend/
COPY backend/passkey-login.js ./backend/

# フロントエンドをコピー
COPY frontend/ ./frontend/

# データディレクトリ作成
RUN mkdir -p /app/data/containers /app/data/backups

# ポート
EXPOSE 4103

# 環境変数
ENV NODE_ENV=production
ENV PORT=4103
ENV DATA_DIR=/app/data
ENV CONTAINERS_DIR=/app/data/containers
ENV BACKUPS_DIR=/app/data/backups
ENV DB_PATH=/app/data/lenovo.db

# ヘルスチェック
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:4103/ || exit 1

# 起動
WORKDIR /app/backend
CMD ["node", "server.js"]
