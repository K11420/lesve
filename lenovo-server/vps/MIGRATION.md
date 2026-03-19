# Lenovo Server: Docker から VPS (KVM VM) への移行

このドキュメントは `lenovo-server` のテナント実行基盤を Docker コンテナから KVM/libvirt VM へ段階移行する手順です。

Docker は共有カーネルなので、`systemd` 完全動作やカーネルモジュール操作など、VPS相当の自由度に限界があります。VM化でこの制約を解消します。

## 0. 前提

移行先ホストに以下が必要です。

```bash
sudo apt update
sudo apt install -y qemu-kvm libvirt-daemon-system virtinst cloud-image-utils bridge-utils sqlite3 rsync curl
sudo systemctl enable --now libvirtd
```

KVM利用可否:

```bash
egrep -c '(vmx|svm)' /proc/cpuinfo
```

`0` より大きい必要があります。

## 1. 先にバックアップ（必須）

`lenovo-server` ディレクトリで実行:

```bash
bash scripts/backup-lenovo-before-vps.sh
```

生成物:

- `backups/lenovo-pre-vps-YYYYmmdd-HHMMSS/`
- `meta/lenovo.db`, `meta/containers-dir.tar.gz`
- `volumes/lenovo-data.tar.gz` など（Docker利用時）
- `SHA256SUMS`

## 2. テナント情報をエクスポート

```bash
bash scripts/export-lenovo-tenants.sh
```

生成物:

- `backups/lenovo-tenant-export-YYYYmmdd-HHMMSS/meta/tenant-manifest.tsv`
- `backups/.../tenants/<tenant-id>/workspace.tar.gz`
- `backups/.../meta/provision-commands.sh`

## 3. VMを作成

### 3.1 手動で1台作る例

```bash
sudo bash scripts/provision-lenovo-vm.sh \
  --tenant-id server-xxxxxxxxxxxx \
  --ssh-key-file ~/.ssh/id_ed25519.pub \
  --bridge br0 \
  --memory-mb 2048 \
  --vcpus 2 \
  --disk-gb 25
```

### 3.2 一括作成

`export-lenovo-tenants.sh` が出力した `provision-commands.sh` を編集して、鍵パスを実値に変更して実行。

## 4. テナントデータをVMへ復元

テナントごとの `workspace.tar.gz` をVMにコピーして展開:

```bash
scp workspace.tar.gz admin@<vm-ip>:/tmp/
ssh admin@<vm-ip>
sudo mkdir -p /srv/tenant/workspace
sudo tar xzf /tmp/workspace.tar.gz -C /srv/tenant/workspace
```

## 5. カットオーバー手順（推奨）

1. 既存 Docker テナントをメンテナンス状態へ（書き込み停止）
2. 最終差分同期
3. DNS / リバースプロキシをVMへ切替
4. 24-48時間は旧Docker系を読み取り専用で保持

## 6. ロールバック

1. DNS を旧Dockerエンドポイントへ戻す
2. 必要ならバックアップから差分復旧
3. 障害原因を特定して再実施

## 補足

- この移行は「制御プレーン（lenovo-server本体）」と「テナント実行基盤」を分離する設計です。
- 移行初期は lenovo-server 本体は Docker 継続でも問題ありません。
- まずはテナント実行を VM 化し、要件に応じて本体も VM 化してください。
