#!/usr/bin/env bash
# Created by: Mustafa Can Caliskan
# Date: 2026-05-06
#
# One-shot GCP deploy: terraform apply + write the inventory + wait for sshd.
#
# Usage:
#   PROJECT_ID=<your-gcp-project> bash scripts/gcp-deploy.sh
#
# Optional env:
#   REGION=europe-west1   ZONE=europe-west1-b   MACHINE_TYPE=n2-standard-4
#   DISK_GB=200           SSH_USER=$USER        SSH_PUBKEY=~/.ssh/id_ed25519.pub
#
# After this script returns, the cluster is up. Run:
#   bash scripts/setup-cluster.sh nebulagraph snb_iv2 1

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="$REPO_ROOT/deploy/terraform/gcp"

PROJECT_ID="${PROJECT_ID:?PROJECT_ID must be set (e.g. PROJECT_ID=project-xxxx bash $0)}"
REGION="${REGION:-europe-west1}"
ZONE="${ZONE:-europe-west1-b}"
MACHINE_TYPE="${MACHINE_TYPE:-n2-standard-4}"
DISK_GB="${DISK_GB:-200}"
SSH_USER="${SSH_USER:-$USER}"
SSH_PUBKEY_FILE="${SSH_PUBKEY_FILE:-$HOME/.ssh/id_ed25519.pub}"
SSH_PUBKEY="${SSH_PUBKEY:-$(cat "$SSH_PUBKEY_FILE")}"

note() { printf '\n\033[1;34m▸ %s\033[0m\n' "$*"; }
ok()   { printf '\033[1;32m✓\033[0m %s\n' "$*"; }

note "terraform init"
terraform -chdir="$TF_DIR" init -upgrade

note "terraform apply (project=$PROJECT_ID, region=$REGION, machine_type=$MACHINE_TYPE)"
terraform -chdir="$TF_DIR" apply -auto-approve \
    -var "project_id=$PROJECT_ID" \
    -var "region=$REGION" \
    -var "zone=$ZONE" \
    -var "machine_type=$MACHINE_TYPE" \
    -var "disk_size_gb=$DISK_GB" \
    -var "ssh_user=$SSH_USER" \
    -var "ssh_pubkey=$SSH_PUBKEY"

note "writing inventory"
terraform -chdir="$TF_DIR" output -raw inventory_yaml > "$REPO_ROOT/configs/inventory/cluster.yaml"
ok "inventory written → configs/inventory/cluster.yaml"
cat "$REPO_ROOT/configs/inventory/cluster.yaml"

note "waiting for sshd on every node"
external_ips=$(terraform -chdir="$TF_DIR" output -json node_external_ips | python3 -c 'import sys,json; [print(v) for v in json.load(sys.stdin).values()]')
for ip in $external_ips; do
    until ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "$SSH_USER@$ip" 'docker --version >/dev/null && echo OK' >/dev/null 2>&1; do
        printf '.'
        sleep 5
    done
    ok "$ip ready"
done

note "next: bash scripts/setup-cluster.sh nebulagraph snb_iv2 1"
