#!/usr/bin/env bash
# Created by: Mustafa Can Caliskan
# Date: 2026-05-06
#
# Tear the GCP cluster down: `terraform destroy` removes every VM + the VPC.
#
# Usage:
#   PROJECT_ID=<your-gcp-project> bash scripts/gcp-teardown.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="$REPO_ROOT/deploy/terraform/gcp"

PROJECT_ID="${PROJECT_ID:?PROJECT_ID must be set}"
REGION="${REGION:-europe-west1}"
ZONE="${ZONE:-europe-west1-b}"
SSH_USER="${SSH_USER:-$USER}"
SSH_PUBKEY="${SSH_PUBKEY:-$(cat "${SSH_PUBKEY_FILE:-$HOME/.ssh/id_ed25519.pub}")}"

terraform -chdir="$TF_DIR" destroy -auto-approve \
    -var "project_id=$PROJECT_ID" \
    -var "region=$REGION" \
    -var "zone=$ZONE" \
    -var "ssh_user=$SSH_USER" \
    -var "ssh_pubkey=$SSH_PUBKEY"
echo "✓ cluster destroyed"
