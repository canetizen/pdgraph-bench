#!/usr/bin/env bash
# Created by: Mustafa Can Caliskan
# Date: 2026-05-06
#
# Cloud-init startup script run on every GCE VM.
#  - Install Docker engine (apt; no snap confinement weirdness)
#  - Authorise the operator's SSH key
#  - Make every cluster member's bare hostname resolvable on every other peer
#    via /etc/hosts (terraform also uses GCE internal DNS but appending the
#    static hosts file lets the deployers' `network_mode: host` containers
#    address peers without DNS round-trips)
#
# Templated by terraform: ${ssh_user} and ${ssh_pubkey} are substituted at
# `terraform apply` time.

set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive

# -- Docker engine ----------------------------------------------------------
apt-get update -y
apt-get install -y ca-certificates curl gnupg lsb-release
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
. /etc/os-release
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $VERSION_CODENAME stable" \
    > /etc/apt/sources.list.d/docker.list
apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
systemctl enable --now docker

# -- Operator user ----------------------------------------------------------
id "${ssh_user}" >/dev/null 2>&1 || useradd -m -s /bin/bash "${ssh_user}"
usermod -aG docker "${ssh_user}"
mkdir -p /home/${ssh_user}/.ssh
chmod 700 /home/${ssh_user}/.ssh
echo "${ssh_pubkey}" > /home/${ssh_user}/.ssh/authorized_keys
chmod 600 /home/${ssh_user}/.ssh/authorized_keys
chown -R "${ssh_user}:${ssh_user}" /home/${ssh_user}/.ssh

# Passwordless sudo for the operator — the deployers use `sudo` in some places
# (image preload, ufw rules) and we don't want to ship a password.
echo "${ssh_user} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${ssh_user}
chmod 440 /etc/sudoers.d/${ssh_user}

echo "[cloud-init] done"
