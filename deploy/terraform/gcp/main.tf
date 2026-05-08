# Created by: Mustafa Can Caliskan
# Date: 2026-05-06
#
# pdgraph-bench cluster on GCP — 5 GCE VMs (orchestrator + 3 sut + 1 reserve)
# in a private VPC with internal DNS so peer hostnames resolve out of the box.
#
# Usage:
#   cd deploy/terraform/gcp
#   terraform init
#   terraform apply -var project_id=<PROJECT_ID>
#
# Output:
#   `terraform output -raw inventory_yaml` prints a ready-to-use
#   configs/inventory/cluster.yaml.
#
# Tear-down:
#   terraform destroy -var project_id=<PROJECT_ID>

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.40"
    }
  }
}

variable "project_id" {
  type        = string
  description = "GCP project ID (e.g. project-579b6a98-6c56-414f-a1b)"
}

variable "region" {
  type        = string
  default     = "europe-west1"
  description = "GCP region. europe-west1 (Belgium) is closest for ITU; us-central1 is cheapest."
}

variable "zone" {
  type        = string
  default     = "europe-west1-b"
}

variable "machine_type" {
  type        = string
  default     = "n2-standard-4"
  description = "4 vCPU + 16 GB RAM. e2-standard-4 cuts cost ~30% with slower CPU."
}

variable "disk_size_gb" {
  type        = number
  default     = 200
  description = "Boot+data disk size per VM. SF=1 datagen + SUT data fits in 200 GB."
}

variable "ssh_user" {
  type        = string
  default     = "mcc"
  description = "Linux username on every VM; must match `ssh_user` in the inventory."
}

variable "ssh_pubkey" {
  type        = string
  description = "Operator's SSH public key (cat ~/.ssh/id_ed25519.pub)."
}

variable "image_family" {
  type        = string
  default     = "ubuntu-2404-lts-amd64"
  description = "Ubuntu 24.04 LTS — apt docker installs cleanly via cloud-init."
}

variable "image_project" {
  type        = string
  default     = "ubuntu-os-cloud"
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# --------------------------------------------------------------- network
resource "google_compute_network" "vpc" {
  name                    = "pdgraph-bench-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "pdgraph-bench-subnet"
  network       = google_compute_network.vpc.id
  ip_cidr_range = "10.10.0.0/24"
  region        = var.region
}

# Open SSH from anywhere (the SSH key is the gate). Tighten to your egress IP
# in production with `source_ranges = ["<YOUR_IP>/32"]`.
resource "google_compute_firewall" "ssh" {
  name      = "pdgraph-bench-allow-ssh"
  network   = google_compute_network.vpc.name
  direction = "INGRESS"
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["pdgraph-bench"]
}

# Intra-cluster: every TCP/UDP port between cluster members. The benchmark
# protocols (NebulaGraph 9559/9669/9779, ArangoDB 5001/6001/8529, Dgraph
# 5080/7080/8080/9080, Cassandra 7000/9042, JanusGraph 8182, HugeGraph 8080)
# are all on this list — a permissive intra-VPC rule beats per-port juggling.
resource "google_compute_firewall" "internal" {
  name      = "pdgraph-bench-allow-internal"
  network   = google_compute_network.vpc.name
  direction = "INGRESS"
  allow { protocol = "tcp" }
  allow { protocol = "udp" }
  allow { protocol = "icmp" }
  source_tags = ["pdgraph-bench"]
  target_tags = ["pdgraph-bench"]
}

# --------------------------------------------------------------- VMs
locals {
  nodes = {
    node1 = { roles = ["client", "orchestrator"] }
    node2 = { roles = ["sut"] }
    node3 = { roles = ["sut"] }
    node4 = { roles = ["sut"] }
    node5 = { roles = ["reserve"] }
  }
}

resource "google_compute_address" "node" {
  for_each = local.nodes
  name     = "pdgraph-bench-${each.key}-internal"
  subnetwork = google_compute_subnetwork.subnet.id
  address_type = "INTERNAL"
  region   = var.region
}

resource "google_compute_address" "external" {
  for_each = local.nodes
  name     = "pdgraph-bench-${each.key}-external"
  region   = var.region
}

resource "google_compute_instance" "node" {
  for_each     = local.nodes
  name         = each.key
  machine_type = var.machine_type
  zone         = var.zone
  tags         = ["pdgraph-bench"]

  boot_disk {
    initialize_params {
      image = "${var.image_project}/${var.image_family}"
      size  = var.disk_size_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.subnet.id
    network_ip = google_compute_address.node[each.key].address
    access_config {
      nat_ip = google_compute_address.external[each.key].address
    }
  }

  metadata = {
    ssh-keys       = "${var.ssh_user}:${var.ssh_pubkey}"
    enable-oslogin = "FALSE"  # pure ssh-keys auth, no IAM round-trip
  }

  metadata_startup_script = templatefile(
    "${path.module}/../../cloud-init/install-docker.sh",
    {
      ssh_user   = var.ssh_user
      ssh_pubkey = var.ssh_pubkey
    }
  )
}

# --------------------------------------------------------------- outputs
output "node_internal_ips" {
  value = { for k, v in google_compute_instance.node : k => v.network_interface[0].network_ip }
}

output "node_external_ips" {
  value = { for k, v in google_compute_instance.node : k => v.network_interface[0].access_config[0].nat_ip }
}

output "orchestrator_ssh" {
  value       = "ssh ${var.ssh_user}@${google_compute_instance.node["node1"].network_interface[0].access_config[0].nat_ip}"
  description = "SSH command to reach the orchestrator node."
}

# Ready-to-use inventory: GCE internal DNS resolves `<node>.<zone>.c.<project>.internal`,
# and within the same VPC the bare hostname `<node>` also works thanks to the
# default `(zone-)c-` suffix being optional. We use the bare logical name.
output "inventory_yaml" {
  value = <<-YAML
    # Generated by terraform — do not edit by hand
    cluster_name: pdgraph-bench-gcp

    nodes:
    %{for name, cfg in local.nodes ~}
      ${name}:
        hostname: ${name}
        ip: ${google_compute_address.node[name].address}
        ssh_user: ${var.ssh_user}
        roles: [${join(", ", cfg.roles)}]
    %{endfor~}
    YAML
  description = "Pipe to configs/inventory/cluster.yaml: terraform output -raw inventory_yaml > ../../../configs/inventory/cluster.yaml"
}
