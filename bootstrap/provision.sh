#!/bin/bash
set -e

# Kubernetes API Shim - VPS Provisioning Script
#
# Run this ONCE, as root, on a fresh VPS. It no longer builds anything on
# the VPS: the shim ships as a prebuilt container image
# (ghcr.io/brawer/kube-shim), run rootless under podman via a systemd
# --user quadlet service, so this script's job is:
#   1. Install podman
#   2. Create a dedicated, unprivileged system user to run it as
#   3. Generate a self-signed TLS cert and the first API bearer token
#   4. Print the remaining manual steps (deploying deploy/kube-shim.container
#      and starting the service is left to you -- see "Next steps" below)
#
# Tested against Fedora/RHEL-style rootless podman + quadlet mechanics
# directly (systemd --user, subuid/subgid allocation, linger); assumes a
# reasonably current Ubuntu LTS (24.04+) whose packaged podman has quadlet
# support built in (podman >= 4.4). If your distribution's podman predates
# that, quadlet won't be available and this script needs adjusting.

echo "=== Kube-Shim VPS Provisioning ==="

SERVICE_USER="kube-shim"

# Update system, install podman + the few tools we still need directly
# (openssl for cert/token generation; curl for troubleshooting;
# systemd-container for `machinectl shell`, used below and in the "Next
# steps" instructions to properly enter the service user's systemd --user
# session -- verified hands-on that it is NOT installed by default on a
# stock Ubuntu 24.04 image, even though it ships `systemctl --user` itself).
echo "Updating system packages..."
apt-get update
apt-get upgrade -y
apt-get install -y curl openssl podman systemd-container

# Rootless podman's rootlessport *can* publish a host port <1024 (needed
# for :443/:80, Phase 4's ACME listener) with no special capability, but
# only once this sysctl is lowered -- verified hands-on against a fresh
# VPS; without it, podman fails with "cannot expose privileged port 443
# ... bind: permission denied". This governs the HOST-side bind only; the
# container's own process still can't bind <1024 inside its own network
# namespace, which is why deploy/kube-shim.container maps host 443/80 to
# unprivileged container-internal ports rather than the same port number.
echo "Allowing rootless podman to publish privileged ports (443/80)..."
echo 'net.ipv4.ip_unprivileged_port_start=443' > /etc/sysctl.d/99-podman-unprivileged-ports.conf
sysctl --system >/dev/null

# Create a dedicated, unprivileged user to run the container as. Rootless
# podman's isolation only means something if the podman process itself
# isn't root -- running it as the login/root account would defeat the
# point of choosing rootless in the first place.
if ! id "$SERVICE_USER" >/dev/null 2>&1; then
    echo "Creating dedicated user '$SERVICE_USER'..."
    useradd --system --create-home --shell /bin/bash "$SERVICE_USER"
else
    echo "User '$SERVICE_USER' already exists, reusing it."
fi

# Rootless podman needs a subuid/subgid range to build user namespaces
# from. A freshly created user does NOT reliably get one automatically
# (verified: it does not on a stock system) -- without this, podman falls
# back to a degraded single-mapping mode and most real images fail to
# unpack ("insufficient UIDs or GIDs available in user namespace").
if ! grep -q "^${SERVICE_USER}:" /etc/subuid 2>/dev/null; then
    echo "Allocating a subuid/subgid range for '$SERVICE_USER'..."
    usermod --add-subuids 200000-265535 --add-subgids 200000-265535 "$SERVICE_USER"
fi

# Rootless podman's systemd --user instance is normally torn down when the
# user's last session ends -- which, on a server, is immediately after
# provisioning finishes. `enable-linger` keeps it running persistently
# (and starts it at boot) with nobody logged in, which is what a
# --user quadlet service needs to survive as a real daemon.
echo "Enabling systemd lingering for '$SERVICE_USER'..."
loginctl enable-linger "$SERVICE_USER"

USER_HOME=$(getent passwd "$SERVICE_USER" | cut -d: -f6)
DATA_DIR="$USER_HOME/kube-shim-data"
QUADLET_DIR="$USER_HOME/.config/containers/systemd"

echo "Creating data directory at $DATA_DIR..."
# podman does NOT auto-create a bind mount's host-side source directory
# (verified) -- without this, the quadlet service fails at startup with
# "no such file or directory" on the volume mount.
mkdir -p "$DATA_DIR" "$QUADLET_DIR"

# Generate self-signed TLS certificates
if [ ! -f "$DATA_DIR/cert.pem" ]; then
    echo "Generating self-signed TLS certificates..."
    openssl req -x509 -newkey rsa:4096 -keyout "$DATA_DIR/key.pem" -out "$DATA_DIR/cert.pem" \
        -days 365 -nodes -subj "/CN=localhost"
    echo "✓ Certificates generated at $DATA_DIR/{cert,key}.pem"
else
    echo "TLS certificates already exist at $DATA_DIR, leaving them alone."
fi

# Generate the first API bearer token and a starter config.toml. The token
# is printed once here -- there's no way to retrieve it again later, since
# the shim only ever stores whatever ends up in config.toml's
# [[server.api_tokens]] yourself.
if [ ! -f "$DATA_DIR/config.toml" ]; then
    echo "Generating initial API bearer token and starter config.toml..."
    API_TOKEN="$(openssl rand -base64 32)"
    cat > "$DATA_DIR/config.toml" << EOF
[server]
host = "0.0.0.0"
# Unprivileged port: deploy/kube-shim.container maps the real, public
# 443 to this one -- see that file's own comments (Phase 4).
port = 8443
tls_cert_path = "/data/cert.pem"
tls_key_path = "/data/key.pem"

# hostname is left unset here on purpose: ACME (Phase 4) engages the
# moment it's set, and a cold-start certificate failure makes the shim
# refuse to start -- which would happen immediately if DNS for this
# host isn't already pointing here yet. Once it is (see docs/RELEASING.md
# / your DNS provider), uncomment and set it, e.g.:
#   hostname = "kube-shim.brawer.ch"
#   acme_directory = "staging"  # flip to "production" once verified
#   acme_contact_email = "you@example.com"
#
# Set regardless of whether ACME is enabled yet, so the ACME account
# key/certs land on the persistent bind mount (not the container's own
# ephemeral filesystem, which is what the default relative path would
# resolve to) as soon as hostname above is uncommented.
acme_cache_dir = "/data/acme-cache"

[[server.api_tokens]]
token = "${API_TOKEN}"

[database]
path = "/data/db.sqlite"

[upcloud]
# REPLACE with your real UpCloud API token before starting the service.
token = "REPLACE_ME"
dry_run = true

[reconciliation]
interval_secs = 10
EOF
    echo "✓ Config written to $DATA_DIR/config.toml"
    echo ""
    echo "  API bearer token (copy into your kubectl/Terraform credentials -- it"
    echo "  will not be shown again): ${API_TOKEN}"
    echo ""
    echo "  (rotate it later by adding a second [[server.api_tokens]] entry with"
    echo "  a new token, migrating clients, then removing this one)"
else
    echo "config.toml already exists at $DATA_DIR, leaving it alone."
fi

chown -R "${SERVICE_USER}:${SERVICE_USER}" "$USER_HOME/kube-shim-data" "$USER_HOME/.config"

# The container runs as non-root UID 1000 (Phase 3), which under rootless
# podman's user namespace does NOT map back to $SERVICE_USER's own host
# UID -- it maps to a *different* host UID somewhere in the subuid range
# allocated above. A bind-mounted directory merely chown'd to
# $SERVICE_USER (as just done) is therefore NOT writable by the container
# process; verified hands-on, it fails at startup with "unable to open
# database file". `podman unshare` runs inside the same user namespace
# podman itself will use, so `chown 1000:1000` there resolves to the
# correct (mapped) host UID automatically, without needing to compute the
# subuid arithmetic by hand. Must run after every file above already
# exists (it does, at this point in the script).
echo "Fixing data directory ownership for the container's own UID 1000..."
SERVICE_UID=$(id -u "$SERVICE_USER")
sudo -u "$SERVICE_USER" -H env XDG_RUNTIME_DIR="/run/user/${SERVICE_UID}" \
    podman unshare chown -R 1000:1000 "$DATA_DIR"

echo ""
echo "=== Provisioning Complete ==="
echo ""
echo "Next steps:"
echo "1. Edit $DATA_DIR/config.toml and set upcloud.token to your real"
echo "   UpCloud API token (currently REPLACE_ME). If DNS for this"
echo "   host is already set up, also uncomment and set hostname (and"
echo "   review acme_directory/acme_contact_email) to enable ACME -- see"
echo "   the comments already in that file."
echo "2. Copy deploy/kube-shim.container to $QUADLET_DIR/kube-shim.container."
echo "   As checked in, it tracks :latest and auto-updates on every release --"
echo "   see the comments in that file for how to pin an explicit version"
echo "   and update manually instead."
echo "3. Start the service as $SERVICE_USER (this properly initializes its"
echo "   systemd --user session, including \$XDG_RUNTIME_DIR -- a plain"
echo "   'sudo -u $SERVICE_USER systemctl --user ...' does NOT do this):"
echo "     sudo machinectl shell ${SERVICE_USER}@ /bin/bash"
echo "     systemctl --user daemon-reload"
echo "     systemctl --user start kube-shim.service"
echo "     systemctl --user status kube-shim.service"
echo "     journalctl --user -u kube-shim.service -f"
echo "4. Enable auto-updates (only needed if kube-shim.container still has"
echo "   its checked-in AutoUpdate=registry label -- skip this if you pinned"
echo "   an explicit version instead), from the same shell:"
echo "     systemctl --user enable --now podman-auto-update.timer"
echo ""
echo "To update manually (whether or not auto-update is enabled):"
echo "     podman pull ghcr.io/brawer/kube-shim:vX.Y.Z-or-latest"
echo "     systemctl --user restart kube-shim.service"
