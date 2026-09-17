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
# (openssl for cert/token generation; curl for troubleshooting).
echo "Updating system packages..."
apt-get update
apt-get upgrade -y
apt-get install -y curl openssl podman

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
port = 6443
tls_cert_path = "/data/cert.pem"
tls_key_path = "/data/key.pem"

[[server.api_tokens]]
token = "${API_TOKEN}"

[database]
path = "/data/db.sqlite"

[hetzner]
# REPLACE with your real Hetzner Cloud API token before starting the service.
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

echo ""
echo "=== Provisioning Complete ==="
echo ""
echo "Next steps:"
echo "1. Edit $DATA_DIR/config.toml and set hetzner.token to your real"
echo "   Hetzner Cloud API token (currently REPLACE_ME)."
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
