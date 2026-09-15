#!/bin/bash
set -e

# Kubernetes API Shim - VPS Provisioning Script
# Run this on a fresh VPS to set up kube-shim

echo "=== Kube-Shim VPS Provisioning ==="

# Update system
echo "Updating system packages..."
apt-get update
apt-get upgrade -y
apt-get install -y curl wget git build-essential pkg-config libssl-dev

# Install Rust
echo "Installing Rust..."
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# Create application directory
echo "Creating application directory..."
mkdir -p /opt/kube-shim
cd /opt/kube-shim

# Create certificate directory
mkdir -p certs

# Generate self-signed certificates
echo "Generating self-signed TLS certificates..."
openssl req -x509 -newkey rsa:4096 -keyout certs/key.pem -out certs/cert.pem \
    -days 365 -nodes -subj "/CN=localhost"

echo "✓ Certificates generated at certs/cert.pem and certs/key.pem"

# Create systemd service file
echo "Setting up systemd service..."
cat > /etc/systemd/system/kube-shim.service << 'EOF'
[Unit]
Description=Kubernetes API Shim
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/kube-shim
ExecStart=/opt/kube-shim/kube-shim -c /opt/kube-shim/config.toml
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload

# Create database directory
mkdir -p /opt/kube-shim/data

echo ""
echo "=== Provisioning Complete ==="
echo ""
echo "Next steps:"
echo "1. Copy your compiled kube-shim binary to /opt/kube-shim/"
echo "2. Copy your config.toml to /opt/kube-shim/"
echo "3. Run: systemctl start kube-shim"
echo "4. Check status: systemctl status kube-shim"
echo "5. View logs: journalctl -u kube-shim -f"
