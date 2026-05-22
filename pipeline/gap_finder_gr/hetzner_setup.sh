#!/usr/bin/env bash
# =============================================================================
# AFTS Food Safety Intelligence — VPS Setup Script
# Hetzner CCX13 (2 dedicated vCPU, 8 GB RAM, 80 GB NVMe, Falkenstein DC)
# Hosts: llama.cpp serving Qwen 2.5 7B Instruct Q4_K_M
# Access: Tailscale-only (no public exposure)
#
# Cost: €9.96/month (Hetzner) + €0 (Tailscale free tier)
#
# Run this ONCE on a fresh Ubuntu 24.04 Hetzner VPS:
#   1. Hetzner Console → Create Server → CCX13 → Ubuntu 24.04 → Falkenstein
#   2. SSH in as root
#   3. Paste this script (or: scp it, then: bash setup.sh)
#   4. Follow Tailscale login prompt
#   5. Done in ~15 minutes. Note the Tailscale IP it prints at the end.
# =============================================================================

set -euo pipefail

# ─── CONFIG ─────────────────────────────────────────────────────────────────
LLAMA_USER="afts"
LLAMA_HOME="/opt/llama"
MODEL_DIR="${LLAMA_HOME}/models"
MODEL_FILE="qwen2.5-7b-instruct-q4_k_m.gguf"
MODEL_URL="https://huggingface.co/Qwen/Qwen2.5-7B-Instruct-GGUF/resolve/main/qwen2.5-7b-instruct-q4_k_m.gguf"
MODEL_SHA256_EXPECTED=""  # Optional: pin SHA256 for supply-chain hardening
LLAMA_PORT=8080
LLAMA_CONTEXT=8192        # Context window — plenty for EFET pages
LLAMA_THREADS=2           # CCX13 has 2 dedicated vCPUs

# ─── PREFLIGHT ──────────────────────────────────────────────────────────────
echo "==> AFTS VPS Setup — $(date -Iseconds)"
echo "    Host: $(hostname)  Kernel: $(uname -r)"

if [ "$EUID" -ne 0 ]; then
    echo "ERROR: must run as root" >&2
    exit 1
fi

if ! grep -qi "ubuntu" /etc/os-release; then
    echo "WARNING: script targeted at Ubuntu; you have $(cat /etc/os-release | grep PRETTY_NAME)"
fi

# ─── 1. SYSTEM HARDENING + UPDATES ──────────────────────────────────────────
echo "==> [1/8] System updates + baseline hardening"
export DEBIAN_FRONTEND=noninteractive

apt-get update -y
apt-get upgrade -y
apt-get install -y --no-install-recommends \
    build-essential cmake git curl ca-certificates \
    libcurl4-openssl-dev libssl-dev pkg-config \
    ufw fail2ban unattended-upgrades \
    htop tmux jq

# Automatic security updates
dpkg-reconfigure -plow unattended-upgrades || true

# ─── 2. CREATE SERVICE USER ─────────────────────────────────────────────────
echo "==> [2/8] Service user '${LLAMA_USER}'"
if ! id -u "${LLAMA_USER}" >/dev/null 2>&1; then
    useradd --system --shell /usr/sbin/nologin --home "${LLAMA_HOME}" \
            --create-home "${LLAMA_USER}"
fi
mkdir -p "${MODEL_DIR}"
chown -R "${LLAMA_USER}:${LLAMA_USER}" "${LLAMA_HOME}"

# ─── 3. BUILD llama.cpp ─────────────────────────────────────────────────────
echo "==> [3/8] Build llama.cpp from source (latest stable)"
if [ ! -d "${LLAMA_HOME}/src/llama.cpp" ]; then
    sudo -u "${LLAMA_USER}" git clone --depth 1 \
        https://github.com/ggerganov/llama.cpp.git \
        "${LLAMA_HOME}/src/llama.cpp"
fi
cd "${LLAMA_HOME}/src/llama.cpp"
sudo -u "${LLAMA_USER}" cmake -B build -DGGML_NATIVE=ON -DLLAMA_CURL=ON
sudo -u "${LLAMA_USER}" cmake --build build --config Release -j 2
ln -sf "${LLAMA_HOME}/src/llama.cpp/build/bin/llama-server" /usr/local/bin/llama-server

# ─── 4. DOWNLOAD QWEN 2.5 7B INSTRUCT (Q4_K_M, ~4.7 GB) ─────────────────────
echo "==> [4/8] Download Qwen 2.5 7B Instruct Q4_K_M"
if [ ! -f "${MODEL_DIR}/${MODEL_FILE}" ]; then
    sudo -u "${LLAMA_USER}" curl --fail --location --retry 3 --retry-delay 5 \
        --output "${MODEL_DIR}/${MODEL_FILE}" "${MODEL_URL}"
fi

if [ -n "${MODEL_SHA256_EXPECTED}" ]; then
    echo "==> Verifying model SHA256..."
    actual_sha=$(sha256sum "${MODEL_DIR}/${MODEL_FILE}" | awk '{print $1}')
    if [ "${actual_sha}" != "${MODEL_SHA256_EXPECTED}" ]; then
        echo "ERROR: SHA256 mismatch! Expected ${MODEL_SHA256_EXPECTED}, got ${actual_sha}" >&2
        exit 1
    fi
    echo "    SHA256 OK: ${actual_sha}"
fi

# ─── 5. INSTALL TAILSCALE ───────────────────────────────────────────────────
echo "==> [5/8] Install Tailscale (mesh VPN, no public port exposure)"
if ! command -v tailscale >/dev/null 2>&1; then
    curl -fsSL https://tailscale.com/install.sh | sh
fi
systemctl enable --now tailscaled

# ─── 6. SYSTEMD SERVICE FOR llama-server ────────────────────────────────────
echo "==> [6/8] Systemd service: afts-llama.service"
cat > /etc/systemd/system/afts-llama.service <<EOF
[Unit]
Description=AFTS llama.cpp server (Qwen 2.5 7B Instruct)
After=network-online.target tailscaled.service
Wants=network-online.target

[Service]
Type=simple
User=${LLAMA_USER}
Group=${LLAMA_USER}
WorkingDirectory=${LLAMA_HOME}
ExecStart=/usr/local/bin/llama-server \\
    --model ${MODEL_DIR}/${MODEL_FILE} \\
    --host 0.0.0.0 \\
    --port ${LLAMA_PORT} \\
    --ctx-size ${LLAMA_CONTEXT} \\
    --threads ${LLAMA_THREADS} \\
    --threads-batch ${LLAMA_THREADS} \\
    --no-warmup \\
    --metrics
Restart=on-failure
RestartSec=10
LimitNOFILE=65536

# Hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${LLAMA_HOME}
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
RestrictRealtime=true
LockPersonality=true
MemoryDenyWriteExecute=false  # llama.cpp needs JIT for some kernels

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable afts-llama.service

# ─── 7. FIREWALL — block 8080 from public, allow only Tailscale ─────────────
echo "==> [7/8] UFW firewall — public access blocked except SSH"
ufw --force reset
ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp comment "SSH"
# llama-server on 8080 is NOT exposed to public — only reachable via Tailscale
# interface (tailscale0). UFW allow rule scoped to the tailnet interface:
ufw allow in on tailscale0 to any port ${LLAMA_PORT} proto tcp \
    comment "llama-server via Tailscale only"
ufw --force enable
ufw status verbose

# fail2ban for SSH brute-force protection
systemctl enable --now fail2ban

# ─── 8. START SERVICES + INSTRUCTIONS ───────────────────────────────────────
echo "==> [8/8] Start services"

# Tailscale up — interactive auth (one-time URL paste)
echo ""
echo "──────────────────────────────────────────────────────────────────────"
echo " NEXT: Authenticate Tailscale. A URL will appear below — open it in"
echo " your browser, sign in, approve this machine. Then this script resumes."
echo "──────────────────────────────────────────────────────────────────────"
echo ""
tailscale up --ssh --accept-routes --hostname=afts-llama-vps

# Start llama-server
systemctl start afts-llama.service
sleep 5
systemctl status afts-llama.service --no-pager || true

# ─── DONE ───────────────────────────────────────────────────────────────────
TS_IP=$(tailscale ip -4 | head -n1)
echo ""
echo "═════════════════════════════════════════════════════════════════════"
echo " AFTS VPS Setup Complete"
echo "═════════════════════════════════════════════════════════════════════"
echo ""
echo " Tailscale IP:    ${TS_IP}"
echo " Hostname:        afts-llama-vps (in your tailnet)"
echo " Endpoint:        http://${TS_IP}:${LLAMA_PORT}/v1   (OpenAI-compatible)"
echo " Model:           Qwen 2.5 7B Instruct Q4_K_M"
echo " Context:         ${LLAMA_CONTEXT} tokens"
echo ""
echo " Test from any machine on your tailnet:"
echo "   curl http://${TS_IP}:${LLAMA_PORT}/v1/models"
echo ""
echo " Logs:"
echo "   journalctl -u afts-llama.service -f"
echo ""
echo " To let GitHub Actions reach this VPS:"
echo "   1. Generate a Tailscale ephemeral auth key at:"
echo "      https://login.tailscale.com/admin/settings/keys"
echo "      (Reusable = OFF, Ephemeral = ON, Pre-approved = ON,"
echo "       Tags = tag:ci, expires in 90 days)"
echo "   2. Add as GitHub repo secret: TAILSCALE_AUTHKEY"
echo "   3. Add to your workflow:"
echo "        - uses: tailscale/github-action@v2"
echo "          with:"
echo "            authkey: \${{ secrets.TAILSCALE_AUTHKEY }}"
echo "            tags: tag:ci"
echo "   4. In your client, set: LLAMA_BASE_URL=http://${TS_IP}:${LLAMA_PORT}/v1"
echo ""
echo " Migration to Mac in 2–3 months:"
echo "   scp -r ${LLAMA_HOME}/models/ user@mac.local:~/llama-models/"
echo "   Then run llama-server on Mac with same flags. Stop this VPS. Save €10/mo."
echo ""
echo "═════════════════════════════════════════════════════════════════════"
