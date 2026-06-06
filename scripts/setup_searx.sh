#!/usr/bin/env bash
#
# AFTS Searx — one-shot setup on afts-llama-vps
#
# Run this on the Hetzner box (NOT in GitHub Actions). It will:
#   1. Install Docker + compose plugin if missing
#   2. Create /opt/afts-searx/ and write all 4 config files
#   3. Generate a Searx secret_key
#   4. Bring up the Docker stack
#   5. Smoke-test the local endpoint
#
# Run it like this on the Hetzner box (as root or via sudo):
#
#   curl -fsSL https://raw.githubusercontent.com/gstoforos/Food-Safety-Intelligence-System/main/scripts/setup_searx.sh | sudo bash
#
# Or, if you scp'd this file up:
#
#   sudo bash setup_searx.sh

set -euo pipefail

INSTALL_DIR="/opt/afts-searx"
SEARX_PORT_LOCAL=8888    # what GitHub Actions calls over Tailscale

echo "═══ AFTS Searx setup on $(hostname) ═══"

# ─── 1. Docker ──────────────────────────────────────────────────────────────
if ! command -v docker >/dev/null 2>&1; then
  echo "→ Installing Docker…"
  curl -fsSL https://get.docker.com | sh
else
  echo "✓ Docker already installed: $(docker --version)"
fi

# ─── 2. Working directory ───────────────────────────────────────────────────
mkdir -p "$INSTALL_DIR/searxng"
cd "$INSTALL_DIR"

# ─── 3. docker-compose.yaml ─────────────────────────────────────────────────
cat > docker-compose.yaml << 'YAML'
version: "3.7"
services:
  caddy:
    container_name: afts-caddy
    image: docker.io/library/caddy:2-alpine
    network_mode: host
    restart: unless-stopped
    volumes:
      - ./Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy-data:/data:rw
      - caddy-config:/config:rw
    cap_drop: [ALL]
    cap_add:  [NET_BIND_SERVICE]
    logging: {driver: "json-file", options: {max-size: "1m", max-file: "1"}}

  searxng:
    container_name: afts-searxng
    image: docker.io/searxng/searxng:latest
    restart: unless-stopped
    ports:
      - "127.0.0.1:8080:8080"
    volumes:
      - ./searxng:/etc/searxng:rw
    environment:
      - SEARXNG_BASE_URL=http://afts-llama-vps:8888/
      - UWSGI_WORKERS=2
      - UWSGI_THREADS=2
    cap_drop: [ALL]
    cap_add:  [CHOWN, SETGID, SETUID]
    logging: {driver: "json-file", options: {max-size: "1m", max-file: "1"}}

volumes:
  caddy-data:
  caddy-config:
YAML

# ─── 4. Caddyfile ───────────────────────────────────────────────────────────
cat > Caddyfile << CADDY
# Caddy listens on the Tailscale-reachable port and forwards to local Searx.
# No public TLS — the tailnet is the perimeter.

:${SEARX_PORT_LOCAL} {
    reverse_proxy 127.0.0.1:8080
    encode gzip
}
CADDY

# ─── 5. searxng/settings.yml ────────────────────────────────────────────────
SECRET_KEY="$(openssl rand -hex 32)"
cat > searxng/settings.yml << SXY
use_default_settings: true

general:
  instance_name:  "AFTS Searx"
  contact_url:    false
  enable_metrics: false
  debug:          false

server:
  secret_key:           "${SECRET_KEY}"
  limiter:              false
  image_proxy:          false
  http_protocol_version: "1.1"

ui:
  static_use_hash: true
  default_locale:  "en"

search:
  safe_search:   0
  autocomplete:  ""
  default_lang:  "en"
  ban_time_on_fail:  0
  max_ban_time_on_fail: 0
  formats:
    - html
    - json

engines:
  - name: google
    engine: google
    shortcut: g
    disabled: false
    timeout: 8.0
  - name: bing
    engine: bing
    shortcut: b
    disabled: false
    timeout: 8.0
  - name: duckduckgo
    engine: duckduckgo
    shortcut: d
    disabled: false
    timeout: 8.0
SXY

# ─── 6. Bring it up ─────────────────────────────────────────────────────────
echo "→ Pulling images and starting…"
docker compose pull
docker compose up -d
sleep 4

# ─── 7. Smoke test ──────────────────────────────────────────────────────────
echo ""
echo "→ Smoke test (local):"
if curl -fsS --max-time 10 "http://localhost:${SEARX_PORT_LOCAL}/search?q=fda+recall&format=json" \
   | head -c 200 ; then
  echo ""
  echo ""
  echo "✓ Searx is up at  http://localhost:${SEARX_PORT_LOCAL}/search"
  echo ""
  echo "→ Next steps:"
  echo "    1. On your laptop (Tailscale connected), test:"
  echo "         curl 'http://afts-llama-vps:${SEARX_PORT_LOCAL}/search?q=test&format=json'"
  echo "    2. Add GitHub Secret:"
  echo "         SEARX_URL = http://afts-llama-vps:${SEARX_PORT_LOCAL}/search"
  echo "    3. Dispatch the workflow."
else
  echo ""
  echo "✗ Smoke test FAILED. Check 'docker compose logs searxng' and 'docker compose logs caddy'"
  exit 1
fi
