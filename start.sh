#!/bin/bash
cd "$(dirname "$0")"
pip3 install -q -r requirements.txt
export REMOTE_OPS_TOKEN="${REMOTE_OPS_TOKEN:-$(python3 -c 'import secrets; print(secrets.token_hex(16))')}"
echo "============================================"
echo "  remote-ops-server"
echo "  Token: $REMOTE_OPS_TOKEN"
echo "  Listen: ${REMOTE_OPS_HOST:-0.0.0.0}:${REMOTE_OPS_PORT:-8765}"
echo "============================================"
exec python3 server.py
