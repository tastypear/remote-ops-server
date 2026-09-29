#!/bin/bash
cd "$(dirname "$0")"
pip3 install -q -r requirements.txt
export AGENT_SHIM_TOKEN="${AGENT_SHIM_TOKEN:-$(python3 -c 'import secrets; print(secrets.token_hex(16))')}"
echo "============================================"
echo "  agent-shim-server"
echo "  Token: $AGENT_SHIM_TOKEN"
echo "  Listen: ${AGENT_SHIM_HOST:-0.0.0.0}:${AGENT_SHIM_PORT:-8765}"
echo "============================================"
exec python3 server.py