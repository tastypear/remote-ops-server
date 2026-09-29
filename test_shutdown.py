#!/usr/bin/env python3
"""Test that tracked processes are killed on graceful server shutdown.

Spawns a DETACHED process via /ws/exec (survives WS disconnect), then
SIGTERMs the server. The lifespan cleanup should kill the process —
without it, the detached proc would be orphaned.
"""
import asyncio
import json
import os
import signal
import subprocess
import sys
import time

import websockets

TOKEN = "testtoken123"
PORT = 8766
URL = f"ws://127.0.0.1:{PORT}/ws/exec"


async def spawn_detached_and_get_pid():
    async with websockets.connect(URL, additional_headers={"Authorization": f"Bearer {TOKEN}"}) as ws:
        await ws.send(json.dumps({"type": "start", "cmd": "sleep 12345", "shell": True, "timeout": 300, "detach": True}))
        while True:
            raw = await ws.recv()
            msg = json.loads(raw)
            if msg["type"] == "pid":
                return msg["pid"]
            if msg["type"] == "error":
                raise RuntimeError(msg["data"])


def main():
    # Start server
    env = dict(os.environ)
    env["REMOTE_OPS_TOKEN"] = TOKEN
    env["REMOTE_OPS_PORT"] = str(PORT)
    env["REMOTE_OPS_HOST"] = "0.0.0.0"
    server = subprocess.Popen([sys.executable, "/tmp/server_ws.py"], env=env,
                              stdout=open("/tmp/test_server.log", "w"), stderr=subprocess.STDOUT)
    time.sleep(4)
    if server.poll() is not None:
        print("FAIL: server did not start")
        with open("/tmp/test_server.log") as f:
            print(f.read())
        sys.exit(1)

    try:
        # Spawn a detached process
        pid = asyncio.run(spawn_detached_and_get_pid())
        print(f"spawned detached sleep 12345 (pid={pid})")

        # WS is now closed; detached proc should still be running
        time.sleep(0.5)
        if not _alive(pid):
            print("FAIL: detached process died after WS disconnect")
            sys.exit(1)
        print("detached process survived WS disconnect (expected)")

        # Graceful server shutdown
        server.send_signal(signal.SIGTERM)
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
        print("server shut down")

        # The lifespan cleanup should have killed the process
        time.sleep(1)
        if _alive(pid):
            print(f"FAIL: process {pid} still alive after server shutdown")
            os.kill(pid, signal.SIGKILL)
            sys.exit(1)
        print("PASS: detached process killed on server shutdown")
        sys.exit(0)
    except Exception as e:
        print(f"FAIL: {e}")
        server.kill()
        sys.exit(1)


def _alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


if __name__ == "__main__":
    main()
