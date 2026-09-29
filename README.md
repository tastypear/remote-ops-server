# remote-ops-server

HTTP backend for [remote-fs-node](https://github.com/tastypear/remote-fs-node) and [remote-cp-node](https://github.com/tastypear/remote-cp-node). A single Python/FastAPI server providing both **filesystem** (SFTP replacement) and **command execution** (SSH exec replacement) APIs over HTTP.

Designed to replace SSH+SFTP+exec as the transport layer for remote hosts — plain HTTP means CDN acceleration (Cloudflare Tunnel), connection pooling, and native batch/patch operations that SSH can't offer.

## Quick start

```bash
cd remote-ops-server
pip3 install -r requirements.txt
REMOTE_OPS_TOKEN=my-secret python3 server.py
```

Or use the launcher (generates a random token if none set):

```bash
./start.sh
```

Server listens on `0.0.0.0:8765` by default.

## Configuration (env vars)

| Variable | Default | Description |
|---|---|---|
| `REMOTE_OPS_TOKEN` | `dev-token-change-me` | Bearer token for auth |
| `REMOTE_OPS_HOST` | `0.0.0.0` | Bind address |
| `REMOTE_OPS_PORT` | `8765` | Listen port |
| `REMOTE_OPS_CORS` | `false` | Enable CORS (`true`/`false`) |
| `REMOTE_OPS_WS_MAX_CONN` | `64` | Max concurrent `/ws/exec` sessions (rejects excess with 4429) |

## Clients

This server is the shared backend for two Node.js drop-in modules:

- **remote-fs-node** — `require("fs")` drop-in (SFTP replacement). Three-layer interception: JS export, `process.binding('fs')`, `node:fs` protocol.
- **remote-cp-node** — `require("child_process")` drop-in (SSH exec replacement). exec/spawn/fork over HTTP with live-PID kill.

Both use the same `configure({ baseURL, token })` shape and can be `patch()`ed in the same process.

## API Reference

All endpoints except `/` and `/health` require `Authorization: Bearer <token>`.

### Command Execution (SSH exec replacement)

| Method | Path | Description |
|---|---|---|
| POST | `/api/exec` | Execute command (sync) — returns `{stdout, stderr, exit_code, pid, duration_ms}` |
| POST | `/api/exec/stream` | Execute command (SSE streaming) — `pid` → `stdout`/`stderr` → `exit` frames; keepalive on idle |
| POST | `/api/exec/kill?pid=&signal_name=` | Kill a spawned process (ownership-checked, process-group `os.killpg`) |
| GET | `/api/exec/status?pid=` | Check if a spawned process is still running (`{pid, running, exit_code, cmd, age_s}`) |
| POST | `/api/exec/stdin` | Write to a spawned process's stdin `{pid, data, close}` |
| WS | `/ws/exec` | Bidirectional exec — streaming stdin, binary-safe stdout/stderr (base64), kill, keepalive |

Request body: `{cmd, args, shell, cwd, env, timeout, stdin, binary}`. `shell:true` runs `sh -c` (for `exec`); `shell:false` passes args as argv (for `spawn`/`execFile`/`fork`, no injection). `binary:true` makes `/api/exec` also return `stdout_b64`/`stderr_b64` (base64 of raw bytes) alongside the decoded strings — lets sync callers avoid `errors="replace"` corruption.

Server guarantees: process registry (every PID tracked, kill/stdin verify ownership), env sanitization (secrets like the auth token stripped from child env), timeout enforced on both sync and stream endpoints, SSE keepalive defeats proxy idle timeouts, orphan cleanup on client disconnect, graceful server shutdown kills all tracked processes and closes all tracked fds.

**`/ws/exec`** covers two edge cases SSE can't: true streaming stdin (write → read → write, interactive) and binary-safe stdout/stderr. Auth via `Authorization: Bearer <token>` header (preferred) or `?token=` query param. JSON message protocol — see `ws_exec` docstring in `server.py` for the full frame reference. PTY mode (`{pty:true, cols, rows}` in the start message) spawns the child with a pseudo-terminal — echo, line editing, terminal control, and `resize` events work. Output is merged (stdout+stderr on one stream, as with any PTY). Detach mode (`{detach:true}`) keeps the process alive after the WS disconnects — output is drained (discarded) in the background and the process stays in the registry for `GET /api/exec/status` or `POST /api/exec/kill`. Binary frames (`{binaryFrames:true}`) send stdout/stderr as WS binary frames (1-byte prefix + raw bytes) instead of JSON+base64, eliminating the 33% base64 overhead.

### File Descriptors (stateful fd session)

| Method | Path | Description |
|---|---|---|
| POST | `/api/fs/fd/open` | Open file `{path, flags, mode}` → `{fd, size}` (numeric O_* flags, `os.open` with O_CLOEXEC) |
| GET | `/api/fs/fd/read?fd=&offset=&length=` | Ranged read (`os.pread`) — StreamingResponse 64KB chunks |
| PUT | `/api/fs/fd/write?fd=&offset=` | Ranged write (`os.pwrite`; O_APPEND uses atomic `os.write`) |
| POST | `/api/fs/fd/close` | Close fd |
| GET | `/api/fs/fd/fstat?fd=` | fstat |
| POST | `/api/fs/fd/ftruncate` | ftruncate `{fd, len}` |
| POST | `/api/fs/fd/fsync` | fsync `{fd}` |
| POST | `/api/fs/fd/fchmod` | fchmod `{fd, mode}` |
| POST | `/api/fs/fd/fchown` | fchown `{fd, uid, gid}` |
| POST | `/api/fs/fd/futimes` | futimes `{fd, atime, mtime}` |

Stateful fd sessions enable O(1) ranged I/O (SFTP parity) — no whole-file buffering per operation. Background sweeper reaps idle fds (300s) and processes (300s).

### File Operations (SFTP replacement)

| Method | Path | Description |
|---|---|---|
| GET | `/api/fs/stat?path=&follow=` | File metadata (follow=true: os.stat, false: os.lstat) |
| GET | `/api/fs/read?path=` | Read file (raw body) |
| PUT | `/api/fs/write?path=&mode=` | Write file (raw body) |
| POST | `/api/fs/delete` | Delete file/dir `{path}` |
| POST | `/api/fs/mkdir` | Create dir `{path, mode}` |
| POST | `/api/fs/move` | Move/rename `{src, dst}` — atomic (`os.rename`, shutil.move fallback) |
| POST | `/api/fs/copy` | Copy `{src, dst}` |
| POST | `/api/fs/chmod` | Change mode `{path, mode}` |
| POST | `/api/fs/touch` | Create empty file |
| POST | `/api/fs/symlink` | Create symlink `{target, link}` |
| GET | `/api/fs/readlink?path=` | Read symlink target |
| GET | `/api/fs/list?path=&recursive=` | List directory |
| GET | `/api/fs/glob?pattern=&path=` | Glob pattern matching |
| GET | `/api/fs/statfs?path=` | Filesystem statistics |
| POST | `/api/fs/chown` | Change owner `{path, uid, gid, follow}` |
| POST | `/api/fs/utimes` | Set times `{path, atime, mtime, follow}` |
| POST | `/api/fs/truncate` | Truncate `{path, len}` |
| POST | `/api/fs/link` | Hard link `{src, dst}` |
| POST | `/api/fs/realpath` | Resolve path `{path}` |
| POST | `/api/fs/mkdtemp` | Create temp dir `{prefix}` |
| GET | `/api/fs/access?path=&mode=` | Check accessibility (204/403/404) |
| GET | `/api/fs/watch?path=&recursive=` | SSE file watcher |

All fs endpoints return structured HTTP errors (404 ENOENT, 409 EEXIST, 403 EACCES) — no 500/traceback on expected failures.

### Batch / Patch

| Method | Path | Description |
|---|---|---|
| POST | `/api/fs/batch` | Multiple ops in one request `{ops: [{op, path, content}]}` |
| POST | `/api/fs/patch` | Apply unified diff via `patch -p1` |

### Utility

| Method | Path | Description |
|---|---|---|
| GET | `/api/which?cmd=` | Find command path |
| GET | `/api/env` | Server environment info (uid, platform) |
| GET | `/health` | Health check (returns pid) |

## Deployment

### systemd

```ini
[Unit]
Description=remote-ops-server
After=network.target

[Service]
ExecStart=/usr/bin/python3 /opt/remote-ops-server/server.py
Environment=REMOTE_OPS_TOKEN=your-secret
Restart=always
User=root

[Install]
WantedBy=multi-user.target
```

### Cloudflare Tunnel

```bash
cloudflared tunnel --url http://localhost:8765
```

Traffic routes through Cloudflare's edge network (line acceleration, no caching). The server stays HTTP; Cloudflare handles TLS termination. SSE keepalive comments prevent proxy idle timeouts on long-running exec streams.

## Test

```bash
./test_api.sh   # curl-based smoke tests against a running server
```

End-to-end test suites live in the client packages (remote-fs-node: 147 tests, remote-cp-node: 47 + 9 integration tests).

## Roadmap

- [x] Python/FastAPI implementation (fs + exec + fd session)
- [x] Stateful fd sessions (os.pread/os.pwrite, SFTP parity)
- [x] Stream-backed exec with live-PID kill + process-group termination
- [ ] WebSocket PTY support for interactive commands
- [ ] Go/Rust rewrite for production throughput

## License

MIT
