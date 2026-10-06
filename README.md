# remote-ops-server

An HTTP backend providing APIs for **filesystem** operations and **command execution**, designed to replace SSH (SFTP + exec).

Two interchangeable implementations of the same API:

- **Go** (`go/`) — Pre-built static binaries on the [releases page](https://github.com/tastypear/remote-ops-server/releases).
- **Python** (`server.py`) — reference implementation using FastAPI/uvicorn.

## Quick start

`REMOTE_OPS_TOKEN=my-secret ./remote-ops-server` or `REMOTE_OPS_TOKEN=my-secret python3 server.py`

Listens on `[::]:8765` by default.

## Configuration (env vars)

| Variable | Default | Description |
|---|---|---|
| `REMOTE_OPS_TOKEN` | `dev-token-change-me` | Bearer token for auth |
| `REMOTE_OPS_HOST` | `::` | Bind address (dual-stack) |
| `REMOTE_OPS_PORT` | `8765` | Listen port |
| `REMOTE_OPS_CORS` | `false` | Enable CORS (`true`/`false`) |
| `REMOTE_OPS_DEBUG` | `false` | Verbose logging |
| `REMOTE_OPS_WS_MAX_CONN` | `64` | Max concurrent `/ws/exec` sessions (rejects excess connections) |

## Clients

- [x] Node: [remote-fs-node](https://github.com/tastypear/remote-fs-node) + [remote-cp-node](https://github.com/tastypear/remote-cp-node)
- [ ] Python: maybe later.

## API Reference

All endpoints except `/` and `/health` require `Authorization: Bearer <token>`.

### Command Execution (SSH exec replacement)

| Method | Path | Description |
|---|---|---|
| POST | `/api/exec` | Execute command (sync) — `{stdout, stderr, exit_code, pid, duration_ms}` |
| POST | `/api/exec/batch` | Execute multiple commands in one request |
| POST | `/api/exec/stream` | Execute command (SSE streaming) — `pid` → `stdout`/`stderr` → `exit` frames; keepalive on idle |
| POST | `/api/exec/kill?pid=&signal_name=` | Kill a spawned process (ownership-checked, process-group signal) |
| GET | `/api/exec/status?pid=` | Check if a spawned process is still running |
| POST | `/api/exec/stdin` | Write to a spawned process's stdin `{pid, data, close}` |
| WS | `/ws/exec` | Bidirectional exec — streaming stdin, binary-safe stdout/stderr, PTY mode, detach mode |

Request body: `{cmd, args, shell, cwd, env, timeout, stdin, binary}`. `shell:true` runs `sh -c`; `shell:false` passes args as argv (no injection). `binary:true` adds base64-encoded `stdout_b64`/`stderr_b64` alongside decoded strings.

`/ws/exec` supports interactive stdin, PTY mode (`{pty:true, cols, rows}` — echo, line editing, resize), detach mode (`{detach:true}` — process survives WS disconnect, stays in registry for status/kill), and binary frames (`{binaryFrames:true}` — raw bytes instead of JSON+base64). Auth via `Authorization` header or `?token=` query param.

### File Descriptors (stateful fd session)

| Method | Path | Description |
|---|---|---|
| POST | `/api/fs/fd/open` | Open file `{path, flags, mode}` → `{fd, size}` |
| GET | `/api/fs/fd/read?fd=&offset=&length=` | Ranged read (`pread`) — 64KB chunks |
| PUT | `/api/fs/fd/write?fd=&offset=` | Ranged write (`pwrite`; O_APPEND uses atomic `write`) |
| POST | `/api/fs/fd/close` | Close fd |
| GET | `/api/fs/fd/fstat?fd=` | fstat |
| POST | `/api/fs/fd/ftruncate` | ftruncate `{fd, len}` |
| POST | `/api/fs/fd/fsync` | fsync `{fd}` |
| POST | `/api/fs/fd/fchmod` | fchmod `{fd, mode}` |
| POST | `/api/fs/fd/fchown` | fchown `{fd, uid, gid}` |
| POST | `/api/fs/fd/futimes` | futimes `{fd, atime, mtime}` |

Stateful fd sessions enable O(1) ranged I/O (SFTP parity). Background sweeper reaps idle fds and processes.

### File Operations (SFTP replacement)

| Method | Path | Description |
|---|---|---|
| GET | `/api/fs/stat?path=&follow=` | File metadata (follow=true: stat, false: lstat) |
| GET | `/api/fs/read?path=` | Read file (raw body) |
| PUT | `/api/fs/write?path=&mode=` | Write file (raw body) |
| POST | `/api/fs/delete` | Delete file/dir `{path}` |
| POST | `/api/fs/mkdir` | Create dir `{path, mode}` |
| POST | `/api/fs/move` | Move/rename `{src, dst}` — atomic |
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

## Test

`./test_api.sh`

curl-based smoke tests, which can also serve as usage examples.

## License

MIT
