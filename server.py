"""
remote-ops-server: HTTP backend for remote file and process operations.
Provides command execution + file management API over HTTP,
designed to replace SSH+SFTP as the transport layer for remote hosts.
"""
import asyncio
import base64
import contextlib
import itertools
import json
import mimetypes
import os
import platform
import shutil
import socket
import stat
import struct
import fcntl
import termios
import pty
import errno
import signal as signal_module
import sys
import time
from pathlib import Path
from typing import Optional, Union

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, StreamingResponse, Response
from pydantic import BaseModel, Field

# ─── Config ──────────────────────────────────────────────────────────────
API_TOKEN = os.environ.get("REMOTE_OPS_TOKEN", "dev-token-change-me")
HOST = os.environ.get("REMOTE_OPS_HOST", "::")  # IPv6 any → dual-stack (also accepts IPv4)
PORT = int(os.environ.get("REMOTE_OPS_PORT", "8765"))
CHUNK_SIZE = 65536  # 64 KB read chunks
ENABLE_CORS = os.environ.get("REMOTE_OPS_CORS", "false").lower() == "true"
WS_MAX_CONN = int(os.environ.get("REMOTE_OPS_WS_MAX_CONN", "64"))  # concurrent WS exec sessions
DEBUG = os.environ.get("REMOTE_OPS_DEBUG", "false").lower() == "true"

_shutdown_event = asyncio.Event()
_main_loop = None


# Lifespan: starts the fd-table and process-table background sweepers on
# startup, cancels them on shutdown. Replaces the deprecated @app.on_event
# ("startup") handlers.
@contextlib.asynccontextmanager
async def lifespan(app):
    global _main_loop
    _main_loop = asyncio.get_event_loop()

    # Install signal handlers that set _shutdown_event before uvicorn's
    # background-task wait. This lets watch generators exit immediately on
    # ^C instead of hanging "Waiting for background tasks to complete".
    _orig_int = signal_module.getsignal(signal_module.SIGINT)
    _orig_term = signal_module.getsignal(signal_module.SIGTERM)

    def _on_signal(signum, frame):
        _main_loop.call_soon_threadsafe(_shutdown_event.set)
        orig = _orig_int if signum == signal_module.SIGINT else _orig_term
        if callable(orig):
            orig(signum, frame)

    signal_module.signal(signal_module.SIGINT, _on_signal)
    signal_module.signal(signal_module.SIGTERM, _on_signal)

    fd_task = asyncio.create_task(_fd_sweeper())
    proc_task = asyncio.create_task(_proc_sweeper())
    try:
        yield
    finally:
        fd_task.cancel()
        proc_task.cancel()
        # Kill all tracked processes on shutdown — the server is going away
        # and can't manage them after restart (proc_table isn't persisted).
        # This covers the gap where a sync /api/exec or detached proc would
        # otherwise be orphaned when the request is cancelled by shutdown.
        for _pid, entry in list(_proc_table.items()):
            try:
                _kill_proc(entry["proc"])
            except Exception:
                pass
        _proc_table.clear()
        # Close all tracked file descriptors.
        for _fd, entry in list(_fd_table.items()):
            try:
                os.close(entry["osfd"])
            except Exception:
                pass
        _fd_table.clear()


app = FastAPI(title="remote-ops-server", version="0.1.0", lifespan=lifespan)

if ENABLE_CORS:
    from fastapi.middleware.cors import CORSMiddleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )


# ─── Auth + Access Log (pure ASGI middleware) ──────────────────────────
# NOTE: Do NOT use @app.middleware("http") — it adds BaseHTTPMiddleware,
# which wraps streaming responses in a task group that doesn't clean up
# when the generator exits, causing uvicorn to hang on shutdown.
class AuthMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope["path"]
        if path in ("/", "/health"):
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers", []))
        auth = headers.get(b"authorization", b"").decode("utf-8", errors="replace")
        token = auth[7:] if auth.startswith("Bearer ") else auth
        if token != API_TOKEN:
            resp = JSONResponse(status_code=401, content={"error": "unauthorized"})
            await resp(scope, receive, send)
            return
        await self.app(scope, receive, send)


class AccessLogMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.time()
        method = scope["method"]
        path = scope["path"]
        query = scope.get("query_string", b"").decode("utf-8", errors="replace")

        # Capture request body for debug logging (POST/PUT/PATCH only).
        body_bytes = b""
        actual_receive = receive
        if DEBUG and method in ("POST", "PUT", "PATCH"):
            more_body = True
            while more_body:
                message = await receive()
                body_bytes += message.get("body", b"")
                more_body = message.get("more_body", False)
            body_consumed = [False]
            async def receive_wrapper():
                if not body_consumed[0]:
                    body_consumed[0] = True
                    return {"type": "http.request", "body": body_bytes, "more_body": False}
                return {"type": "http.request", "body": b"", "more_body": False}
            actual_receive = receive_wrapper

        # Wrap send to capture status code.
        status_code = [0]
        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status_code[0] = message["status"]
            await send(message)

        await self.app(scope, actual_receive, send_wrapper)

        duration_ms = (time.time() - start) * 1000
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        full_path = path + (f"?{query}" if query else "")
        line = f"{ts} {method} {full_path} {status_code[0]} {duration_ms:.0f}ms"
        if DEBUG and body_bytes:
            preview = body_bytes[:500].decode("utf-8", errors="replace")
            line += f" body={preview}"
        print(line, flush=True)


app.add_middleware(AuthMiddleware)
app.add_middleware(AccessLogMiddleware)


# ─── Pydantic models ─────────────────────────────────────────────────────
class ExecRequest(BaseModel):
    cmd: str = ""                       # command string (shell=True) or argv[0] (shell=False)
    args: list[str] = Field(default_factory=list)  # argv[1:] when shell=False
    shell: Union[bool, str] = True     # True=sh -c, False=exec argv, str=use that shell -c
    cwd: str = "/"
    env: dict[str, str] = Field(default_factory=dict)
    timeout: int = 0                   # seconds (0 = no timeout); enforced on BOTH sync and stream
    stdin: Optional[str] = None         # one-shot stdin written before process runs
    binary: bool = False                # True → also return stdout_b64/stderr_b64 (binary-safe)
    uid: Optional[int] = None           # drop to this uid before exec (requires root)
    gid: Optional[int] = None           # drop to this gid before exec (requires root)


class StdinRequest(BaseModel):
    pid: int
    data: str = ""
    close: bool = True                  # close stdin after writing (攒-end semantics)


class BatchExecRequest(BaseModel):
    cmds: list[str]
    mode: str = "sequential"            # "sequential" (shared shell) or "parallel" (independent)
    cwd: str = "/"
    env: dict[str, str] = Field(default_factory=dict)
    timeout: int = 0                    # per-batch (sequential) or per-cmd (parallel), seconds


# Env keys never inherited from the server's own environment — prevents
# REMOTE_OPS_TOKEN and other secrets from leaking into child processes.
_SENSITIVE_ENV = ("TOKEN", "SECRET", "KEY", "PASSWORD", "CREDENTIAL", "AUTH")


def _is_sensitive_env(key: str) -> bool:
    k = key.upper()
    return any(s in k for s in _SENSITIVE_ENV)


def _build_env(req_env: dict[str, str]) -> dict[str, str]:
    # Start from a sanitized copy of os.environ (strip secrets), then layer the
    # client's explicit env on top. SSH exec similarly inherits a sanitized env.
    env = {k: v for k, v in os.environ.items() if not _is_sensitive_env(k)}
    env.update(req_env)
    return env


class SimplePath(BaseModel):
    path: str


class MkdirRequest(BaseModel):
    path: str
    mode: Optional[str] = None
    recursive: bool = True  # default matches os.makedirs (creates parents)


class MoveCopyRequest(BaseModel):
    src: str
    dst: str


class ChmodRequest(BaseModel):
    path: str
    mode: str


class TouchRequest(BaseModel):
    path: str
    mode: Optional[str] = None


class SymlinkRequest(BaseModel):
    target: str
    link: str


class ChownRequest(BaseModel):
    path: str
    uid: int
    gid: int
    follow: bool = True  # True=chown (follow symlink), False=lchown (don't follow)


class UtimesRequest(BaseModel):
    path: str
    atime: int  # epoch milliseconds
    mtime: int  # epoch milliseconds
    follow: bool = True  # True=utimes, False=lutimes


class TruncateRequest(BaseModel):
    path: str
    len: int = 0


class LinkRequest(BaseModel):
    existing: str
    newpath: str


class RealpathRequest(BaseModel):
    path: str


class MkdtempRequest(BaseModel):
    prefix: str


class BatchOp(BaseModel):
    op: str
    path: str
    content: Optional[str] = None
    encoding: str = "text"
    dst: Optional[str] = None
    mode: Optional[str] = None
    follow: bool = True
    recursive: bool = False


class BatchRequest(BaseModel):
    ops: list[BatchOp]


class PatchRequest(BaseModel):
    path: str
    patch: str


# ─── Helpers ─────────────────────────────────────────────────────────────
def _stat_to_dict(s: os.stat_result) -> dict:
    """Convert an os.stat_result into a JSON-serializable dict.

    All timestamps are returned as integer MILLISECONDS since epoch (the unit
    Node's fs.Stats uses for *Ms fields). The client consumes them directly
    without any unit conversion, so the wire contract is: epoch ms, int.
    """
    if stat.S_ISDIR(s.st_mode):
        ftype = "dir"
    elif stat.S_ISLNK(s.st_mode):
        ftype = "symlink"
    elif stat.S_ISBLK(s.st_mode):
        ftype = "blockdev"
    elif stat.S_ISCHR(s.st_mode):
        ftype = "chardev"
    elif stat.S_ISFIFO(s.st_mode):
        ftype = "fifo"
    elif stat.S_ISSOCK(s.st_mode):
        ftype = "socket"
    else:
        ftype = "file"
    # st_mode in full (including S_IFMT bits) so the client can reconstruct
    # the exact mode without guessing from `type`. mode (perm bits only) kept
    # as an octal string for backwards compat with existing callers.
    return {
        "type": ftype,
        "size": s.st_size,
        "mode": oct(s.st_mode & 0o777),
        "fullMode": s.st_mode,
        "mtime": int(s.st_mtime * 1000),
        "atime": int(s.st_atime * 1000),
        "ctime": int(s.st_ctime * 1000),
        "birthtime": int(getattr(s, "st_birthtime", s.st_mtime) * 1000),
        "uid": s.st_uid,
        "gid": s.st_gid,
        "ino": s.st_ino,
        "nlink": s.st_nlink,
        "blksize": getattr(s, "st_blksize", 4096),
        "blocks": getattr(s, "st_blocks", 0),
        "rdev": s.st_rdev,
        "dev": s.st_dev,
    }


def _safe_cwd(cwd: str) -> str:
    return cwd if os.path.isdir(cwd) else "/"


async def _run_fs(fn, *args):
    """Run a blocking fs op in a thread, translating OSError → structured HTTP
    errors. Without this, any OSError (EEXIST, ENOENT, EACCES, ENOTEMPTY …)
    bubbles to uvicorn as a 500 + HTML traceback, which corrupts the HTTP
    connection and can cause socket hang-ups on subsequent requests. HTTPException
    raised inside fn (e.g. the 404 checks in fs_delete/fs_read) is re-raised
    unchanged so existing explicit error codes are preserved."""
    try:
        return await asyncio.to_thread(fn, *args)
    except HTTPException:
        raise
    except FileNotFoundError:
        raise HTTPException(404, "not found")
    except FileExistsError:
        raise HTTPException(409, "exists")
    except PermissionError:
        raise HTTPException(403, "permission denied")
    except OSError as e:
        raise HTTPException(400, str(e))


# ─── Stateful fd session ─────────────────────────────────────────────────
# The fd table makes the server stateful: open files are tracked by an integer
# id so the client can issue O(1) ranged read/write (os.pread/os.pwrite) instead
# of re-reading the whole file per operation. This is the SFTP-parity threshold.
#
# Entry shape: {path, osfd, flags, is_append, last_use}
_fd_table: dict[int, dict] = {}
_next_fd = itertools.count(1000)
_FD_TTL = 300  # seconds an fd may sit idle before the sweeper closes it


def _fd_get(fd: int) -> dict:
    entry = _fd_table.get(fd)
    if entry is None:
        raise HTTPException(410, f"bad fd: {fd}")
    entry["last_use"] = time.monotonic()
    return entry


async def _fd_sweeper():
    """Background task: every 60s close any fd idle longer than _FD_TTL. Guards
    against fd leaks when the client crashes without calling close."""
    while True:
        await asyncio.sleep(60)
        now = time.monotonic()
        stale = [fd for fd, e in _fd_table.items() if now - e["last_use"] > _FD_TTL]
        for fd in stale:
            entry = _fd_table.pop(fd, None)
            if entry:
                try:
                    os.close(entry["osfd"])
                except OSError:
                    pass


# Numeric O_* flags from the client (matching Linux values, same as
# lib/constants.js). We receive the bitmask directly so the server honors
# O_CREAT/O_EXCL/O_TRUNC/O_APPEND exactly without a string round-trip.
_O_ACCESS_MASK = os.O_RDONLY | os.O_WRONLY | os.O_RDWR


class FdOpenRequest(BaseModel):
    path: str
    flags: int  # numeric bitmask (O_RDONLY | O_CREAT | ... )
    mode: int = 0o666


class FdCloseRequest(BaseModel):
    fd: int


class FdTruncateRequest(BaseModel):
    fd: int
    len: int


class FdSyncRequest(BaseModel):
    fd: int


class FdChmodRequest(BaseModel):
    fd: int
    mode: int  # numeric (e.g. 0o644 = 420)


class FdChownRequest(BaseModel):
    fd: int
    uid: int
    gid: int


class FdUtimesRequest(BaseModel):
    fd: int
    atime: int  # epoch milliseconds
    mtime: int  # epoch milliseconds


@app.post("/api/fs/fd/open")
async def fd_open(req: FdOpenRequest):
    def _do():
        # Auto-create parent dirs when creating a file (matches /api/fs/write behavior).
        if req.flags & os.O_CREAT:
            parent = os.path.dirname(req.path)
            if parent:
                os.makedirs(parent, exist_ok=True)
        # O_CLOEXEC so the fd doesn't leak to child processes (e.g. exec endpoints).
        osfd = os.open(req.path, req.flags | os.O_CLOEXEC, req.mode)
        st = os.fstat(osfd)
        fd = next(_next_fd)
        _fd_table[fd] = {
            "path": req.path,
            "osfd": osfd,
            "flags": req.flags,
            "is_append": bool(req.flags & os.O_APPEND),
            "last_use": time.monotonic(),
        }
        return {"fd": fd, "size": st.st_size}
    return await _run_fs(_do)


@app.get("/api/fs/fd/read")
async def fd_read(
    fd: int = Query(...),
    offset: int = Query(0),
    length: int = Query(65536),
):
    entry = _fd_get(fd)
    osfd = entry["osfd"]

    def _iter():
        # os.pread reads at `offset` without moving the fd's position, so
        # concurrent reads on the same fd are safe. Stream in 64KB chunks so
        # large reads don't buffer the whole result in memory.
        remaining = length
        pos = offset
        while remaining > 0:
            n = min(CHUNK_SIZE, remaining)
            chunk = os.pread(osfd, n, pos)
            if not chunk:
                break
            yield chunk
            pos += len(chunk)
            remaining -= len(chunk)

    return StreamingResponse(_iter(), media_type="application/octet-stream")


@app.put("/api/fs/fd/write")
async def fd_write(
    request: Request,
    fd: int = Query(...),
    offset: int = Query(-1),
):
    entry = _fd_get(fd)
    osfd = entry["osfd"]
    is_append = entry["is_append"]
    body = await request.body()

    def _do():
        if is_append:
            # O_APPEND: offset is ignored; os.write appends atomically.
            return os.write(osfd, body)
        # offset -1 means "use current position" (write at the fd's cursor).
        if offset < 0:
            return os.write(osfd, body)
        return os.pwrite(osfd, body, offset)

    written = await _run_fs(_do)
    return {"ok": True, "bytes": written}


@app.post("/api/fs/fd/close")
async def fd_close(req: FdCloseRequest):
    entry = _fd_table.pop(req.fd, None)
    if entry is None:
        raise HTTPException(410, f"bad fd: {req.fd}")
    def _do():
        try:
            os.close(entry["osfd"])
        except OSError:
            pass
    await asyncio.to_thread(_do)
    return {"ok": True}


@app.get("/api/fs/fd/fstat")
async def fd_fstat(fd: int = Query(...)):
    entry = _fd_get(fd)
    return await _run_fs(lambda: _stat_to_dict(os.fstat(entry["osfd"])))


@app.post("/api/fs/fd/ftruncate")
async def fd_ftruncate(req: FdTruncateRequest):
    entry = _fd_get(req.fd)
    await _run_fs(os.ftruncate, entry["osfd"], req.len)
    return {"ok": True}


@app.post("/api/fs/fd/fsync")
async def fd_fsync(req: FdSyncRequest):
    entry = _fd_get(req.fd)
    await _run_fs(os.fsync, entry["osfd"])
    return {"ok": True}


@app.post("/api/fs/fd/fchmod")
async def fd_fchmod(req: FdChmodRequest):
    entry = _fd_get(req.fd)
    await _run_fs(os.fchmod, entry["osfd"], req.mode)
    return {"ok": True}


@app.post("/api/fs/fd/fchown")
async def fd_fchown(req: FdChownRequest):
    entry = _fd_get(req.fd)
    await _run_fs(os.fchown, entry["osfd"], req.uid, req.gid)
    return {"ok": True}


@app.post("/api/fs/fd/futimes")
async def fd_futimes(req: FdUtimesRequest):
    # Python has no os.futimes; use the stored path (stable while the fd is
    # open) so callers get futimes semantics via os.utime.
    entry = _fd_get(req.fd)
    times = (req.atime / 1000.0, req.mtime / 1000.0)
    await _run_fs(os.utime, entry["path"], times)
    return {"ok": True}


# ─── File watching (SSE) ─────────────────────────────────────────────────
def _scan_dir(base_path, recursive):
    """Scan a path and return {full_path: (mtime, size, is_dir)}."""
    result = {}
    if os.path.isdir(base_path) and not os.path.islink(base_path):
        if recursive:
            for root, dirs, files in os.walk(base_path):
                for name in dirs + files:
                    full = os.path.join(root, name)
                    try:
                        s = os.lstat(full)
                        result[full] = (s.st_mtime, s.st_size, stat.S_ISDIR(s.st_mode))
                    except OSError:
                        pass
        else:
            for name in os.listdir(base_path):
                full = os.path.join(base_path, name)
                try:
                    s = os.lstat(full)
                    result[full] = (s.st_mtime, s.st_size, stat.S_ISDIR(s.st_mode))
                except OSError:
                    pass
    else:
        try:
            s = os.lstat(base_path)
            result[base_path] = (s.st_mtime, s.st_size, stat.S_ISDIR(s.st_mode))
        except OSError:
            pass
    return result


@app.get("/api/fs/watch")
async def fs_watch(
    path: str = Query(...),
    recursive: bool = Query(False),
    interval: int = Query(500),
):
    """Watch a file/directory for changes. Returns SSE stream.

    Events:
      {"event":"change","filename":"rel/path","action":"modified"}
      {"event":"rename","filename":"rel/path","action":"created"}
      {"event":"rename","filename":"rel/path","action":"deleted"}
    """
    if not os.path.lexists(path):
        raise HTTPException(404, f"not found: {path}")

    async def generate():
        prev = await asyncio.to_thread(_scan_dir, path, recursive)
        try:
            while not _shutdown_event.is_set():
                try:
                    await asyncio.wait_for(_shutdown_event.wait(), timeout=interval / 1000.0)
                    break
                except asyncio.TimeoutError:
                    pass
                curr = await asyncio.to_thread(_scan_dir, path, recursive)

                # Deleted
                for fpath in prev:
                    if fpath not in curr:
                        rel = os.path.relpath(fpath, path) if fpath != path else None
                        yield f"data: {json.dumps({'event': 'rename', 'filename': rel, 'action': 'deleted'})}\n\n"

                # Created
                for fpath in curr:
                    if fpath not in prev:
                        rel = os.path.relpath(fpath, path) if fpath != path else None
                        yield f"data: {json.dumps({'event': 'rename', 'filename': rel, 'action': 'created'})}\n\n"

                # Modified
                for fpath in curr:
                    if fpath in prev and prev[fpath] != curr[fpath]:
                        rel = os.path.relpath(fpath, path) if fpath != path else None
                        yield f"data: {json.dumps({'event': 'change', 'filename': rel, 'action': 'modified'})}\n\n"

                prev = curr
        except asyncio.CancelledError:
            raise
        except (GeneratorExit, Exception):
            pass

    return StreamingResponse(generate(), media_type="text/event-stream")

# ─── System endpoints ───────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "name": "remote-ops-server",
        "version": "0.1.0",
        "endpoints": {
            "exec": ["POST /api/exec", "POST /api/exec/batch", "POST /api/exec/stream", "POST /api/exec/kill", "GET /api/exec/status", "POST /api/exec/stdin", "WS /ws/exec"],
            "fs": [
                "GET /api/fs/stat", "GET /api/fs/read", "PUT /api/fs/write",
                "POST /api/fs/delete", "POST /api/fs/mkdir", "POST /api/fs/move",
                "POST /api/fs/copy", "POST /api/fs/chmod", "POST /api/fs/touch",
                "POST /api/fs/symlink", "GET /api/fs/readlink", "GET /api/fs/list",
                "POST /api/fs/batch", "POST /api/fs/patch",
            ],
            "util": ["GET /api/which", "GET /api/env"],
        },
    }


@app.get("/health")
async def health():
    return {"status": "ok", "pid": os.getpid()}


# ─── Command execution ──────────────────────────────────────────────────
# Stateful process table: every spawned process is registered so the kill and
# stdin endpoints can verify ownership (no raw os.kill on arbitrary PIDs).
# Entries are removed on process exit or reaped by a background sweeper.
# Mirrors the _fd_table / _fd_sweeper pattern used by the fd session layer.
_proc_table: dict[int, dict] = {}   # {pid: {proc, cmd, spawn_time, last_use}}
_PROC_MAX_IDLE = 300                # 5 min — matches fd TTL; reaps orphaned processes promptly


def _proc_register(proc, cmd: str) -> None:
    now = time.monotonic()
    _proc_table[proc.pid] = {"proc": proc, "cmd": cmd, "spawn_time": now, "last_use": now}


def _proc_unregister(pid: int) -> None:
    _proc_table.pop(pid, None)


def _proc_get(pid: int):
    entry = _proc_table.get(pid)
    if entry:
        entry["last_use"] = time.monotonic()
    return entry


def _proc_touch(pid: int) -> None:
    """Mark a process as recently active so the sweeper doesn't reap it."""
    entry = _proc_table.get(pid)
    if entry:
        entry["last_use"] = time.monotonic()


async def _proc_sweeper():
    while True:
        await asyncio.sleep(60)
        now = time.monotonic()
        for pid, entry in list(_proc_table.items()):
            proc = entry["proc"]
            if proc.returncode is not None:
                _proc_table.pop(pid, None)
                continue
            if now - entry["last_use"] > _PROC_MAX_IDLE:
                _kill_proc(proc)
                _proc_table.pop(pid, None)


def _make_preexec(gid, uid):
    """Build a preexec_fn that drops privileges (gid before uid). None if no-op."""
    if gid is None and uid is None:
        return None
    def fn():
        if gid is not None:
            os.setgid(gid)
        if uid is not None:
            os.setuid(uid)
    return fn


async def _spawn(req: ExecRequest):
    """Create the subprocess per req.shell (True→sh -c, str→that shell -c, else exec argv)."""
    env = _build_env(req.env)
    cwd = _safe_cwd(req.cwd)
    preexec = _make_preexec(req.gid, req.uid)
    # start_new_session=True puts the child in its own process group so timeout
    # and kill can take down the whole tree (sh -c "sleep 3" kills sleep too).
    if req.shell is True:
        label = req.cmd
        proc = await asyncio.create_subprocess_shell(
            req.cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
            cwd=cwd,
            env=env,
            start_new_session=True,
            preexec_fn=preexec,
        )
    else:
        if isinstance(req.shell, str) and req.shell:
            argv = [req.shell, "-c", req.cmd]
        else:
            argv = [req.cmd] + list(req.args)
        label = " ".join(argv)
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.PIPE,
            cwd=cwd,
            env=env,
            start_new_session=True,
            preexec_fn=preexec,
        )
    _proc_register(proc, label)
    return proc


def _kill_proc(proc, sig=signal_module.SIGKILL):
    """Kill the child's whole process group (proc is a session leader)."""
    try:
        os.killpg(os.getpgid(proc.pid), sig)
        return True
    except (ProcessLookupError, PermissionError):
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        return False


@app.post("/api/exec")
async def exec_command(req: ExecRequest):
    start = time.monotonic()
    try:
        proc = await _spawn(req)
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={
            "error": "command not found", "error_code": "ENOENT", "cmd": req.cmd,
            "stdout": "", "stderr": "", "exit_code": -2, "pid": 0,
            "duration_ms": int((time.monotonic() - start) * 1000),
        })
    except PermissionError:
        return JSONResponse(status_code=403, content={
            "error": "permission denied", "error_code": "EACCES", "cmd": req.cmd,
            "stdout": "", "stderr": "", "exit_code": -13, "pid": 0,
            "duration_ms": int((time.monotonic() - start) * 1000),
        })
    except OSError as e:
        return JSONResponse(status_code=400, content={
            "error": e.strerror or str(e), "error_code": "E2BIG" if e.errno == 7 else "EOTHER",
            "cmd": req.cmd[:200], "stdout": "", "stderr": "", "exit_code": -19, "pid": 0,
            "duration_ms": int((time.monotonic() - start) * 1000),
        })
    pid = proc.pid
    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(input=req.stdin.encode() if req.stdin else None),
            timeout=req.timeout if req.timeout > 0 else None,
        )
    except asyncio.TimeoutError:
        _kill_proc(proc)
        await proc.wait()
        _proc_unregister(pid)
        return JSONResponse(
            status_code=408,
            content={
                "error": "timeout",
                "timeout": req.timeout,
                "stdout": "",
                "stderr": "",
                "exit_code": -1,
                "pid": pid,
                "duration_ms": int((time.monotonic() - start) * 1000),
            },
        )
    _proc_unregister(pid)
    resp = {
        "stdout": stdout_b.decode(errors="replace"),
        "stderr": stderr_b.decode(errors="replace"),
        "exit_code": proc.returncode,
        "pid": pid,
        "duration_ms": int((time.monotonic() - start) * 1000),
    }
    if req.binary:
        resp["stdout_b64"] = base64.b64encode(stdout_b).decode("ascii")
        resp["stderr_b64"] = base64.b64encode(stderr_b).decode("ascii")
    return resp


async def _exec_one(cmd: str, cwd: str, env: dict, timeout: int) -> dict:
    start = time.monotonic()
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=_safe_cwd(cwd),
            env=_build_env(env),
            start_new_session=True,
        )
    except Exception as e:
        return {"stdout": "", "stderr": str(e), "exit_code": -1,
                "duration_ms": int((time.monotonic() - start) * 1000)}
    try:
        stdout_b, stderr_b = await asyncio.wait_for(
            proc.communicate(),
            timeout=timeout if timeout > 0 else None,
        )
    except asyncio.TimeoutError:
        _kill_proc(proc)
        await proc.wait()
        return {"stdout": "", "stderr": "", "exit_code": -1,
                "duration_ms": int((time.monotonic() - start) * 1000)}
    return {
        "stdout": stdout_b.decode(errors="replace"),
        "stderr": stderr_b.decode(errors="replace"),
        "exit_code": proc.returncode,
        "duration_ms": int((time.monotonic() - start) * 1000),
    }


@app.post("/api/exec/batch")
async def exec_batch(req: BatchExecRequest):
    if len(req.cmds) == 0:
        return {"results": []}
    if len(req.cmds) > 64:
        raise HTTPException(413, "batch too large: max 64 cmds")
    if req.mode == "parallel":
        results = await asyncio.gather(*[
            _exec_one(cmd, req.cwd, req.env, req.timeout) for cmd in req.cmds
        ])
        return {"results": list(results)}
    else:
        joined = "\n".join(req.cmds)
        result = await _exec_one(joined, req.cwd, req.env, req.timeout)
        return {"results": [result]}


@app.post("/api/exec/stream")
async def exec_stream(req: ExecRequest):
    async def generate():
        try:
            proc = await _spawn(req)
        except FileNotFoundError:
            yield f"data: {json.dumps({'type': 'error', 'data': f'spawn {req.cmd} ENOENT', 'code': 'ENOENT'})}\n\n"
            yield f"data: {json.dumps({'type': 'exit', 'code': -2, 'signal': None, 'duration_ms': 0})}\n\n"
            return
        except PermissionError:
            yield f"data: {json.dumps({'type': 'error', 'data': f'spawn {req.cmd} EACCES', 'code': 'EACCES'})}\n\n"
            yield f"data: {json.dumps({'type': 'exit', 'code': -13, 'signal': None, 'duration_ms': 0})}\n\n"
            return
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'data': str(e)})}\n\n"
            yield f"data: {json.dumps({'type': 'exit', 'code': -2, 'signal': None, 'duration_ms': 0})}\n\n"
            return
        pid = proc.pid
        deadline = time.monotonic() + req.timeout if req.timeout and req.timeout > 0 else None
        start = time.monotonic()
        queue: asyncio.Queue = asyncio.Queue()

        # One-shot initial stdin (EOF after write — this is the input blob, not
        # an interactive stream). Incremental stdin arrives via /api/exec/stdin.
        if req.stdin:
            proc.stdin.write(req.stdin.encode())
            await proc.stdin.drain()
            proc.stdin.close()

        async def reader(stream, stream_type):
            try:
                while True:
                    chunk = await stream.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    await queue.put({"type": stream_type, "data": chunk.decode(errors="replace")})
            except Exception as e:
                await queue.put({"type": "error", "data": str(e)})
            finally:
                await queue.put(None)

        t1 = asyncio.create_task(reader(proc.stdout, "stdout"))
        t2 = asyncio.create_task(reader(proc.stderr, "stderr"))

        yield f"data: {json.dumps({'type': 'pid', 'pid': pid})}\n\n"

        timed_out = False
        done = 0
        try:
            while done < 2:
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        timed_out = True
                        break
                wait = remaining if remaining is not None and remaining < 15 else 15
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=wait)
                except asyncio.TimeoutError:
                    # Idle — emit SSE keepalive comment to defeat proxy idle
                    # timeouts (e.g. Cloudflare's 100s).
                    yield ": keepalive\n\n"
                    continue
                if item is None:
                    done += 1
                else:
                    yield f"data: {json.dumps(item)}\n\n"
        except (asyncio.CancelledError, GeneratorExit):
            # Client disconnected — best-effort kill to prevent orphans.
            _kill_proc(proc)
            raise
        finally:
            if timed_out:
                _kill_proc(proc)
            await asyncio.gather(t1, t2, return_exceptions=True)
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except Exception:
                pass
            try:
                if proc.stdin and not proc.stdin.is_closing():
                    proc.stdin.close()
            except Exception:
                pass
            _proc_unregister(pid)

        duration_ms = int((time.monotonic() - start) * 1000)
        code = proc.returncode
        yield f"data: {json.dumps({'type': 'exit', 'code': code, 'signal': 'SIGKILL' if timed_out else None, 'duration_ms': duration_ms})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/fs/access")
async def fs_access(
    path: str = Query(...),
    mode: str = Query("0"),
):
    """Check file accessibility (like os.access). Returns 204 if accessible, 403 if not."""
    try:
        mode_int = int(mode, 8) if mode else 0
    except ValueError:
        mode_int = 0

    def _do():
        if not os.path.lexists(path):
            raise FileNotFoundError(path)
        if mode_int:
            return os.access(path, mode_int)
        return True

    try:
        ok = await asyncio.to_thread(_do)
        if ok:
            return Response(status_code=204)
        else:
            raise HTTPException(403, f"access denied: {path}")
    except FileNotFoundError:
        raise HTTPException(404, f"not found: {path}")


@app.post("/api/exec/kill")
async def exec_kill(pid: int = Query(...), signal_name: str = Query("SIGTERM")):
    """Kill a process spawned by this server. Ownership-checked against _proc_table."""
    entry = _proc_get(pid)
    if not entry:
        return JSONResponse(status_code=404, content={"error": "no such process", "pid": pid})
    allowed = {"SIGTERM", "SIGKILL", "SIGINT", "SIGUSR1", "SIGUSR2", "SIGHUP"}
    sig_name = signal_name if signal_name in allowed else "SIGTERM"
    sig = getattr(signal_module, sig_name)

    def _do():
        return _kill_proc(entry["proc"], sig)

    ok = await asyncio.to_thread(_do)
    return {"ok": ok, "pid": pid, "signal": sig_name}


@app.get("/api/exec/status")
async def exec_status(pid: int = Query(...)):
    """Check if a spawned process is still running. Ownership-checked against _proc_table."""
    entry = _proc_get(pid)
    if not entry:
        return JSONResponse(status_code=404, content={"error": "no such process", "pid": pid})
    proc = entry["proc"]
    return {
        "pid": pid,
        "running": proc.returncode is None,
        "exit_code": proc.returncode,
        "cmd": entry["cmd"],
        "age_s": int(time.monotonic() - entry["spawn_time"]),
    }


@app.post("/api/exec/stdin")
async def exec_stdin(req: StdinRequest):
    """Write to a spawned process's stdin (攒-end: write data, optionally close)."""
    entry = _proc_get(req.pid)
    if not entry:
        return JSONResponse(status_code=404, content={"error": "no such process", "pid": req.pid})
    proc = entry["proc"]
    if proc.stdin is None:
        return JSONResponse(status_code=409, content={"error": "process has no stdin", "pid": req.pid})
    try:
        if req.data:
            proc.stdin.write(req.data.encode())
            try:
                await proc.stdin.drain()
            except (BrokenPipeError, RuntimeError):
                pass
        if req.close:
            # close() is synchronous on asyncio StreamWriter; do not await.
            proc.stdin.close()
    except (BrokenPipeError, RuntimeError) as e:
        return JSONResponse(status_code=409, content={"error": str(e), "pid": req.pid})
    return {"ok": True, "pid": req.pid}


# ─── WebSocket exec (bidirectional, streaming stdin + binary-safe) ───────
# Edge cases that SSE can't handle:
#   1. True streaming stdin (write → read → write, interactive)
#   2. Binary-safe stdout/stderr (base64 fallback; SSE used errors="replace")
#   3. Real-time bidirectional (sub-frame latency both directions)
#
# Message protocol (JSON text frames):
#   Client → Server:
#     {"type":"start","cmd":"...","args":[...],"shell":true,"cwd":"/tmp","env":{},"timeout":30}
#     {"type":"stdin","data":"...","encoding":"utf8"}         # text stdin
#     {"type":"stdin","data":"base64...","encoding":"base64"} # binary stdin
#     {"type":"stdin_close"}                                  # EOF
#     {"type":"kill","signal":"SIGTERM"}
#
#   Server → Client:
#     {"type":"pid","pid":12345}
#     {"type":"stdout","data":"...","encoding":"utf8"}
#     {"type":"stdout","data":"base64...","encoding":"base64"}
#     {"type":"stderr","data":"...","encoding":"utf8"}
#     {"type":"exit","code":0,"signal":null,"duration_ms":123}
#     {"type":"error","data":"..."}
#     {"type":"keepalive"}                                    # idle ping


def _ws_auth(ws: WebSocket) -> bool:
    """Bearer header first (keeps token out of URLs/logs), query param as fallback."""
    auth = ws.headers.get("authorization", "")
    if auth.startswith("Bearer ") and auth[7:] == API_TOKEN:
        return True
    return ws.query_params.get("token", "") == API_TOKEN


_ws_connections: int = 0   # active /ws/exec sessions (bounded by WS_MAX_CONN)
_drain_tasks: set = set()   # strong refs to detached drain tasks (prevent GC pre-3.12)


def _pty_set_winsize(fd: int, rows: int, cols: int) -> None:
    """Set the PTY window size (TIOCSWINSZ ioctl)."""
    winsize = struct.pack("HHHH", rows, cols, 0, 0)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, winsize)


async def _drain_proc(proc, master_fd, use_pty, pid):
    """Drain output from a detached process until it exits, then unregister."""
    try:
        if use_pty and master_fd is not None:
            loop = asyncio.get_event_loop()
            while True:
                try:
                    chunk = await loop.run_in_executor(None, os.read, master_fd, CHUNK_SIZE)
                except OSError as e:
                    if e.errno == errno.EIO:
                        break
                    raise
                if not chunk:
                    break
        else:
            async def drain_stream(stream):
                while True:
                    chunk = await stream.read(CHUNK_SIZE)
                    if not chunk:
                        break
            await asyncio.gather(drain_stream(proc.stdout), drain_stream(proc.stderr))
        await proc.wait()
    except Exception:
        pass
    finally:
        # If the proc is still alive (e.g., drain task cancelled on server
        # shutdown), kill it before unregistering so it's not orphaned.
        if proc.returncode is None:
            try:
                _kill_proc(proc)
            except Exception:
                pass
        if use_pty and master_fd is not None:
            try:
                os.close(master_fd)
            except Exception:
                pass
        _proc_unregister(pid)


@app.websocket("/ws/exec")
async def ws_exec(ws: WebSocket):
    global _ws_connections
    await ws.accept()

    if not _ws_auth(ws):
        await ws.send_json({"type": "error", "data": "unauthorized"})
        await ws.close(code=4401)
        return

    if _ws_connections >= WS_MAX_CONN:
        await ws.send_json({"type": "error", "data": "too many connections"})
        await ws.close(code=4429)
        return

    _ws_connections += 1
    proc = None
    pid = None
    try:
        msg = json.loads(await ws.receive_text())
        if msg.get("type") != "start":
            await ws.send_json({"type": "error", "data": "expected start message"})
            await ws.close()
            return

        # Build ExecRequest from known fields only (ignore unknown keys).
        known = ExecRequest.model_fields
        req = ExecRequest(**{k: v for k, v in msg.items() if k in known})

        use_pty = bool(msg.get("pty"))
        detachable = bool(msg.get("detach"))
        use_binary = bool(msg.get("binaryFrames"))
        master_fd = None

        try:
            if use_pty:
                master_fd, slave_fd = pty.openpty()
                _pty_set_winsize(master_fd, int(msg.get("rows", 24)), int(msg.get("cols", 80)))
                env = _build_env(req.env)
                env.setdefault("TERM", "xterm-256color")
                cwd = _safe_cwd(req.cwd)
                preexec = _make_preexec(req.gid, req.uid)
                if req.shell is True:
                    proc = await asyncio.create_subprocess_shell(
                        req.cmd, stdin=slave_fd, stdout=slave_fd, stderr=slave_fd,
                        cwd=cwd, env=env, start_new_session=True, preexec_fn=preexec,
                    )
                else:
                    if isinstance(req.shell, str) and req.shell:
                        argv = [req.shell, "-c", req.cmd]
                    else:
                        argv = [req.cmd] + list(req.args)
                    proc = await asyncio.create_subprocess_exec(
                        *argv, stdin=slave_fd, stdout=slave_fd, stderr=slave_fd,
                        cwd=cwd, env=env, start_new_session=True, preexec_fn=preexec,
                    )
                os.close(slave_fd)
                _proc_register(proc, req.cmd)
            else:
                proc = await _spawn(req)
        except FileNotFoundError:
            await ws.send_json({"type": "error", "data": f"spawn {req.cmd} ENOENT", "code": "ENOENT"})
            await ws.send_json({"type": "exit", "code": -2, "signal": None, "duration_ms": 0})
            await ws.close()
            return
        except PermissionError:
            await ws.send_json({"type": "error", "data": f"spawn {req.cmd} EACCES", "code": "EACCES"})
            await ws.send_json({"type": "exit", "code": -13, "signal": None, "duration_ms": 0})
            await ws.close()
            return
        except Exception as e:
            await ws.send_json({"type": "error", "data": str(e)})
            await ws.send_json({"type": "exit", "code": -2, "signal": None, "duration_ms": 0})
            await ws.close()
            return

        pid = proc.pid
        await ws.send_json({"type": "pid", "pid": pid})

        deadline = time.monotonic() + req.timeout if req.timeout and req.timeout > 0 else None
        start = time.monotonic()
        queue: asyncio.Queue = asyncio.Queue()

        # One-shot initial stdin (no EOF — streaming stdin may follow).
        if req.stdin:
            if use_pty:
                try:
                    os.write(master_fd, req.stdin.encode())
                except OSError:
                    pass
            else:
                try:
                    proc.stdin.write(req.stdin.encode())
                    await proc.stdin.drain()
                except (BrokenPipeError, RuntimeError):
                    pass

        async def reader(stream, stream_type):
            prefix = b"\x00" if stream_type == "stdout" else b"\x01"
            try:
                while True:
                    chunk = await stream.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    if use_binary:
                        await queue.put(prefix + chunk)
                    else:
                        try:
                            text = chunk.decode("utf-8")
                            await queue.put({"type": stream_type, "data": text, "encoding": "utf8"})
                        except UnicodeDecodeError:
                            b64 = base64.b64encode(chunk).decode("ascii")
                            await queue.put({"type": stream_type, "data": b64, "encoding": "base64"})
            except Exception as e:
                await queue.put({"type": "error", "data": str(e)})
            finally:
                await queue.put(None)

        if use_pty:
            async def pty_reader():
                loop = asyncio.get_event_loop()
                try:
                    while True:
                        try:
                            chunk = await loop.run_in_executor(None, os.read, master_fd, CHUNK_SIZE)
                        except OSError as e:
                            if e.errno == errno.EIO:
                                break
                            raise
                        if not chunk:
                            break
                        if use_binary:
                            await queue.put(b"\x00" + chunk)
                        else:
                            try:
                                text = chunk.decode("utf-8")
                                await queue.put({"type": "stdout", "data": text, "encoding": "utf8"})
                            except UnicodeDecodeError:
                                b64 = base64.b64encode(chunk).decode("ascii")
                                await queue.put({"type": "stdout", "data": b64, "encoding": "base64"})
                except Exception as e:
                    await queue.put({"type": "error", "data": str(e)})
                finally:
                    await queue.put(None)
            reader_tasks = [asyncio.create_task(pty_reader())]
            num_readers = 1
        else:
            reader_tasks = [
                asyncio.create_task(reader(proc.stdout, "stdout")),
                asyncio.create_task(reader(proc.stderr, "stderr")),
            ]
            num_readers = 2

        async def stdin_writer():
            """Receive client messages and write to proc.stdin / master_fd / kill."""
            try:
                while True:
                    raw = await ws.receive_text()
                    m = json.loads(raw)
                    t = m.get("type")
                    if t == "stdin":
                        data = m.get("data", "")
                        if m.get("encoding") == "base64":
                            data = base64.b64decode(data)
                        else:
                            data = data.encode("utf-8")
                        if use_pty:
                            try:
                                os.write(master_fd, data)
                            except OSError:
                                pass
                        else:
                            try:
                                proc.stdin.write(data)
                                await proc.stdin.drain()
                            except (BrokenPipeError, RuntimeError):
                                pass
                        _proc_touch(pid)
                    elif t == "stdin_close":
                        if not use_pty:
                            try:
                                proc.stdin.close()
                            except Exception:
                                pass
                        # PTY: closing the master kills the session; just ignore.
                    elif t == "kill":
                        sig = getattr(signal_module, m.get("signal", "SIGTERM"), signal_module.SIGTERM)
                        _kill_proc(proc, sig)
                    elif t == "resize":
                        if use_pty:
                            try:
                                _pty_set_winsize(master_fd, int(m.get("rows", 24)), int(m.get("cols", 80)))
                            except Exception:
                                pass
            except WebSocketDisconnect:
                if not detachable:
                    _kill_proc(proc)
            except Exception:
                pass

        stdin_task = asyncio.create_task(stdin_writer())

        timed_out = False
        done = 0
        try:
            while done < num_readers:
                remaining = None
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        timed_out = True
                        break
                wait = remaining if remaining is not None and remaining < 15 else 15
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=wait)
                except asyncio.TimeoutError:
                    await ws.send_json({"type": "keepalive"})
                    continue
                if item is None:
                    done += 1
                else:
                    _proc_touch(pid)
                    if isinstance(item, bytes):
                        await ws.send_bytes(item)
                    else:
                        await ws.send_json(item)
        except WebSocketDisconnect:
            if not detachable:
                _kill_proc(proc)
            raise
        except asyncio.CancelledError:
            _kill_proc(proc)
            raise
        finally:
            stdin_task.cancel()
            if timed_out:
                _kill_proc(proc)
            # Detach: keep the proc alive, drain output in the background.
            if detachable and proc.returncode is None and not timed_out:
                for t in reader_tasks:
                    t.cancel()
                await asyncio.gather(*reader_tasks, return_exceptions=True)
                dt = asyncio.create_task(_drain_proc(proc, master_fd, use_pty, pid))
                _drain_tasks.add(dt)
                dt.add_done_callback(_drain_tasks.discard)
            else:
                await asyncio.gather(*reader_tasks, return_exceptions=True)
                try:
                    await asyncio.wait_for(proc.wait(), timeout=5)
                except Exception:
                    pass
                try:
                    if not use_pty and proc.stdin and not proc.stdin.is_closing():
                        proc.stdin.close()
                except Exception:
                    pass
                if use_pty and master_fd is not None:
                    try:
                        os.close(master_fd)
                    except Exception:
                        pass
                _proc_unregister(pid)

                duration_ms = int((time.monotonic() - start) * 1000)
                code = proc.returncode
                signal_name = "SIGKILL" if timed_out else None
                try:
                    await ws.send_json({
                        "type": "exit",
                        "code": code,
                        "signal": signal_name,
                        "duration_ms": duration_ms,
                    })
                except Exception:
                    pass

    except WebSocketDisconnect:
        if proc and not detachable:
            _kill_proc(proc)
        if pid is not None and not detachable:
            _proc_unregister(pid)
    except Exception as e:
        try:
            await ws.send_json({"type": "error", "data": str(e)})
        except Exception:
            pass
    finally:
        _ws_connections -= 1
        try:
            await ws.close()
        except Exception:
            pass


# ─── File operations ─────────────────────────────────────────────────────
@app.get("/api/fs/stat")
async def fs_stat(
    path: str = Query(...),
    follow: bool = Query(True),
):
    """Stat a path. follow=true (default) uses os.stat (follows symlinks);
    follow=false uses os.lstat (does not follow)."""
    def _do():
        try:
            st = os.stat(path) if follow else os.lstat(path)
            return _stat_to_dict(st)
        except FileNotFoundError:
            raise HTTPException(404, f"not found: {path}")
        except Exception as e:
            raise HTTPException(400, str(e))
    return await asyncio.to_thread(_do)


@app.get("/api/fs/read")
async def fs_read(
    path: str = Query(...),
    download: bool = Query(False),
):
    def _check():
        if not os.path.exists(path):
            raise HTTPException(404, f"not found: {path}")
        if os.path.isdir(path) and not os.path.islink(path):
            raise HTTPException(400, f"is a directory: {path}")
    await asyncio.to_thread(_check)

    content_type, _ = mimetypes.guess_type(path)
    headers = {}
    if download:
        headers["Content-Disposition"] = f'attachment; filename="{os.path.basename(path)}"'

    # Non-regular files (devices, FIFOs, sockets) may block on f.read() after
    # the first chunk. StreamingResponse runs the sync generator in a threadpool
    # that can't be interrupted on client disconnect, leaving a stuck task.
    # Read once and return a plain Response instead.
    def _is_regular():
        try:
            st = os.stat(path)
            return stat.S_ISREG(st.st_mode)
        except OSError:
            return False

    if not await asyncio.to_thread(_is_regular):
        def _read_once():
            try:
                fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
                try:
                    return os.read(fd, CHUNK_SIZE)
                finally:
                    os.close(fd)
            except OSError:
                return b""
        data = await asyncio.to_thread(_read_once)
        return Response(content=data, media_type=content_type or "application/octet-stream", headers=headers)

    def _iter():
        with open(path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                yield chunk

    return StreamingResponse(
        _iter(),
        media_type=content_type or "application/octet-stream",
        headers=headers,
    )


@app.put("/api/fs/write")
async def fs_write(
    request: Request,
    path: str = Query(...),
    mode: Optional[str] = Query(None),
    append: bool = Query(False),
    flush: bool = Query(False),
):
    body = await request.body()

    def _do():
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        if append and p.exists():
            # Append mode: open for append, write body, preserve existing content.
            with open(path, "ab") as f:
                f.write(body)
        else:
            p.write_bytes(body)
        if mode:
            os.chmod(path, int(mode, 8))
        if flush:
            # fs.createWriteStream({flush:true}) — durably persist before close.
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    await _run_fs(_do)
    return {"ok": True, "path": path, "bytes": len(body)}


@app.post("/api/fs/delete")
async def fs_delete(req: SimplePath):
    def _do():
        if not os.path.lexists(req.path):
            raise HTTPException(404, f"not found: {req.path}")
        if os.path.isdir(req.path) and not os.path.islink(req.path):
            shutil.rmtree(req.path)
        else:
            os.remove(req.path)
    await _run_fs(_do)
    return {"ok": True, "path": req.path}


@app.post("/api/fs/mkdir")
async def fs_mkdir(req: MkdirRequest):
    def _do():
        mode_int = int(req.mode, 8) if req.mode else None
        if req.recursive:
            # os.makedirs creates intermediate dirs; exist_ok=True so it's idempotent.
            # On Linux, intermediate dirs get default perms; only the final leaf gets `mode`.
            if mode_int is not None:
                os.makedirs(req.path, exist_ok=True)
                os.chmod(req.path, mode_int)
            else:
                os.makedirs(req.path, exist_ok=True)
        else:
            # Strict single-level mkdir: raise FileExistsError if it exists,
            # FileNotFoundError if the parent is missing — mirroring os.mkdir.
            if mode_int is not None:
                os.mkdir(req.path, mode_int)
            else:
                os.mkdir(req.path)
    await _run_fs(_do)
    return {"ok": True, "path": req.path}


@app.post("/api/fs/move")
async def fs_move(req: MoveCopyRequest):
    def _do():
        # os.rename is atomic on the same filesystem; fall back to shutil.move
        # for cross-device (EXDEV) or when the destination is a non-empty dir.
        try:
            os.rename(req.src, req.dst)
        except OSError:
            shutil.move(req.src, req.dst)
    await _run_fs(_do)
    return {"ok": True, "src": req.src, "dst": req.dst}


@app.post("/api/fs/copy")
async def fs_copy(req: MoveCopyRequest):
    def _do():
        if os.path.isdir(req.src):
            shutil.copytree(req.src, req.dst)
        else:
            shutil.copy2(req.src, req.dst)
    await _run_fs(_do)
    return {"ok": True, "src": req.src, "dst": req.dst}


@app.post("/api/fs/chmod")
async def fs_chmod(req: ChmodRequest):
    try:
        mode_val = int(req.mode, 8)
    except (ValueError, TypeError):
        raise HTTPException(400, f"invalid mode: {req.mode}")
    await _run_fs(os.chmod, req.path, mode_val)
    return {"ok": True, "path": req.path, "mode": req.mode}


@app.post("/api/fs/touch")
async def fs_touch(req: TouchRequest):
    def _do():
        Path(req.path).touch()
        if req.mode:
            os.chmod(req.path, int(req.mode, 8))
    await _run_fs(_do)
    return {"ok": True, "path": req.path}


@app.post("/api/fs/symlink")
async def fs_symlink(req: SymlinkRequest):
    await _run_fs(os.symlink, req.target, req.link)
    return {"ok": True, "target": req.target, "link": req.link}


@app.get("/api/fs/readlink")
async def fs_readlink(path: str = Query(...)):
    try:
        target = await asyncio.to_thread(os.readlink, path)
        return {"path": path, "target": target}
    except OSError as e:
        raise HTTPException(400, str(e))


@app.post("/api/fs/chown")
async def fs_chown(req: ChownRequest):
    """chown (follow=true) or lchown (follow=false). Uses os.chown / os.lchown
    directly — no shell, so paths with spaces/$/backticks are safe."""
    def _do():
        if not os.path.lexists(req.path):
            raise FileNotFoundError(req.path)
        if req.follow:
            os.chown(req.path, req.uid, req.gid)
        else:
            os.lchown(req.path, req.uid, req.gid)
    try:
        await asyncio.to_thread(_do)
    except FileNotFoundError:
        raise HTTPException(404, f"not found: {req.path}")
    except PermissionError as e:
        raise HTTPException(403, str(e))
    except OSError as e:
        raise HTTPException(400, str(e))
    return {"ok": True, "path": req.path, "uid": req.uid, "gid": req.gid, "follow": req.follow}


@app.post("/api/fs/utimes")
async def fs_utimes(req: UtimesRequest):
    """Set atime/mtime (epoch ms on the wire). follow=true→os.utimes,
    follow=false→os.utime(..., follow_symlinks=False) (lutimes)."""
    def _do():
        if not os.path.lexists(req.path):
            raise FileNotFoundError(req.path)
        times = (req.atime / 1000.0, req.mtime / 1000.0)
        if req.follow:
            os.utime(req.path, times)
        else:
            os.utime(req.path, times, follow_symlinks=False)
    try:
        await asyncio.to_thread(_do)
    except FileNotFoundError:
        raise HTTPException(404, f"not found: {req.path}")
    except OSError as e:
        raise HTTPException(400, str(e))
    return {"ok": True, "path": req.path}


@app.post("/api/fs/truncate")
async def fs_truncate(req: TruncateRequest):
    def _do():
        if not os.path.lexists(req.path):
            raise FileNotFoundError(req.path)
        with open(req.path, "r+b") as f:
            f.truncate(req.len)
    try:
        await asyncio.to_thread(_do)
    except FileNotFoundError:
        raise HTTPException(404, f"not found: {req.path}")
    except OSError as e:
        raise HTTPException(400, str(e))
    return {"ok": True, "path": req.path, "len": req.len}


@app.post("/api/fs/link")
async def fs_link(req: LinkRequest):
    """Create a hard link. Uses os.link directly — no shell."""
    def _do():
        if not os.path.lexists(req.existing):
            raise FileNotFoundError(req.existing)
        os.link(req.existing, req.newpath)
    try:
        await asyncio.to_thread(_do)
    except FileNotFoundError:
        raise HTTPException(404, f"not found: {req.existing}")
    except FileExistsError:
        raise HTTPException(409, f"exists: {req.newpath}")
    except OSError as e:
        raise HTTPException(400, str(e))
    return {"ok": True, "existing": req.existing, "newpath": req.newpath}


@app.post("/api/fs/realpath")
async def fs_realpath(req: RealpathRequest):
    """Resolve a path to an absolute canonical path. Uses os.path.realpath —
    no shell, so path content is never interpreted by a shell."""
    def _do():
        if not os.path.lexists(req.path):
            raise FileNotFoundError(req.path)
        return os.path.realpath(req.path)
    try:
        rp = await asyncio.to_thread(_do)
    except FileNotFoundError:
        raise HTTPException(404, f"not found: {req.path}")
    except OSError as e:
        raise HTTPException(400, str(e))
    return {"path": req.path, "realpath": rp}


@app.post("/api/fs/mkdtemp")
async def fs_mkdtemp(req: MkdtempRequest):
    """Create a unique temp directory. Uses tempfile.mkdtemp — no shell.
    Returns the full path created."""
    import tempfile
    # tempfile requires a suffix; we synthesize from prefix like mktemp(1).
    suffix = ""
    prefix = req.prefix
    # If prefix contains XXXXXX, strip it (mktemp convention) — tempfile adds
    # its own randomness.
    if prefix.endswith("XXXXXX"):
        prefix = prefix[:-6]
    def _do():
        return tempfile.mkdtemp(prefix=prefix, suffix=suffix)
    try:
        path = await asyncio.to_thread(_do)
    except OSError as e:
        raise HTTPException(400, str(e))
    return {"path": path}


@app.get("/api/fs/statfs")
async def fs_statfs(path: str = Query(...)):
    """Filesystem statistics (statvfs) for the filesystem holding `path`.
    Returns fields matching Node's fs.statfs return shape."""
    def _do():
        if not os.path.lexists(path):
            raise FileNotFoundError(path)
        sv = os.statvfs(path)
        # Node's fs.StatFs fields: type, bsize, frsize, blocks, bfree, bavail,
        # files, ffree. os.statvfs gives: f_bsize, f_frsize, f_blocks, f_bfree,
        # f_bavail, f_files, f_ffree, f_fsid, f_flag, f_namemax.
        return {
            "type": 0,                      # fs type (Linux-specific; 0 = unknown)
            "bsize": sv.f_bsize,
            "frsize": sv.f_frsize,
            "blocks": sv.f_blocks,
            "bfree": sv.f_bfree,
            "bavail": sv.f_bavail,
            "files": sv.f_files,
            "ffree": sv.f_ffree,
        }
    try:
        return await asyncio.to_thread(_do)
    except FileNotFoundError:
        raise HTTPException(404, f"not found: {path}")
    except OSError as e:
        raise HTTPException(400, str(e))


@app.get("/api/fs/list")
async def fs_list(
    path: str = Query(...),
    recursive: bool = Query(False),
):
    def _do():
        if not os.path.exists(path):
            raise HTTPException(404, f"not found: {path}")
        if not os.path.isdir(path):
            raise HTTPException(400, f"not a directory: {path}")
        entries = []
        if recursive:
            for root, dirs, files in os.walk(path):
                for name in sorted(dirs + files):
                    full = os.path.join(root, name)
                    rel = os.path.relpath(full, path)
                    try:
                        s = os.stat(full)
                        entries.append({
                            "name": rel,
                            "type": "dir" if stat.S_ISDIR(s.st_mode) else "file",
                            "size": s.st_size,
                        })
                    except Exception:
                        entries.append({"name": rel, "type": "unknown"})
        else:
            for name in sorted(os.listdir(path)):
                full = os.path.join(path, name)
                try:
                    s = os.lstat(full)
                    ftype = "dir" if stat.S_ISDIR(s.st_mode) else (
                        "symlink" if stat.S_ISLNK(s.st_mode) else "file"
                    )
                    entries.append({
                        "name": name,
                        "type": ftype,
                        "size": s.st_size,
                        "mode": oct(s.st_mode & 0o777),
                    })
                except Exception:
                    entries.append({"name": name, "type": "unknown"})
        return entries
    return await _run_fs(_do)


@app.get("/api/fs/glob")
async def fs_glob(
    pattern: str = Query(...),
    cwd: Optional[str] = Query(None),
    recursive: bool = Query(True),
):
    """Glob-match paths. Uses Python's glob.glob — no shell, so pattern content
    is never interpreted by a shell. Supports *, **, ?, [...].

    Returns [{path, type}] where:
      - path is relative to cwd when cwd is given (else as glob returns it,
        relative to the server's cwd),
      - type is from lstat (dir/symlink/file), for the client to build Dirents.

    `recursive=True` enables ** to cross directory separators. The client
    applies exclude/includeHidden/caseSensitive/deep filtering itself.
    """
    import glob as pyglob

    def _do():
        if not pattern:
            return []
        kwargs = {"recursive": recursive}
        if cwd:
            kwargs["root_dir"] = cwd
        try:
            matches = pyglob.glob(pattern, **kwargs)
        except Exception as e:
            raise HTTPException(400, str(e))
        base = cwd or os.getcwd()
        result = []
        for m in matches:
            full = m if os.path.isabs(m) else os.path.join(base, m)
            try:
                s = os.lstat(full)
                if stat.S_ISDIR(s.st_mode):
                    t = "dir"
                elif stat.S_ISLNK(s.st_mode):
                    t = "symlink"
                else:
                    t = "file"
            except OSError:
                t = "file"
            result.append({"path": m, "type": t})
        return result

    return await asyncio.to_thread(_do)


# ─── Batch operations ───────────────────────────────────────────────────
MAX_BATCH_OPS = 256
MAX_BATCH_READ_FILE = 1 << 20  # 1 MB


def _batch_err_result(exc: Exception) -> dict:
    if isinstance(exc, FileNotFoundError):
        return {"status": 404, "body": {"error": "not found"}}
    if isinstance(exc, PermissionError):
        return {"status": 403, "body": {"error": "permission denied"}}
    return {"status": 400, "body": {"error": str(exc)}}


def _batch_stat(op: BatchOp) -> dict:
    if op.op == "lstat" or not op.follow:
        st = os.lstat(op.path)
    else:
        st = os.stat(op.path)
    return {"status": 200, "body": _stat_to_dict(st)}


def _batch_readdir(op: BatchOp) -> dict:
    info = os.stat(op.path)
    if not stat.S_ISDIR(info.st_mode):
        return {"status": 400, "body": {"error": f"not a directory: {op.path}"}}
    entries = []
    if op.recursive:
        for root, dirs, files in os.walk(op.path):
            dirs.sort()
            files.sort()
            for name in dirs + files:
                full = os.path.join(root, name)
                rel = os.path.relpath(full, op.path)
                try:
                    st = os.stat(full)
                    t = "dir" if stat.S_ISDIR(st.st_mode) else "file"
                    entries.append({"name": rel, "type": t, "size": st.st_size})
                except OSError:
                    entries.append({"name": rel, "type": "unknown"})
    else:
        for name in sorted(os.listdir(op.path)):
            full = os.path.join(op.path, name)
            try:
                st = os.lstat(full)
                if stat.S_ISDIR(st.st_mode):
                    t = "dir"
                elif stat.S_ISLNK(st.st_mode):
                    t = "symlink"
                else:
                    t = "file"
                entries.append({"name": name, "type": t, "size": st.st_size, "mode": oct(st.st_mode & 0o777)})
            except OSError:
                entries.append({"name": name, "type": "unknown"})
    return {"status": 200, "body": entries}


def _batch_read_file(op: BatchOp) -> dict:
    info = os.stat(op.path)
    if stat.S_ISDIR(info.st_mode):
        return {"status": 400, "body": {"error": f"is a directory: {op.path}"}}
    if info.st_size > MAX_BATCH_READ_FILE:
        return {"status": 413, "body": {"error": f"file too large for batch: max {MAX_BATCH_READ_FILE} bytes"}}
    if not stat.S_ISREG(info.st_mode):
        try:
            fd = os.open(op.path, os.O_RDONLY | os.O_NONBLOCK)
            try:
                data = os.read(fd, MAX_BATCH_READ_FILE)
            finally:
                os.close(fd)
        except OSError:
            data = b""
        return {"status": 200, "body": base64.b64encode(data).decode("ascii")}
    with open(op.path, "rb") as f:
        data = f.read()
    return {"status": 200, "body": base64.b64encode(data).decode("ascii")}


def _batch_access(op: BatchOp) -> dict:
    os.lstat(op.path)  # raises FileNotFoundError if missing
    mode_int = 0
    if op.mode:
        try:
            mode_int = int(op.mode, 8)
        except ValueError:
            pass
    if mode_int and not os.access(op.path, mode_int):
        return {"status": 403, "body": {"error": f"access denied: {op.path}"}}
    return {"status": 200, "body": None}


def _batch_write(op: BatchOp) -> dict:
    if op.encoding == "base64":
        content = base64.b64decode(op.content)
    else:
        content = op.content.encode()
    Path(op.path).parent.mkdir(parents=True, exist_ok=True)
    with open(op.path, "wb") as f:
        f.write(content)
    if op.mode:
        os.chmod(op.path, int(op.mode, 8))
    return {"status": 200, "body": None}


def _batch_delete(op: BatchOp) -> dict:
    if os.path.isdir(op.path) and not os.path.islink(op.path):
        shutil.rmtree(op.path)
    else:
        os.remove(op.path)
    return {"status": 200, "body": None}


def _batch_mkdir(op: BatchOp) -> dict:
    os.makedirs(op.path, exist_ok=True)
    if op.mode:
        os.chmod(op.path, int(op.mode, 8))
    return {"status": 200, "body": None}


def _batch_move(op: BatchOp) -> dict:
    try:
        os.rename(op.path, op.dst)
    except OSError:
        shutil.move(op.path, op.dst)
    return {"status": 200, "body": None}


def _batch_copy(op: BatchOp) -> dict:
    if os.path.isdir(op.path):
        shutil.copytree(op.path, op.dst)
    else:
        shutil.copy2(op.path, op.dst)
    return {"status": 200, "body": None}


_BATCH_DISPATCH = {
    "stat": _batch_stat,
    "lstat": _batch_stat,
    "readdir": _batch_readdir,
    "readFile": _batch_read_file,
    "access": _batch_access,
    "write": _batch_write,
    "delete": _batch_delete,
    "mkdir": _batch_mkdir,
    "move": _batch_move,
    "copy": _batch_copy,
}


@app.post("/api/fs/batch")
async def fs_batch(req: BatchRequest):
    if len(req.ops) > MAX_BATCH_OPS:
        raise HTTPException(413, f"batch too large: max {MAX_BATCH_OPS} ops")

    def _do():
        results = []
        for op in req.ops:
            handler = _BATCH_DISPATCH.get(op.op)
            if handler is None:
                results.append({"status": 400, "body": {"error": f"unknown op: {op.op}"}})
                continue
            try:
                results.append(handler(op))
            except Exception as e:
                results.append(_batch_err_result(e))
        return results

    results = await asyncio.to_thread(_do)
    return {"results": results}


# ─── Patch ───────────────────────────────────────────────────────────────
@app.post("/api/fs/patch")
async def fs_patch(req: PatchRequest):
    dirpath = "/"  # use root so absolute paths in diff work directly

    proc = await asyncio.create_subprocess_exec(
        "patch", "-p1",
        cwd=dirpath,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate(input=req.patch.encode())
    if proc.returncode != 0:
        raise HTTPException(400, f"patch failed: {stderr.decode(errors='replace')}")
    return {"ok": True, "output": stdout.decode(errors="replace")}


# ─── Utility ─────────────────────────────────────────────────────────────
@app.get("/api/which")
async def which(cmd: str = Query(...)):
    result = shutil.which(cmd)
    return {"cmd": cmd, "path": result}


@app.get("/api/env")
async def get_env():
    safe_env = {k: v for k, v in os.environ.items() if not k.startswith("REMOTE_OPS_")}
    return {
        "python": sys.version,
        "platform": platform.platform(),
        "hostname": socket.gethostname(),
        "cwd": os.getcwd(),
        "uid": os.getuid() if hasattr(os, "getuid") else "n/a",
        "env": safe_env,
    }


# ─── Main ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"remote-ops-server starting on {HOST}:{PORT}")
    print(f"Auth token: {API_TOKEN}")
    print(f"Debug mode: {DEBUG}")

    # For IPv6 hosts (::), create a dual-stack socket that also accepts IPv4.
    # Python/asyncio defaults to IPV6_V6ONLY=1 on some systems (WSL2), which
    # rejects IPv4 connections. Explicitly setting IPV6_V6ONLY=0 enables both.
    socks = None
    if ":" in HOST:
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((HOST, PORT))
        sock.listen(128)
        socks = [sock]

    config = uvicorn.Config(app, host=HOST, port=PORT, log_level="info", access_log=False,
                            timeout_graceful_shutdown=5)
    try:
        uvicorn.Server(config).run(sockets=socks)
    except KeyboardInterrupt:
        pass
