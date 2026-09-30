package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
)

var wsUpgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
var drainTasks sync.WaitGroup

func spawnErrCode(err error) string {
	if errors.Is(err, exec.ErrNotFound) || os.IsNotExist(err) {
		return "ENOENT"
	}
	if os.IsPermission(err) {
		return "EACCES"
	}
	return "ENOENT"
}

func wsExec(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	token := auth
	if len(auth) > 7 && auth[:7] == "Bearer " {
		token = auth[7:]
	}
	if token == "" {
		token = r.URL.Query().Get("token")
	}
	if token != apiToken {
		w.WriteHeader(401)
		return
	}

	wsConnCount.Lock()
	if wsConnCount.n >= wsMaxConn {
		wsConnCount.Unlock()
		w.WriteHeader(429)
		return
	}
	wsConnCount.n++
	wsConnCount.Unlock()
	defer func() {
		wsConnCount.Lock()
		wsConnCount.n--
		wsConnCount.Unlock()
	}()

	ws, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()

	_, raw, err := ws.ReadMessage()
	if err != nil {
		return
	}
	var msg map[string]any
	if json.Unmarshal(raw, &msg) != nil {
		ws.WriteJSON(map[string]any{"type": "error", "data": "invalid start"})
		return
	}
	if msg["type"] != "start" {
		ws.WriteJSON(map[string]any{"type": "error", "data": "expected start"})
		return
	}

	req := parseExecMsg(msg)
	usePty := boolVal(msg, "pty")
	detachable := boolVal(msg, "detach")
	useBinary := boolVal(msg, "binaryFrames")

	var c *exec.Cmd
	var master *os.File
	var stdinWriter io.WriteCloser
	var stdoutPipe, stderrPipe io.ReadCloser

	if usePty {
		c = buildCmd(&req)
		// Preserve credential (uid/gid) from buildCmd; PTY needs Setsid.
		cred := c.SysProcAttr.Credential
		c.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Credential: cred}
		m, err := pty.Start(c)
		if err != nil {
			code := spawnErrCode(err)
			ws.WriteJSON(map[string]any{"type": "error", "data": "spawn " + req.Cmd + " " + code, "code": code})
			ws.WriteJSON(map[string]any{"type": "exit", "code": -2, "signal": nil, "duration_ms": 0})
			return
		}
		master = m
		pty.Setsize(master, &pty.Winsize{Rows: uint16(intVal(msg, "rows", 24)), Cols: uint16(intVal(msg, "cols", 80))})
	} else {
		c = buildCmd(&req)
		stdinWriter, err = c.StdinPipe()
		if err != nil {
			ws.WriteJSON(map[string]any{"type": "error", "data": err.Error()})
			return
		}
		stdoutPipe, _ = c.StdoutPipe()
		stderrPipe, _ = c.StderrPipe()
		if err := c.Start(); err != nil {
			code := spawnErrCode(err)
			ws.WriteJSON(map[string]any{"type": "error", "data": "spawn " + req.Cmd + " " + code, "code": code})
			ws.WriteJSON(map[string]any{"type": "exit", "code": -2, "signal": nil, "duration_ms": 0})
			return
		}
	}

	pid := c.Process.Pid
	e := procRegister(pid, req.Cmd, c.Process, nil)
	ws.WriteJSON(map[string]any{"type": "pid", "pid": pid})

	if req.Stdin != nil {
		if usePty {
			master.Write([]byte(*req.Stdin))
		} else {
			stdinWriter.Write([]byte(*req.Stdin))
		}
	}

	start := time.Now()
	queue := make(chan any, 32)
	disconnect := make(chan struct{})

	numReaders := 2
	if usePty {
		numReaders = 1
		go ptyReader(master, queue, useBinary)
	} else {
		go pipeReader(stdoutPipe, "stdout", queue, useBinary)
		go pipeReader(stderrPipe, "stderr", queue, useBinary)
	}

	go wsStdinWriter(ws, c, master, stdinWriter, usePty, pid, detachable, disconnect)

	deadline := time.Time{}
	if req.Timeout > 0 {
		deadline = start.Add(time.Duration(req.Timeout) * time.Second)
	}
	timedOut := false
	done := 0
	clientGone := false

	for done < numReaders && !clientGone {
		var timeout <-chan time.Time
		if !deadline.IsZero() {
			rem := time.Until(deadline)
			if rem <= 0 {
				timedOut = true
				break
			}
			if rem < 15*time.Second {
				timeout = time.After(rem)
			} else {
				timeout = time.After(15 * time.Second)
			}
		} else {
			timeout = time.After(15 * time.Second)
		}

		select {
		case <-disconnect:
			clientGone = true
		case <-timeout:
			if !deadline.IsZero() && time.Until(deadline) <= 0 {
				timedOut = true
			} else if err := ws.WriteJSON(map[string]any{"type": "keepalive"}); err != nil {
				clientGone = true
			}
		case item := <-queue:
			if item == nil {
				done++
			} else {
				procTouch(pid)
				switch v := item.(type) {
				case []byte:
					if err := ws.WriteMessage(websocket.BinaryMessage, v); err != nil {
						clientGone = true
					}
				default:
					if err := ws.WriteJSON(v); err != nil {
						clientGone = true
					}
				}
			}
		}
	}

	if timedOut {
		killProcGroup(c.Process)
	}

	if clientGone && detachable && !timedOut {
		drainTasks.Add(1)
		go func() {
			defer drainTasks.Done()
			if master != nil {
				io.Copy(io.Discard, master)
			}
			c.Wait()
			procUnregister(pid)
		}()
		return
	}

	if clientGone && !detachable {
		killProcGroup(c.Process)
	}

	c.Wait()
	if master != nil {
		master.Close()
	}
	if stdinWriter != nil {
		stdinWriter.Close()
	}
	procUnregister(pid)
	close(e.done)

	sig := ""
	if timedOut {
		sig = "SIGKILL"
	}
	ws.WriteJSON(map[string]any{"type": "exit", "code": exitCode(c), "signal": sig, "duration_ms": msSince(start)})
}

func parseExecMsg(msg map[string]any) execRequest {
	req := execRequest{Cwd: "/", Shell: true}
	req.Cmd, _ = msg["cmd"].(string)
	if args, ok := msg["args"].([]any); ok {
		for _, a := range args {
			if s, ok := a.(string); ok {
				req.Args = append(req.Args, s)
			}
		}
	}
	if shell, ok := msg["shell"]; ok {
		req.Shell = shell
	}
	if cwd, ok := msg["cwd"].(string); ok && cwd != "" {
		req.Cwd = cwd
	}
	if env, ok := msg["env"].(map[string]any); ok {
		req.Env = map[string]string{}
		for k, v := range env {
			if s, ok := v.(string); ok {
				req.Env[k] = s
			}
		}
	}
	if t, ok := msg["timeout"].(float64); ok {
		req.Timeout = int(t)
	}
	if stdin, ok := msg["stdin"].(string); ok && stdin != "" {
		req.Stdin = &stdin
	}
	if uid, ok := msg["uid"].(float64); ok {
		u := int(uid)
		req.UID = &u
	}
	if gid, ok := msg["gid"].(float64); ok {
		g := int(gid)
		req.GID = &g
	}
	return req
}

func boolVal(msg map[string]any, key string) bool {
	return msg[key] == true
}

func intVal(msg map[string]any, key string, def int) int {
	if v, ok := msg[key].(float64); ok {
		return int(v)
	}
	return def
}

func pipeReader(rc io.ReadCloser, kind string, queue chan any, useBinary bool) {
	buf := make([]byte, chunkSize)
	for {
		n, err := rc.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if useBinary {
				prefix := byte(0)
				if kind == "stderr" {
					prefix = 1
				}
				queue <- append([]byte{prefix}, chunk...)
			} else {
				queue <- map[string]any{"type": kind, "data": string(chunk)}
			}
		}
		if err != nil {
			break
		}
	}
	queue <- nil
}

func ptyReader(master *os.File, queue chan any, useBinary bool) {
	buf := make([]byte, chunkSize)
	for {
		n, err := master.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if useBinary {
				queue <- append([]byte{0}, chunk...)
			} else {
				queue <- map[string]any{"type": "stdout", "data": string(chunk)}
			}
		}
		if err != nil {
			break
		}
	}
	queue <- nil
}

func wsStdinWriter(ws *websocket.Conn, c *exec.Cmd, master *os.File, stdin io.WriteCloser, usePty bool, pid int, detachable bool, disconnect chan struct{}) {
	for {
		_, raw, err := ws.ReadMessage()
		if err != nil {
			close(disconnect)
			return
		}
		var msg map[string]any
		if json.Unmarshal(raw, &msg) != nil {
			continue
		}
		switch msg["type"] {
		case "stdin":
			data, _ := msg["data"].(string)
			var b []byte
			if msg["encoding"] == "base64" {
				b, _ = base64.StdEncoding.DecodeString(data)
			} else {
				b = []byte(data)
			}
			if usePty {
				master.Write(b)
			} else if stdin != nil {
				stdin.Write(b)
			}
			procTouch(pid)
		case "stdin_close":
			if !usePty && stdin != nil {
				stdin.Close()
			}
		case "kill":
			sigName, _ := msg["signal"].(string)
			if sigName == "" {
				sigName = "SIGTERM"
			}
			killProcGroupSig(c.Process, sigByName(sigName))
		case "resize":
			if usePty {
				pty.Setsize(master, &pty.Winsize{Rows: uint16(intVal(msg, "rows", 24)), Cols: uint16(intVal(msg, "cols", 80))})
			}
		}
	}
}

func exitCode(c *exec.Cmd) int {
	if c.ProcessState != nil {
		return c.ProcessState.ExitCode()
	}
	return -1
}
