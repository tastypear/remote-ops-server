package main

import (
	"encoding/base64"
	"encoding/json"
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
		if c.SysProcAttr == nil {
			c.SysProcAttr = &syscall.SysProcAttr{}
		}
		c.SysProcAttr.Setpgid = true
		m, err := pty.Start(c)
		if err != nil {
			ws.WriteJSON(map[string]any{"type": "error", "data": "spawn " + req.Cmd + " ENOENT", "code": "ENOENT"})
			ws.WriteJSON(map[string]any{"type": "exit", "code": -2, "signal": nil, "duration_ms": 0})
			return
		}
		master = m
		rows := intVal(msg, "rows", 24)
		cols := intVal(msg, "cols", 80)
		pty.Setsize(master, &pty.Winsize{Rows: uint16(rows), Cols: uint16(cols)})
	} else {
		c = buildCmd(&req)
		c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		sw, err := c.StdinPipe()
		if err != nil {
			ws.WriteJSON(map[string]any{"type": "error", "data": err.Error()})
			return
		}
		stdinWriter = sw
		stdoutPipe, _ = c.StdoutPipe()
		stderrPipe, _ = c.StderrPipe()
		if err := c.Start(); err != nil {
			ws.WriteJSON(map[string]any{"type": "error", "data": "spawn " + req.Cmd + " ENOENT", "code": "ENOENT"})
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

	numReaders := 2
	if usePty {
		numReaders = 1
		go ptyReader(master, queue, useBinary)
	} else {
		go pipeReader(stdoutPipe, "stdout", queue, useBinary)
		go pipeReader(stderrPipe, "stderr", queue, useBinary)
	}

	stopStdin := make(chan struct{})
	go wsStdinWriter(ws, c, master, stdinWriter, usePty, pid, detachable, stopStdin)

	deadline := time.Time{}
	if req.Timeout > 0 {
		deadline = start.Add(time.Duration(req.Timeout) * time.Second)
	}
	timedOut := false
	done := 0

	for done < numReaders {
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
		case <-timeout:
			if !deadline.IsZero() && time.Until(deadline) <= 0 {
				timedOut = true
				break
			}
			ws.WriteJSON(map[string]any{"type": "keepalive"})
		case item := <-queue:
			if item == nil {
				done++
			} else {
				procTouch(pid)
				switch v := item.(type) {
				case []byte:
					ws.WriteMessage(websocket.BinaryMessage, v)
				default:
					ws.WriteJSON(v)
				}
			}
		}
	}

	if timedOut {
		killProcGroup(c.Process)
	}
	close(stopStdin)
	c.Wait()

	if detachable && !timedOut {
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

func wsStdinWriter(ws *websocket.Conn, c *exec.Cmd, master *os.File, stdin io.WriteCloser, usePty bool, pid int, detachable bool, stop chan struct{}) {
	for {
		_, raw, err := ws.ReadMessage()
		if err != nil {
			if !detachable {
				killProcGroup(c.Process)
			}
			return
		}
		var msg map[string]any
		if json.Unmarshal(raw, &msg) != nil {
			continue
		}
		switch msg["type"] {
		case "stdin":
			data, _ := msg["data"].(string)
			var bytes []byte
			if msg["encoding"] == "base64" {
				bytes, _ = base64.StdEncoding.DecodeString(data)
			} else {
				bytes = []byte(data)
			}
			if usePty {
				master.Write(bytes)
			} else if stdin != nil {
				stdin.Write(bytes)
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
