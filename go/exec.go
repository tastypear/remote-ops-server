package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

type execRequest struct {
	Cmd     string            `json:"cmd"`
	Args    []string          `json:"args"`
	Shell   any               `json:"shell"`
	Cwd     string            `json:"cwd"`
	Env     map[string]string `json:"env"`
	Timeout int               `json:"timeout"`
	Stdin   *string           `json:"stdin"`
	Binary  bool              `json:"binary"`
	UID     *int              `json:"uid"`
	GID     *int              `json:"gid"`
}

var sensitiveEnv = []string{"TOKEN", "SECRET", "KEY", "PASSWORD", "CREDENTIAL", "AUTH"}

func isSensitive(key string) bool {
	k := strings.ToUpper(key)
	for _, s := range sensitiveEnv {
		if strings.Contains(k, s) {
			return true
		}
	}
	return false
}

func buildEnv(reqEnv map[string]string) []string {
	env := []string{}
	for _, e := range os.Environ() {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 && !isSensitive(parts[0]) {
			env = append(env, e)
		}
	}
	for k, v := range reqEnv {
		env = append(env, k+"="+v)
	}
	return env
}

func buildCmd(req *execRequest) *exec.Cmd {
	var c *exec.Cmd
	switch v := req.Shell.(type) {
	case bool:
		if v {
			c = exec.Command("sh", "-c", req.Cmd)
		} else {
			c = exec.Command(req.Cmd, req.Args...)
		}
	case string:
		if v != "" {
			c = exec.Command(v, "-c", req.Cmd)
		} else {
			c = exec.Command(req.Cmd, req.Args...)
		}
	default:
		c = exec.Command("sh", "-c", req.Cmd)
	}
	c.Dir = safeCwd(req.Cwd)
	c.Env = buildEnv(req.Env)

	attr := &syscall.SysProcAttr{Setpgid: true}
	if req.UID != nil || req.GID != nil {
		uid, gid := uint32(0), uint32(0)
		if req.UID != nil {
			uid = uint32(*req.UID)
		}
		if req.GID != nil {
			gid = uint32(*req.GID)
		}
		attr.Credential = &syscall.Credential{Uid: uid, Gid: gid, NoSetGroups: true}
	}
	c.SysProcAttr = attr
	return c
}

func execSync(w http.ResponseWriter, r *http.Request) {
	var req execRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}

	start := time.Now()
	c := buildCmd(&req)
	var stdoutBuf, stderrBuf bytes.Buffer
	c.Stdout = &stdoutBuf
	c.Stderr = &stderrBuf
	if req.Stdin != nil {
		c.Stdin = strings.NewReader(*req.Stdin)
	}

	if err := c.Start(); err != nil {
		if errors.Is(err, exec.ErrNotFound) || os.IsNotExist(err) {
			writeJSON(w, 404, map[string]any{"error": "command not found", "error_code": "ENOENT", "cmd": req.Cmd, "stdout": "", "stderr": "", "exit_code": -2, "pid": 0, "duration_ms": msSince(start)})
			return
		}
		if os.IsPermission(err) {
			writeJSON(w, 403, map[string]any{"error": "permission denied", "error_code": "EACCES", "cmd": req.Cmd, "stdout": "", "stderr": "", "exit_code": -13, "pid": 0, "duration_ms": msSince(start)})
			return
		}
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}

	pid := c.Process.Pid
	e := procRegister(pid, req.Cmd, c.Process, nil)
	defer func() { procUnregister(pid); close(e.done) }()

	done := make(chan error, 1)
	go func() { done <- c.Wait() }()

	var err error
	if req.Timeout > 0 {
		select {
		case err = <-done:
		case <-time.After(time.Duration(req.Timeout) * time.Second):
			killProcGroup(c.Process)
			c.Wait()
			writeJSON(w, 408, map[string]any{"error": "timeout", "timeout": req.Timeout, "stdout": "", "stderr": "", "exit_code": -1, "pid": pid, "duration_ms": msSince(start)})
			return
		}
	} else {
		err = <-done
	}

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}

	resp := map[string]any{"stdout": stdoutBuf.String(), "stderr": stderrBuf.String(), "exit_code": exitCode, "pid": pid, "duration_ms": msSince(start)}
	if req.Binary {
		resp["stdout_b64"] = base64.StdEncoding.EncodeToString(stdoutBuf.Bytes())
		resp["stderr_b64"] = base64.StdEncoding.EncodeToString(stderrBuf.Bytes())
	}
	writeJSON(w, 200, resp)
}

func execKill(w http.ResponseWriter, r *http.Request) {
	pid := queryInt(r, "pid", 0)
	sigName := r.URL.Query().Get("signal_name")
	if sigName == "" {
		sigName = "SIGTERM"
	}
	e := procGet(pid)
	if e == nil {
		writeJSON(w, 404, map[string]any{"error": "no such process", "pid": pid})
		return
	}
	killProcGroupSig(e.proc, sigByName(sigName))
	writeJSON(w, 200, map[string]any{"ok": true, "pid": pid, "signal": sigName})
}

func execStatus(w http.ResponseWriter, r *http.Request) {
	pid := queryInt(r, "pid", 0)
	e := procGet(pid)
	if e == nil {
		writeJSON(w, 404, map[string]any{"error": "no such process", "pid": pid})
		return
	}
	running := true
	if err := e.proc.Signal(syscall.Signal(0)); err != nil {
		running = false
	}
	writeJSON(w, 200, map[string]any{"pid": pid, "running": running, "exit_code": -1, "cmd": e.cmd, "age_s": int(time.Since(e.spawnTime).Seconds())})
}

type stdinRequest struct {
	Pid   int    `json:"pid"`
	Data  string `json:"data"`
	Close bool   `json:"close"`
}

func execStdin(w http.ResponseWriter, r *http.Request) {
	var req stdinRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	e := procGet(req.Pid)
	if e == nil {
		writeJSON(w, 404, map[string]any{"error": "no such process", "pid": req.Pid})
		return
	}
	if e.stdin == nil {
		writeJSON(w, 409, map[string]any{"error": "process has no stdin", "pid": req.Pid})
		return
	}
	if req.Data != "" {
		e.stdin.Write([]byte(req.Data))
	}
	if req.Close {
		e.stdin.Close()
	}
	procTouch(req.Pid)
	writeJSON(w, 200, map[string]any{"ok": true, "pid": req.Pid})
}

func sigByName(name string) syscall.Signal {
	switch name {
	case "SIGKILL":
		return syscall.SIGKILL
	case "SIGINT":
		return syscall.SIGINT
	case "SIGUSR1":
		return syscall.SIGUSR1
	case "SIGUSR2":
		return syscall.SIGUSR2
	case "SIGHUP":
		return syscall.SIGHUP
	default:
		return syscall.SIGTERM
	}
}

func procSweeper() {
	for range time.Tick(60 * time.Second) {
		now := time.Now()
		procTable.Lock()
		for pid, e := range procTable.m {
			select {
			case <-e.done:
				delete(procTable.m, pid)
			default:
				if now.Sub(e.lastUse) > 300*time.Second {
					killProcGroup(e.proc)
					delete(procTable.m, pid)
				}
			}
		}
		procTable.Unlock()
	}
}

func fdSweeper() {
	for range time.Tick(60 * time.Second) {
		now := time.Now()
		fdTable.Lock()
		for fd, e := range fdTable.m {
			if now.Sub(e.lastUse) > 300*time.Second {
				e.osfd.Close()
				delete(fdTable.m, fd)
			}
		}
		fdTable.Unlock()
	}
}

func init() {
	go procSweeper()
	go fdSweeper()
}

var _ = sync.WaitGroup{}
