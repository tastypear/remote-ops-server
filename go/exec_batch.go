package main

import (
	"bytes"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

const maxBatchCmds = 64

type batchExecRequest struct {
	Cmds    []string          `json:"cmds"`
	Mode    string            `json:"mode"`
	Cwd     string            `json:"cwd"`
	Env     map[string]string `json:"env"`
	Timeout int               `json:"timeout"`
}

// execBatch — POST /api/exec/batch
// Sequential (default): join cmds with \n, run as one sh -c, return 1 result.
// Parallel: run each cmd in its own process, return N results 1:1.
// Server always returns 200; per-command exit_code in each result.
func execBatch(w http.ResponseWriter, r *http.Request) {
	var req batchExecRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if len(req.Cmds) == 0 {
		writeJSON(w, 200, map[string]any{"results": []map[string]any{}})
		return
	}
	if len(req.Cmds) > maxBatchCmds {
		writeJSON(w, 413, map[string]any{"error": "batch too large"})
		return
	}

	if req.Mode == "parallel" {
		results := make([]map[string]any, len(req.Cmds))
		var wg sync.WaitGroup
		for i, cmd := range req.Cmds {
			wg.Add(1)
			go func(idx int, c string) {
				defer wg.Done()
				results[idx] = execOne(c, req.Cwd, req.Env, req.Timeout)
			}(i, cmd)
		}
		wg.Wait()
		writeJSON(w, 200, map[string]any{"results": results})
	} else {
		joined := strings.Join(req.Cmds, "\n")
		result := execOne(joined, req.Cwd, req.Env, req.Timeout)
		writeJSON(w, 200, map[string]any{"results": []map[string]any{result}})
	}
}

// execOne runs a single command via sh -c and returns the result.
func execOne(cmd, cwd string, env map[string]string, timeout int) map[string]any {
	start := time.Now()
	c := exec.Command("sh", "-c", cmd)
	c.Dir = safeCwd(cwd)
	c.Env = buildEnv(env)
	c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	var stdoutBuf, stderrBuf bytes.Buffer
	c.Stdout = &stdoutBuf
	c.Stderr = &stderrBuf

	if err := c.Start(); err != nil {
		return map[string]any{"stdout": "", "stderr": err.Error(), "exit_code": -1, "duration_ms": msSince(start)}
	}

	done := make(chan error, 1)
	go func() { done <- c.Wait() }()

	var err error
	if timeout > 0 {
		select {
		case err = <-done:
		case <-time.After(time.Duration(timeout) * time.Second):
			killProcGroup(c.Process)
			c.Wait()
			return map[string]any{"stdout": stdoutBuf.String(), "stderr": stderrBuf.String(), "exit_code": -1, "duration_ms": msSince(start)}
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

	return map[string]any{"stdout": stdoutBuf.String(), "stderr": stderrBuf.String(), "exit_code": exitCode, "duration_ms": msSince(start)}
}
