package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

func execStream(w http.ResponseWriter, r *http.Request) {
	var req execRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}

	c := buildCmd(&req)
	stdoutPipe, err := c.StdoutPipe()
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	stderrPipe, err := c.StderrPipe()
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}
	stdinPipe, err := c.StdinPipe()
	if err != nil {
		writeJSON(w, 500, map[string]any{"error": err.Error()})
		return
	}

	if err := c.Start(); err != nil {
		sseError(w, fmt.Sprintf("spawn %s ENOENT", req.Cmd), "ENOENT")
		sseExit(w, -2, "", 0)
		return
	}

	pid := c.Process.Pid
	e := procRegister(pid, req.Cmd, c.Process, stdinPipe)
	defer procUnregister(pid)
	defer close(e.done)

	flusher, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(200)

	sseWrite := func(v any) {
		data, _ := json.Marshal(v)
		fmt.Fprintf(w, "data: %s\n\n", data)
		if flusher != nil {
			flusher.Flush()
		}
	}

	sseWrite(map[string]any{"type": "pid", "pid": pid})

	if req.Stdin != nil {
		stdinPipe.Write([]byte(*req.Stdin))
		stdinPipe.Close()
	}

	queue := make(chan any, 16)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		buf := make([]byte, chunkSize)
		for {
			n, err := stdoutPipe.Read(buf)
			if n > 0 {
				queue <- map[string]any{"type": "stdout", "data": string(buf[:n])}
			}
			if err != nil {
				break
			}
		}
		queue <- nil
	}()

	go func() {
		defer wg.Done()
		buf := make([]byte, chunkSize)
		for {
			n, err := stderrPipe.Read(buf)
			if n > 0 {
				queue <- map[string]any{"type": "stderr", "data": string(buf[:n])}
			}
			if err != nil {
				break
			}
		}
		queue <- nil
	}()

	start := time.Now()
	deadline := time.Time{}
	if req.Timeout > 0 {
		deadline = start.Add(time.Duration(req.Timeout) * time.Second)
	}

	done := 0
	timedOut := false
	clientGone := r.Context().Done()

	for done < 2 {
		var timeout <-chan time.Time
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				timedOut = true
				break
			}
			if remaining < 15*time.Second {
				timeout = time.After(remaining)
			} else {
				timeout = time.After(15 * time.Second)
			}
		} else {
			timeout = time.After(15 * time.Second)
		}

		select {
		case <-clientGone:
			killProcGroup(c.Process)
			return
		case <-timeout:
			if !deadline.IsZero() && time.Until(deadline) <= 0 {
				timedOut = true
				break
			}
			fmt.Fprintf(w, ": keepalive\n\n")
			if flusher != nil {
				flusher.Flush()
			}
		case item := <-queue:
			if item == nil {
				done++
			} else {
				procTouch(pid)
				sseWrite(item)
			}
		}
	}

	if timedOut {
		killProcGroup(c.Process)
	}

	wg.Wait()
	c.Wait()

	exitCode := 0
	if c.ProcessState != nil {
		exitCode = c.ProcessState.ExitCode()
	}

	sig := ""
	if timedOut {
		sig = "SIGKILL"
	}
	sseWrite(map[string]any{"type": "exit", "code": exitCode, "signal": sig, "duration_ms": msSince(start)})
}

func sseError(w http.ResponseWriter, data, code string) {
	flusher, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(200)
	v, _ := json.Marshal(map[string]any{"type": "error", "data": data, "code": code})
	fmt.Fprintf(w, "data: %s\n\n", v)
	if flusher != nil {
		flusher.Flush()
	}
}

func sseExit(w http.ResponseWriter, code int, signal string, durationMs int) {
	flusher, _ := w.(http.Flusher)
	v, _ := json.Marshal(map[string]any{"type": "exit", "code": code, "signal": signal, "duration_ms": durationMs})
	fmt.Fprintf(w, "data: %s\n\n", v)
	if flusher != nil {
		flusher.Flush()
	}
}
