package main

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"syscall"
)

const maxBatchOps = 256
const maxBatchReadFile = 1 << 20 // 1MB

type batchOp struct {
	Op        string `json:"op"`
	Path      string `json:"path"`
	Dst       string `json:"dst"`
	Follow    bool   `json:"follow"`
	Recursive bool   `json:"recursive"`
	Content   string `json:"content"`
	Encoding  string `json:"encoding"`
	Mode      string `json:"mode"`
}

// fsBatch — POST /api/fs/batch
// Request:  {"ops": [{op, path, ...}]}
// Response: {"results": [{status, body}]}  — always 1:1 with request ops.
// Server returns 200 if the request body parsed; per-item failures are in each result's status.
func fsBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Ops []batchOp `json:"ops"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if len(req.Ops) > maxBatchOps {
		writeJSON(w, 413, map[string]any{"error": fmt.Sprintf("batch too large: max %d ops", maxBatchOps)})
		return
	}

	results := make([]map[string]any, 0, len(req.Ops))
	for _, op := range req.Ops {
		results = append(results, batchDispatch(op))
	}
	writeJSON(w, 200, map[string]any{"results": results})
}

// batchDispatch routes one op to its handler. Always returns a result (never panics out).
func batchDispatch(op batchOp) (result map[string]any) {
	defer func() {
		if rv := recover(); rv != nil {
			result = map[string]any{"status": 500, "body": map[string]any{"error": fmt.Sprintf("internal error: %v", rv)}}
		}
	}()

	switch op.Op {
	case "stat", "lstat":
		return batchStat(op)
	case "readdir":
		return batchReaddir(op)
	case "readFile":
		return batchReadFile(op)
	case "access":
		return batchAccess(op)
	case "write":
		return batchWrite(op)
	case "delete":
		return batchDelete(op)
	case "mkdir":
		return batchMkdir(op)
	case "move":
		return batchMove(op)
	case "copy":
		return batchCopy(op)
	default:
		return map[string]any{"status": 400, "body": map[string]any{"error": "unknown op: " + op.Op}}
	}
}

func batchErrResult(err error) map[string]any {
	if os.IsNotExist(err) {
		return map[string]any{"status": 404, "body": map[string]any{"error": "not found"}}
	}
	if os.IsPermission(err) {
		return map[string]any{"status": 403, "body": map[string]any{"error": "permission denied"}}
	}
	return map[string]any{"status": 400, "body": map[string]any{"error": err.Error()}}
}

func batchStat(op batchOp) map[string]any {
	var info map[string]any
	var err error
	if op.Op == "lstat" || !op.Follow {
		info, err = lstatToDict(op.Path)
	} else {
		info, err = statToDictFollow(op.Path)
	}
	if err != nil {
		return batchErrResult(err)
	}
	return map[string]any{"status": 200, "body": info}
}

func batchReaddir(op batchOp) map[string]any {
	info, err := os.Stat(op.Path)
	if err != nil {
		return batchErrResult(err)
	}
	if !info.IsDir() {
		return map[string]any{"status": 400, "body": map[string]any{"error": "not a directory: " + op.Path}}
	}
	entries := []map[string]any{}
	if op.Recursive {
		filepath.WalkDir(op.Path, func(full string, d os.DirEntry, err error) error {
			if full == op.Path || err != nil {
				return nil
			}
			rel, _ := filepath.Rel(op.Path, full)
			if st, err := os.Stat(full); err == nil {
				t := "file"
				if st.IsDir() {
					t = "dir"
				}
				entries = append(entries, map[string]any{"name": rel, "type": t, "size": st.Size()})
			} else {
				entries = append(entries, map[string]any{"name": rel, "type": "unknown"})
			}
			return nil
		})
	} else {
		names, _ := os.ReadDir(op.Path)
		sort.Slice(names, func(i, j int) bool { return names[i].Name() < names[j].Name() })
		for _, n := range names {
			full := filepath.Join(op.Path, n.Name())
			st, err := os.Lstat(full)
			if err != nil {
				entries = append(entries, map[string]any{"name": n.Name(), "type": "unknown"})
				continue
			}
			t := "file"
			if st.IsDir() {
				t = "dir"
			} else if st.Mode()&os.ModeSymlink != 0 {
				t = "symlink"
			}
			entries = append(entries, map[string]any{"name": n.Name(), "type": t, "size": st.Size(), "mode": formatOctal(st.Mode() & 0777)})
		}
	}
	return map[string]any{"status": 200, "body": entries}
}

func batchReadFile(op batchOp) map[string]any {
	info, err := os.Stat(op.Path)
	if err != nil {
		return batchErrResult(err)
	}
	if info.IsDir() {
		return map[string]any{"status": 400, "body": map[string]any{"error": "is a directory: " + op.Path}}
	}
	if info.Size() > maxBatchReadFile {
		return map[string]any{"status": 413, "body": map[string]any{"error": fmt.Sprintf("file too large for batch: max %d bytes", maxBatchReadFile)}}
	}
	if !info.Mode().IsRegular() {
		// Non-regular file — syscall.Open + syscall.Read (raw) to avoid blocking.
		fd, err := syscall.Open(op.Path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			return batchErrResult(err)
		}
		defer syscall.Close(fd)
		buf := make([]byte, maxBatchReadFile)
		n, _ := syscall.Read(fd, buf)
		return map[string]any{"status": 200, "body": base64.StdEncoding.EncodeToString(buf[:n])}
	}
	data, err := os.ReadFile(op.Path)
	if err != nil {
		return batchErrResult(err)
	}
	return map[string]any{"status": 200, "body": base64.StdEncoding.EncodeToString(data)}
}

func batchAccess(op batchOp) map[string]any {
	if _, err := os.Lstat(op.Path); err != nil {
		return batchErrResult(err)
	}
	modeInt := 0
	if op.Mode != "" {
		for _, c := range op.Mode {
			if c >= '0' && c <= '7' {
				modeInt = modeInt*8 + int(c-'0')
			}
		}
	}
	if modeInt != 0 {
		if err := syscall.Access(op.Path, uint32(modeInt)); err != nil {
			return map[string]any{"status": 403, "body": map[string]any{"error": "access denied: " + op.Path}}
		}
	}
	return map[string]any{"status": 200, "body": nil}
}

func batchWrite(op batchOp) map[string]any {
	var content []byte
	if op.Encoding == "base64" {
		var err error
		content, err = base64.StdEncoding.DecodeString(op.Content)
		if err != nil {
			return map[string]any{"status": 400, "body": map[string]any{"error": err.Error()}}
		}
	} else {
		content = []byte(op.Content)
	}
	os.MkdirAll(filepath.Dir(op.Path), 0755)
	if err := os.WriteFile(op.Path, content, 0644); err != nil {
		return batchErrResult(err)
	}
	if op.Mode != "" {
		if m, err := parseOctal(op.Mode); err == nil {
			os.Chmod(op.Path, m)
		}
	}
	return map[string]any{"status": 200, "body": nil}
}

func batchDelete(op batchOp) map[string]any {
	if err := os.RemoveAll(op.Path); err != nil {
		return batchErrResult(err)
	}
	return map[string]any{"status": 200, "body": nil}
}

func batchMkdir(op batchOp) map[string]any {
	if err := os.MkdirAll(op.Path, 0755); err != nil {
		return batchErrResult(err)
	}
	if op.Mode != "" {
		if m, err := parseOctal(op.Mode); err == nil {
			os.Chmod(op.Path, m)
		}
	}
	return map[string]any{"status": 200, "body": nil}
}

func batchMove(op batchOp) map[string]any {
	if err := os.Rename(op.Path, op.Dst); err != nil {
		return batchErrResult(err)
	}
	return map[string]any{"status": 200, "body": nil}
}

func batchCopy(op batchOp) map[string]any {
	if err := copyAll(op.Path, op.Dst); err != nil {
		return batchErrResult(err)
	}
	return map[string]any{"status": 200, "body": nil}
}