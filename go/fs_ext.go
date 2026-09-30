package main

import (
	"encoding/base64"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

func fsStatfs(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if _, err := os.Lstat(path); err != nil && os.IsNotExist(err) {
		writeJSON(w, 404, map[string]any{"error": "not found: " + path})
		return
	}
	var sv syscall.Statfs_t
	if err := syscall.Statfs(path, &sv); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"type": 0, "bsize": sv.Bsize, "frsize": sv.Frsize, "blocks": sv.Blocks, "bfree": sv.Bfree, "bavail": sv.Bavail, "files": sv.Files, "ffree": sv.Ffree})
}

func fsList(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	recursive := queryBool(r, "recursive", false)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, 404, map[string]any{"error": "not found: " + path})
			return
		}
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if !info.IsDir() {
		writeJSON(w, 400, map[string]any{"error": "not a directory: " + path})
		return
	}
	entries := []map[string]any{}
	if recursive {
		filepath.WalkDir(path, func(full string, d os.DirEntry, err error) error {
			if full == path || err != nil {
				return nil
			}
			rel, _ := filepath.Rel(path, full)
			if info, err := os.Stat(full); err == nil {
				t := "file"
				if info.IsDir() {
					t = "dir"
				}
				entries = append(entries, map[string]any{"name": rel, "type": t, "size": info.Size()})
			} else {
				entries = append(entries, map[string]any{"name": rel, "type": "unknown"})
			}
			return nil
		})
	} else {
		names, _ := os.ReadDir(path)
		sort.Slice(names, func(i, j int) bool { return names[i].Name() < names[j].Name() })
		for _, n := range names {
			full := filepath.Join(path, n.Name())
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
	writeJSON(w, 200, entries)
}

func fsGlob(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	cwd := r.URL.Query().Get("cwd")
	recursive := queryBool(r, "recursive", true)
	base := cwd
	if base == "" {
		base, _ = os.Getwd()
	}
	var matches []string
	if recursive && strings.Contains(pattern, "**") {
		matches = globRecursive(pattern, base)
	} else {
		full := pattern
		if !filepath.IsAbs(full) {
			full = filepath.Join(base, pattern)
		}
		matches, _ = filepath.Glob(full)
	}
	result := []map[string]any{}
	for _, m := range matches {
		st, err := os.Lstat(m)
		if err != nil {
			continue
		}
		t := "file"
		if st.IsDir() {
			t = "dir"
		} else if st.Mode()&os.ModeSymlink != 0 {
			t = "symlink"
		}
		rel, _ := filepath.Rel(base, m)
		result = append(result, map[string]any{"path": rel, "type": t})
	}
	writeJSON(w, 200, result)
}

func globRecursive(pattern, base string) []string {
	var result []string
	root := base
	idx := strings.Index(pattern, "**")
	if idx >= 0 {
		rootPart := strings.TrimSuffix(pattern[:idx], "/")
		if rootPart != "" {
			root = filepath.Join(base, rootPart)
		}
	}
	filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, err := filepath.Rel(base, path)
		if err != nil || rel == "." {
			return nil
		}
		if globMatch(pattern, rel) {
			result = append(result, path)
		}
		return nil
	})
	return result
}

func globMatch(pattern, name string) bool {
	idx := strings.Index(pattern, "**")
	if idx < 0 {
		matched, _ := filepath.Match(pattern, name)
		return matched
	}
	prefix := strings.TrimSuffix(pattern[:idx], "/")
	suffix := strings.TrimPrefix(pattern[idx+2:], "/")
	if prefix != "" {
		if name == prefix {
			return suffix == ""
		}
		if !strings.HasPrefix(name, prefix+"/") {
			return false
		}
		name = name[len(prefix)+1:]
	}
	if suffix == "" {
		return true
	}
	parts := strings.Split(name, "/")
	for i := 0; i < len(parts); i++ {
		sub := strings.Join(parts[i:], "/")
		if matched, _ := filepath.Match(suffix, sub); matched {
			return true
		}
	}
	return false
}

func fsBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Ops []struct {
			Op       string `json:"op"`
			Path     string `json:"path"`
			Dst      string `json:"dst"`
			Content  string `json:"content"`
			Encoding string `json:"encoding"`
			Mode     string `json:"mode"`
		} `json:"ops"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	results := []map[string]any{}
	for _, op := range req.Ops {
		switch op.Op {
		case "write":
			var content []byte
			if op.Encoding == "base64" {
				content, _ = base64.StdEncoding.DecodeString(op.Content)
			} else {
				content = []byte(op.Content)
			}
			os.MkdirAll(filepath.Dir(op.Path), 0755)
			if err := os.WriteFile(op.Path, content, 0644); err != nil {
				results = append(results, map[string]any{"op": op.Op, "path": op.Path, "ok": false, "error": err.Error()})
				continue
			}
			if op.Mode != "" {
				m, mErr := parseOctal(op.Mode)
				if mErr == nil {
					os.Chmod(op.Path, m)
				}
			}
			results = append(results, map[string]any{"op": op.Op, "path": op.Path, "ok": true})
		case "delete":
			os.RemoveAll(op.Path)
			results = append(results, map[string]any{"op": op.Op, "path": op.Path, "ok": true})
		case "mkdir":
			os.MkdirAll(op.Path, 0755)
			if op.Mode != "" {
				m, mErr := parseOctal(op.Mode)
				if mErr == nil {
					os.Chmod(op.Path, m)
				}
			}
			results = append(results, map[string]any{"op": op.Op, "path": op.Path, "ok": true})
		case "move":
			if err := os.Rename(op.Path, op.Dst); err != nil {
				results = append(results, map[string]any{"op": op.Op, "path": op.Path, "ok": false, "error": err.Error()})
			} else {
				results = append(results, map[string]any{"op": op.Op, "path": op.Path, "ok": true})
			}
		case "copy":
			if err := copyAll(op.Path, op.Dst); err != nil {
				results = append(results, map[string]any{"op": op.Op, "path": op.Path, "ok": false, "error": err.Error()})
			} else {
				results = append(results, map[string]any{"op": op.Op, "path": op.Path, "ok": true})
			}
		default:
			results = append(results, map[string]any{"op": op.Op, "path": op.Path, "ok": false, "error": "unknown op: " + op.Op})
		}
	}
	writeJSON(w, 200, map[string]any{"results": results})
}

func fsPatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Patch string `json:"patch"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	c := exec.Command("patch", "-p1")
	c.Dir = "/"
	c.Stdin = strings.NewReader(req.Patch)
	var stdout, stderr strings.Builder
	c.Stdout = &stdout
	c.Stderr = &stderr
	if err := c.Run(); err != nil {
		writeJSON(w, 400, map[string]any{"error": "patch failed: " + stderr.String()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "output": stdout.String()})
}

func whichHandler(w http.ResponseWriter, r *http.Request) {
	cmd := r.URL.Query().Get("cmd")
	path, err := exec.LookPath(cmd)
	if err != nil {
		path = ""
	}
	writeJSON(w, 200, map[string]any{"cmd": cmd, "path": path})
}

func envHandler(w http.ResponseWriter, r *http.Request) {
	env := map[string]string{}
	for _, e := range os.Environ() {
		parts := strings.SplitN(e, "=", 2)
		if len(parts) == 2 && !strings.HasPrefix(parts[0], "REMOTE_OPS_") {
			env[parts[0]] = parts[1]
		}
	}
	writeJSON(w, 200, map[string]any{"go": "1.24", "platform": "linux", "hostname": hostname(), "cwd": mustGetwd(), "uid": os.Getuid(), "env": env})
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}

func mustGetwd() string {
	wd, _ := os.Getwd()
	return wd
}
