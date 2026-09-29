package main

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func fsError(w http.ResponseWriter, err error) {
	if os.IsNotExist(err) {
		writeJSON(w, 404, map[string]any{"error": "not found"})
	} else if os.IsExist(err) {
		writeJSON(w, 409, map[string]any{"error": "exists"})
	} else if os.IsPermission(err) {
		writeJSON(w, 403, map[string]any{"error": "permission denied"})
	} else {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
	}
}

func parseOctal(s string) (os.FileMode, error) {
	var m uint64
	for _, c := range s {
		if c >= '0' && c <= '7' {
			m = m*8 + uint64(c-'0')
		}
	}
	return os.FileMode(m), nil
}

func formatOctal(m os.FileMode) string {
	return "0o" + uintToOct(uint64(m))
}

func uintToOct(n uint64) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%8)) + s
		n /= 8
	}
	return s
}

func fsStat(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	follow := queryBool(r, "follow", true)
	var info map[string]any
	var err error
	if follow {
		info, err = statToDictFollow(path)
	} else {
		info, err = lstatToDict(path)
	}
	if err != nil {
		fsError(w, err)
		return
	}
	writeJSON(w, 200, info)
}

func fsRead(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, 404, map[string]any{"error": "not found: " + path})
			return
		}
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if info.IsDir() {
		writeJSON(w, 400, map[string]any{"error": "is a directory: " + path})
		return
	}
	f, err := os.Open(path)
	if err != nil {
		fsError(w, err)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	if r.URL.Query().Get("download") == "true" {
		w.Header().Set("Content-Disposition", "attachment; filename=\""+filepath.Base(path)+"\"")
	}
	io.Copy(w, f)
}

func fsWrite(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	mode := r.URL.Query().Get("mode")
	appendMode := queryBool(r, "append", false)
	flush := queryBool(r, "flush", false)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	os.MkdirAll(filepath.Dir(path), 0755)
	var f *os.File
	if appendMode {
		f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	} else {
		f, err = os.Create(path)
	}
	if err != nil {
		fsError(w, err)
		return
	}
	f.Write(body)
	f.Close()
	if mode != "" {
		m, _ := parseOctal(mode)
		os.Chmod(path, m)
	}
	if flush {
		if fd, err := os.Open(path); err == nil {
			fd.Sync()
			fd.Close()
		}
	}
	writeJSON(w, 200, map[string]any{"ok": true, "path": path, "bytes": len(body)})
}

func fsDelete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if _, err := os.Lstat(req.Path); err != nil && os.IsNotExist(err) {
		writeJSON(w, 404, map[string]any{"error": "not found: " + req.Path})
		return
	}
	if info, _ := os.Lstat(req.Path); info != nil && info.IsDir() {
		os.RemoveAll(req.Path)
	} else {
		os.Remove(req.Path)
	}
	writeJSON(w, 200, map[string]any{"ok": true, "path": req.Path})
}

func fsMkdir(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path      string `json:"path"`
		Mode      string `json:"mode"`
		Recursive bool   `json:"recursive"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	var err error
	if req.Recursive {
		err = os.MkdirAll(req.Path, 0755)
		if err == nil && req.Mode != "" {
			m, _ := parseOctal(req.Mode)
			os.Chmod(req.Path, m)
		}
	} else {
		m := os.FileMode(0755)
		if req.Mode != "" {
			m, _ = parseOctal(req.Mode)
		}
		err = os.Mkdir(req.Path, m)
	}
	if err != nil {
		fsError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "path": req.Path})
}

func fsMove(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Src string
		Dst string `json:"src"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if err := os.Rename(req.Src, req.Dst); err != nil {
		if err2 := copyAll(req.Src, req.Dst); err2 != nil {
			writeJSON(w, 400, map[string]any{"error": err.Error()})
			return
		}
		os.RemoveAll(req.Src)
	}
	writeJSON(w, 200, map[string]any{"ok": true, "src": req.Src, "dst": req.Dst})
}

func fsCopy(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Src string
		Dst string `json:"src"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if err := copyAll(req.Src, req.Dst); err != nil {
		fsError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "src": req.Src, "dst": req.Dst})
}

func copyAll(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return copyDir(src, dst)
	}
	return copyFile(src, dst, info)
}

func copyDir(src, dst string) error {
	os.MkdirAll(dst, 0755)
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		s := filepath.Join(src, e.Name())
		d := filepath.Join(dst, e.Name())
		if e.IsDir() {
			if err := copyDir(s, d); err != nil {
				return err
			}
		} else {
			info, _ := e.Info()
			copyFile(s, d, info)
		}
	}
	return nil
}

func copyFile(src, dst string, info os.FileInfo) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	io.Copy(out, in)
	out.Chmod(info.Mode())
	return nil
}

func fsChmod(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string
		Mode string `json:"path"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	m, _ := parseOctal(req.Mode)
	if err := os.Chmod(req.Path, m); err != nil {
		fsError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "path": req.Path, "mode": req.Mode})
}

func fsTouch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string
		Mode string `json:"path"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	f, err := os.OpenFile(req.Path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fsError(w, err)
		return
	}
	f.Close()
	if req.Mode != "" {
		m, _ := parseOctal(req.Mode)
		os.Chmod(req.Path, m)
	}
	writeJSON(w, 200, map[string]any{"ok": true, "path": req.Path})
}

func fsSymlink(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Target string
		Link   string `json:"target"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if err := os.Symlink(req.Target, req.Link); err != nil {
		fsError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "target": req.Target, "link": req.Link})
}

func fsReadlink(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	target, err := os.Readlink(path)
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"path": path, "target": target})
}

func fsChown(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path   string `json:"path"`
		UID    int    `json:"uid"`
		GID    int    `json:"gid"`
		Follow bool   `json:"follow"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if _, err := os.Lstat(req.Path); err != nil && os.IsNotExist(err) {
		writeJSON(w, 404, map[string]any{"error": "not found: " + req.Path})
		return
	}
	var err error
	if req.Follow {
		err = os.Chown(req.Path, req.UID, req.GID)
	} else {
		err = os.Lchown(req.Path, req.UID, req.GID)
	}
	if err != nil {
		fsError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "path": req.Path, "uid": req.UID, "gid": req.GID, "follow": req.Follow})
}

func fsUtimes(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path   string `json:"path"`
		ATime  int64  `json:"atime"`
		MTime  int64  `json:"mtime"`
		Follow bool   `json:"follow"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if _, err := os.Lstat(req.Path); err != nil && os.IsNotExist(err) {
		writeJSON(w, 404, map[string]any{"error": "not found: " + req.Path})
		return
	}
	utimes := []syscall.Timespec{
		syscall.NsecToTimespec(req.ATime * 1e6),
		syscall.NsecToTimespec(req.MTime * 1e6),
	}
	var err error
	if req.Follow {
		err = syscall.UtimesNano(req.Path, utimes)
	} else {
		err = syscall.UtimesNano(req.Path, utimes)
	}
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "path": req.Path})
}

func fsTruncate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
		Len  int64  `json:"len"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if err := os.Truncate(req.Path, req.Len); err != nil {
		fsError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "path": req.Path, "len": req.Len})
}

func fsLink(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Existing string
		Newpath  string `json:"existing"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if _, err := os.Lstat(req.Existing); err != nil && os.IsNotExist(err) {
		writeJSON(w, 404, map[string]any{"error": "not found: " + req.Existing})
		return
	}
	if err := os.Link(req.Existing, req.Newpath); err != nil {
		if os.IsExist(err) {
			writeJSON(w, 409, map[string]any{"error": "exists: " + req.Newpath})
			return
		}
		fsError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "existing": req.Existing, "newpath": req.Newpath})
}

func fsRealpath(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path string `json:"path"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if _, err := os.Lstat(req.Path); err != nil && os.IsNotExist(err) {
		writeJSON(w, 404, map[string]any{"error": "not found: " + req.Path})
		return
	}
	rp, err := filepath.EvalSymlinks(req.Path)
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"path": req.Path, "realpath": rp})
}

func fsMkdtemp(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Prefix string `json:"prefix"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	prefix := req.Prefix
	if strings.HasSuffix(prefix, "XXXXXX") {
		prefix = prefix[:len(prefix)-6]
	}
	dir, err := os.MkdirTemp("", prefix)
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"path": dir})
}

func fsAccess(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	modeStr := r.URL.Query().Get("mode")
	if _, err := os.Lstat(path); err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, 404, map[string]any{"error": "not found: " + path})
			return
		}
	}
	modeInt := 0
	if modeStr != "" {
		for _, c := range modeStr {
			if c >= '0' && c <= '7' {
				modeInt = modeInt*8 + int(c-'0')
			}
		}
	}
	if modeInt == 0 {
		w.WriteHeader(204)
		return
	}
	if err := syscall.Access(path, uint32(modeInt)); err != nil {
		writeJSON(w, 403, map[string]any{"error": "access denied: " + path})
		return
	}
	w.WriteHeader(204)
}
