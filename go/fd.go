package main

import (
	"io"
	"net/http"
	"os"
	"syscall"
	"time"
)

func fdGet(fd int) (*fdEntry, bool) {
	fdTable.Lock()
	e := fdTable.m[fd]
	if e == nil {
		fdTable.Unlock()
		return nil, false
	}
	e.lastUse = time.Now()
	fdTable.Unlock()
	return e, true
}

func fdOpen(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Path  string `json:"path"`
		Flags int    `json:"flags"`
		Mode  int    `json:"mode"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	if req.Mode == 0 {
		req.Mode = 0666
	}
	flag := req.Flags | syscall.O_CLOEXEC
	osfd, err := syscall.Open(req.Path, flag, uint32(req.Mode))
	if err != nil {
		fsError(w, err)
		return
	}
	var st syscall.Stat_t
	syscall.Fstat(osfd, &st)
	fdTable.Lock()
	fd := fdTable.next
	fdTable.next++
	fdTable.m[fd] = &fdEntry{
		path:     req.Path,
		osfd:     os.NewFile(uintptr(osfd), req.Path),
		flags:    req.Flags,
		isAppend: req.Flags&syscall.O_APPEND != 0,
		lastUse:  time.Now(),
	}
	fdTable.Unlock()
	writeJSON(w, 200, map[string]any{"fd": fd, "size": int64(st.Size)})
}

func fdRead(w http.ResponseWriter, r *http.Request) {
	fd := queryInt(r, "fd", 0)
	offset := queryInt(r, "offset", 0)
	length := queryInt(r, "length", 65536)
	e, ok := fdGet(fd)
	if !ok {
		writeJSON(w, 410, map[string]any{"error": "bad fd: " + strconvItoa(fd)})
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	buf := make([]byte, chunkSize)
	remaining := length
	pos := int64(offset)
	for remaining > 0 {
		n := chunkSize
		if remaining < n {
			n = remaining
		}
		nr, err := syscall.Pread(int(e.osfd.Fd()), buf[:n], pos)
		if nr > 0 {
			w.Write(buf[:nr])
			pos += int64(nr)
			remaining -= nr
		}
		if err != nil || nr == 0 {
			break
		}
	}
}

func fdWrite(w http.ResponseWriter, r *http.Request) {
	fd := queryInt(r, "fd", 0)
	offset := queryInt(r, "offset", -1)
	e, ok := fdGet(fd)
	if !ok {
		writeJSON(w, 410, map[string]any{"error": "bad fd: " + strconvItoa(fd)})
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	var n int
	if e.isAppend || offset < 0 {
		n, err = syscall.Write(int(e.osfd.Fd()), body)
	} else {
		n, err = syscall.Pwrite(int(e.osfd.Fd()), body, int64(offset))
	}
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true, "bytes": n})
}

func fdClose(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Fd int `json:"fd"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	fdTable.Lock()
	e := fdTable.m[req.Fd]
	delete(fdTable.m, req.Fd)
	fdTable.Unlock()
	if e == nil {
		writeJSON(w, 410, map[string]any{"error": "bad fd: " + strconvItoa(req.Fd)})
		return
	}
	e.osfd.Close()
	writeJSON(w, 200, map[string]any{"ok": true})
}

func fdFstat(w http.ResponseWriter, r *http.Request) {
	fd := queryInt(r, "fd", 0)
	e, ok := fdGet(fd)
	if !ok {
		writeJSON(w, 410, map[string]any{"error": "bad fd: " + strconvItoa(fd)})
		return
	}
	var st syscall.Stat_t
	if err := syscall.Fstat(int(e.osfd.Fd()), &st); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, statToDict(&st))
}

func fdFtruncate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Fd  int   `json:"fd"`
		Len int64 `json:"len"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	e, ok := fdGet(req.Fd)
	if !ok {
		writeJSON(w, 410, map[string]any{"error": "bad fd: " + strconvItoa(req.Fd)})
		return
	}
	if err := syscall.Ftruncate(int(e.osfd.Fd()), req.Len); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func fdFsync(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Fd int `json:"fd"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	e, ok := fdGet(req.Fd)
	if !ok {
		writeJSON(w, 410, map[string]any{"error": "bad fd: " + strconvItoa(req.Fd)})
		return
	}
	if err := syscall.Fsync(int(e.osfd.Fd())); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func fdFchmod(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Fd   int `json:"fd"`
		Mode int `json:"mode"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	e, ok := fdGet(req.Fd)
	if !ok {
		writeJSON(w, 410, map[string]any{"error": "bad fd: " + strconvItoa(req.Fd)})
		return
	}
	if err := syscall.Fchmod(int(e.osfd.Fd()), uint32(req.Mode)); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func fdFchown(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Fd  int `json:"fd"`
		UID int `json:"uid"`
		GID int `json:"gid"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	e, ok := fdGet(req.Fd)
	if !ok {
		writeJSON(w, 410, map[string]any{"error": "bad fd: " + strconvItoa(req.Fd)})
		return
	}
	if err := syscall.Fchown(int(e.osfd.Fd()), req.UID, req.GID); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func fdFutimes(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Fd    int   `json:"fd"`
		ATime int64 `json:"atime"`
		MTime int64 `json:"mtime"`
	}
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	e, ok := fdGet(req.Fd)
	if !ok {
		writeJSON(w, 410, map[string]any{"error": "bad fd: " + strconvItoa(req.Fd)})
		return
	}
	tv := []syscall.Timeval{
		{Sec: req.ATime / 1000, Usec: int64(req.ATime % 1000 * 1000)},
		{Sec: req.MTime / 1000, Usec: int64(req.MTime % 1000 * 1000)},
	}
	if err := syscall.Futimes(int(e.osfd.Fd()), tv); err != nil {
		writeJSON(w, 400, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func strconvItoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}
