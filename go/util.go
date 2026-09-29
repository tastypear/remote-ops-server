package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"syscall"
	"time"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func queryInt(r *http.Request, key string, def int) int {
	s := r.URL.Query().Get(key)
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

func queryBool(r *http.Request, key string, def bool) bool {
	s := r.URL.Query().Get(key)
	if s == "" {
		return def
	}
	return s == "true" || s == "1"
}

func statType(mode uint32) string {
	switch mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		return "dir"
	case syscall.S_IFLNK:
		return "symlink"
	case syscall.S_IFBLK:
		return "blockdev"
	case syscall.S_IFCHR:
		return "chardev"
	case syscall.S_IFIFO:
		return "fifo"
	case syscall.S_IFSOCK:
		return "socket"
	default:
		return "file"
	}
}

func statToDict(st *syscall.Stat_t) map[string]any {
	mtimeMs := int64(st.Mtim.Sec)*1000 + int64(st.Mtim.Nsec)/1e6
	atimeMs := int64(st.Atim.Sec)*1000 + int64(st.Atim.Nsec)/1e6
	ctimeMs := int64(st.Ctim.Sec)*1000 + int64(st.Ctim.Nsec)/1e6
	return map[string]any{
		"type":      statType(uint32(st.Mode)),
		"size":      int64(st.Size),
		"mode":      fmt.Sprintf("0o%o", st.Mode&0777),
		"fullMode":  uint32(st.Mode),
		"mtime":     mtimeMs,
		"atime":     atimeMs,
		"ctime":     ctimeMs,
		"birthtime": mtimeMs,
		"uid":       uint32(st.Uid),
		"gid":       uint32(st.Gid),
		"ino":       uint64(st.Ino),
		"nlink":     uint64(st.Nlink),
		"blksize":   int64(st.Blksize),
		"blocks":    int64(st.Blocks),
		"rdev":      uint64(st.Rdev),
		"dev":       uint64(st.Dev),
	}
}

func lstatToDict(path string) (map[string]any, error) {
	var st syscall.Stat_t
	if err := syscall.Lstat(path, &st); err != nil {
		return nil, err
	}
	return statToDict(&st), nil
}

func statToDictFollow(path string) (map[string]any, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return nil, err
	}
	return statToDict(&st), nil
}

func procRegister(pid int, cmd string, proc *os.Process, stdin *os.File) *procEntry {
	e := &procEntry{
		cmd:       cmd,
		spawnTime: time.Now(),
		lastUse:   time.Now(),
		proc:      proc,
		stdin:     stdin,
		done:      make(chan struct{}),
	}
	procTable.Lock()
	procTable.m[pid] = e
	procTable.Unlock()
	return e
}

func procUnregister(pid int) {
	procTable.Lock()
	delete(procTable.m, pid)
	procTable.Unlock()
}

func procGet(pid int) *procEntry {
	procTable.Lock()
	e := procTable.m[pid]
	if e != nil {
		e.lastUse = time.Now()
	}
	procTable.Unlock()
	return e
}

func procTouch(pid int) {
	procTable.Lock()
	if e := procTable.m[pid]; e != nil {
		e.lastUse = time.Now()
	}
	procTable.Unlock()
}

func killProcGroup(proc *os.Process) {
	if proc == nil {
		return
	}
	pgid, err := syscall.Getpgid(proc.Pid)
	if err != nil {
		proc.Signal(syscall.SIGKILL)
		return
	}
	syscall.Kill(-pgid, syscall.SIGKILL)
}

func killProcGroupSig(proc *os.Process, sig syscall.Signal) {
	if proc == nil {
		return
	}
	pgid, err := syscall.Getpgid(proc.Pid)
	if err != nil {
		proc.Signal(sig)
		return
	}
	syscall.Kill(-pgid, sig)
}

func msSince(start time.Time) int {
	return int(time.Since(start).Milliseconds())
}

func safeCwd(cwd string) string {
	if cwd == "" {
		return "/"
	}
	if info, err := os.Stat(cwd); err == nil && info.IsDir() {
		return cwd
	}
	return "/"
}