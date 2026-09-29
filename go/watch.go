package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type fileState struct {
	mtime  int64
	size   int64
	isDir  bool
}

func scanDir(base string, recursive bool) map[string]fileState {
	result := map[string]fileState{}
	info, err := os.Lstat(base)
	if err != nil {
		return result
	}
	if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
		if recursive {
			filepath.WalkDir(base, func(full string, d os.DirEntry, err error) error {
				if err != nil || full == base {
					return nil
				}
				if st, err := os.Lstat(full); err == nil {
					result[full] = fileState{mtime: st.ModTime().UnixNano(), size: st.Size(), isDir: st.IsDir()}
				}
				return nil
			})
		} else {
			entries, _ := os.ReadDir(base)
			for _, e := range entries {
				full := filepath.Join(base, e.Name())
				if st, err := os.Lstat(full); err == nil {
					result[full] = fileState{mtime: st.ModTime().UnixNano(), size: st.Size(), isDir: st.IsDir()}
				}
			}
		}
	} else {
		result[base] = fileState{mtime: info.ModTime().UnixNano(), size: info.Size(), isDir: info.IsDir()}
	}
	return result
}

func fsWatch(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	recursive := queryBool(r, "recursive", false)
	interval := queryInt(r, "interval", 500)

	if _, err := os.Lstat(path); err != nil {
		writeJSON(w, 404, map[string]any{"error": "not found: " + path})
		return
	}

	flusher, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(200)

	sseEmit := func(event, filename, action string) {
		v, _ := json.Marshal(map[string]any{"event": event, "filename": filename, "action": action})
		fmt.Fprintf(w, "data: %s\n\n", v)
		if flusher != nil {
			flusher.Flush()
		}
	}

	prev := scanDir(path, recursive)
	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
		curr := scanDir(path, recursive)
		for f := range prev {
			if _, ok := curr[f]; !ok {
				rel := ""
				if f != path {
					rel, _ = filepath.Rel(path, f)
				}
				sseEmit("rename", rel, "deleted")
			}
		}
		for f := range curr {
			if _, ok := prev[f]; !ok {
				rel := ""
				if f != path {
					rel, _ = filepath.Rel(path, f)
				}
				sseEmit("rename", rel, "created")
			}
		}
		for f, s := range curr {
			if ps, ok := prev[f]; ok && ps != s {
				rel := ""
				if f != path {
					rel, _ = filepath.Rel(path, f)
				}
				sseEmit("change", rel, "modified")
			}
		}
		prev = curr
	}
}

var _ = syscall.Stat_t{}