package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

var (
	apiToken   = getenv("REMOTE_OPS_TOKEN", "dev-token-change-me")
	listenAddr = getenv("REMOTE_OPS_HOST", "0.0.0.0") + ":" + getenv("REMOTE_OPS_PORT", "8765")
	enableCORS = getenv("REMOTE_OPS_CORS", "false") == "true"
	wsMaxConn  = getenvInt("REMOTE_OPS_WS_MAX_CONN", 64)
	chunkSize  = 65536
)

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func getenvInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return d
}

// procTable tracks spawned processes for kill/status/stdin and orphan cleanup.
var procTable = struct {
	sync.Mutex
	m map[int]*procEntry
}{m: make(map[int]*procEntry)}

type procEntry struct {
	cmd       string
	spawnTime time.Time
	lastUse   time.Time
	proc      *os.Process
 stdin     *os.File // non-nil for stream/WS procs (for stdin writes)
	done      chan struct{} // closed when process exits
}

// fdTable tracks open file descriptors for ranged read/write.
var fdTable = struct {
	sync.Mutex
	m    map[int]*fdEntry
	next int
}{m: make(map[int]*fdEntry), next: 1000}

type fdEntry struct {
	path     string
	osfd     *os.File
	flags    int
	isAppend bool
	lastUse  time.Time
}

var wsConnCount struct {
	sync.Mutex
	n int
}

func main() {
	mux := http.NewServeMux()
	registerRoutes(mux)

	handler := http.Handler(mux)
	if enableCORS {
		handler = corsMiddleware(handler)
	}
	handler = authMiddleware(handler)

	srv := &http.Server{Addr: listenAddr, Handler: handler}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("remote-ops-server starting on %s", listenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	// Kill all tracked processes.
	procTable.Lock()
	for _, e := range procTable.m {
		killProcGroup(e.proc)
	}
	procTable.m = make(map[int]*procEntry)
	procTable.Unlock()

	// Close all tracked fds.
	fdTable.Lock()
	for _, e := range fdTable.m {
		e.osfd.Close()
	}
	fdTable.m = make(map[int]*fdEntry)
	fdTable.Unlock()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}

func registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", rootHandler)
	mux.HandleFunc("GET /health", healthHandler)

	// exec
	mux.HandleFunc("POST /api/exec", execSync)
	mux.HandleFunc("POST /api/exec/stream", execStream)
	mux.HandleFunc("POST /api/exec/kill", execKill)
	mux.HandleFunc("GET /api/exec/status", execStatus)
	mux.HandleFunc("POST /api/exec/stdin", execStdin)

	// websocket
	mux.HandleFunc("/ws/exec", wsExec)

	// fs basic
	mux.HandleFunc("GET /api/fs/stat", fsStat)
	mux.HandleFunc("GET /api/fs/read", fsRead)
	mux.HandleFunc("PUT /api/fs/write", fsWrite)
	mux.HandleFunc("POST /api/fs/delete", fsDelete)
	mux.HandleFunc("POST /api/fs/mkdir", fsMkdir)
	mux.HandleFunc("POST /api/fs/move", fsMove)
	mux.HandleFunc("POST /api/fs/copy", fsCopy)
	mux.HandleFunc("POST /api/fs/chmod", fsChmod)
	mux.HandleFunc("POST /api/fs/touch", fsTouch)
	mux.HandleFunc("POST /api/fs/symlink", fsSymlink)
	mux.HandleFunc("GET /api/fs/readlink", fsReadlink)
	mux.HandleFunc("POST /api/fs/chown", fsChown)
	mux.HandleFunc("POST /api/fs/utimes", fsUtimes)
	mux.HandleFunc("POST /api/fs/truncate", fsTruncate)
	mux.HandleFunc("POST /api/fs/link", fsLink)
	mux.HandleFunc("POST /api/fs/realpath", fsRealpath)
	mux.HandleFunc("POST /api/fs/mkdtemp", fsMkdtemp)
	mux.HandleFunc("GET /api/fs/statfs", fsStatfs)
	mux.HandleFunc("GET /api/fs/list", fsList)
	mux.HandleFunc("GET /api/fs/glob", fsGlob)
	mux.HandleFunc("GET /api/fs/access", fsAccess)
	mux.HandleFunc("POST /api/fs/batch", fsBatch)
	mux.HandleFunc("POST /api/fs/patch", fsPatch)

	// fd session
	mux.HandleFunc("POST /api/fs/fd/open", fdOpen)
	mux.HandleFunc("GET /api/fs/fd/read", fdRead)
	mux.HandleFunc("PUT /api/fs/fd/write", fdWrite)
	mux.HandleFunc("POST /api/fs/fd/close", fdClose)
	mux.HandleFunc("GET /api/fs/fd/fstat", fdFstat)
	mux.HandleFunc("POST /api/fs/fd/ftruncate", fdFtruncate)
	mux.HandleFunc("POST /api/fs/fd/fsync", fdFsync)
	mux.HandleFunc("POST /api/fs/fd/fchmod", fdFchmod)
	mux.HandleFunc("POST /api/fs/fd/fchown", fdFchown)
	mux.HandleFunc("POST /api/fs/fd/futimes", fdFutimes)

	// watch
	mux.HandleFunc("GET /api/fs/watch", fsWatch)

	// misc
	mux.HandleFunc("GET /api/which", whichHandler)
	mux.HandleFunc("GET /api/env", envHandler)
}

func rootHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"service": "remote-ops-server", "status": "running"})
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"status": "ok", "pid": os.Getpid()})
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		token := auth
		if len(auth) > 7 && auth[:7] == "Bearer " {
			token = auth[7:]
		}
		if token != apiToken {
			writeJSON(w, 401, map[string]any{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		next.ServeHTTP(w, r)
	})
}