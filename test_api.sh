#!/bin/bash
T="test-token"
B="http://127.0.0.1:8765"
H="Authorization: Bearer $T"
CT="Content-Type: application/json"
JQ="python3 -m json.tool"

echo "=== 1. Health ==="
curl -s $B/health
echo ""

echo "=== 2. Exec simple ==="
curl -s -X POST $B/api/exec -H "$H" -H "$CT" -d '{"cmd":"echo hello world","cwd":"/tmp"}'
echo ""

echo "=== 3. Exec with env + exit code ==="
curl -s -X POST $B/api/exec -H "$H" -H "$CT" -d '{"cmd":"echo $MY_VAR; exit 42","env":{"MY_VAR":"test123"}}'
echo ""

echo "=== 4. Exec timeout (should 408) ==="
curl -s -X POST $B/api/exec -H "$H" -H "$CT" -d '{"cmd":"sleep 10","timeout":2}'
echo ""

echo "=== 5. Exec with stdin ==="
curl -s -X POST $B/api/exec -H "$H" -H "$CT" -d '{"cmd":"cat","stdin":"piped content here"}'
echo ""

echo "=== 6. File write (raw body) ==="
curl -s -X PUT "$B/api/fs/write?path=/tmp/remote_ops_test/hello.txt&mode=0644" -H "$H" -d "Hello from remote-ops-server!"
echo ""

echo "=== 7. File read (raw body) ==="
curl -s "$B/api/fs/read?path=/tmp/remote_ops_test/hello.txt" -H "$H"
echo ""

echo "=== 8. File stat ==="
curl -s "$B/api/fs/stat?path=/tmp/remote_ops_test/hello.txt" -H "$H"
echo ""

echo "=== 9. File list ==="
curl -s "$B/api/fs/list?path=/tmp/remote_ops_test" -H "$H"
echo ""

echo "=== 10. Batch operations ==="
curl -s -X POST "$B/api/fs/batch" -H "$H" -H "$CT" -d '{"ops":[{"op":"write","path":"/tmp/remote_ops_test/a.txt","content":"file A"},{"op":"write","path":"/tmp/remote_ops_test/b.txt","content":"file B"},{"op":"mkdir","path":"/tmp/remote_ops_test/subdir"},{"op":"delete","path":"/tmp/remote_ops_test/hello.txt"}]}'
echo ""

echo "=== 11. List after batch ==="
curl -s "$B/api/fs/list?path=/tmp/remote_ops_test" -H "$H"
echo ""

echo "=== 12. Which ==="
curl -s "$B/api/which?cmd=python3" -H "$H"
echo ""

echo "=== 13. Mkdir + move + copy ==="
curl -s -X POST "$B/api/fs/mkdir" -H "$H" -H "$CT" -d '{"path":"/tmp/remote_ops_test/movedir"}'
curl -s -X POST "$B/api/fs/move" -H "$H" -H "$CT" -d '{"src":"/tmp/remote_ops_test/a.txt","dst":"/tmp/remote_ops_test/movedir/a.txt"}'
curl -s -X POST "$B/api/fs/copy" -H "$H" -H "$CT" -d '{"src":"/tmp/remote_ops_test/b.txt","dst":"/tmp/remote_ops_test/b_copy.txt"}'
curl -s "$B/api/fs/list?path=/tmp/remote_ops_test&recursive=true" -H "$H"
echo ""

echo "=== 14. Auth failure (no token) ==="
curl -s -X POST "$B/api/exec" -H "$CT" -d '{"cmd":"echo should fail"}'
echo ""

echo "=== 15. Patch ==="
curl -s -X PUT "$B/api/fs/write?path=/tmp/remote_ops_test/patch_test.txt" -H "$H" -d "line1
line2
line3"
curl -s -X POST "$B/api/fs/patch" -H "$H" -H "$CT" -d '{"path":"/tmp/remote_ops_test/patch_test.txt","patch":"*** Begin Patch\n*** Update File: /tmp/remote_ops_test/patch_test.txt\n@@ -1,3 +1,3 @@\n line1\n-line2\n+line2 modified\n line3\n*** End Patch"}'
echo ""
curl -s "$B/api/fs/read?path=/tmp/remote_ops_test/patch_test.txt" -H "$H"
echo ""

echo "=== 16. Exec stream (SSE) ==="
curl -s -N -X POST "$B/api/exec/stream" -H "$H" -H "$CT" -d '{"cmd":"for i in 1 2 3; do echo line_$i; sleep 0.3; done"}' 2>&1 | head -10
echo ""

echo "=== DONE ==="