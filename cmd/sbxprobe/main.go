// Command sbxprobe is a TEMPORARY validation harness used to confirm that a
// real Python plugin (syscall-heavy: sockets, TLS, subprocess, threads) runs
// under the native sandbox (namespaces + NO_NEW_PRIVS + seccomp allowlist).
// It is deleted after the fleet rollout validation.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"sentra-agent/internal/plugin"
	"sentra-agent/internal/sandbox"
	"sentra-agent/internal/system"
)

// reexecTarget mirrors cmd/main.go: the sandbox re-execs the agent binary with
// --seccomp-exec / --no-new-privs-exec as its first argument. A harness that
// calls plugin.Execute must honor it, exactly like the real agent.
func reexecTarget() (mode, target string, args []string) {
	if len(os.Args) < 3 {
		return "", "", nil
	}
	switch os.Args[1] {
	case "--seccomp-exec", "--no-new-privs-exec":
		return os.Args[1], os.Args[2], os.Args[3:]
	}
	return "", "", nil
}

const pyPlugin = `import json, os, sys, socket, ssl, subprocess, threading, hashlib, struct, select

payload = {}
try:
    raw = sys.stdin.read()
    if raw.strip():
        payload = json.loads(raw)
except Exception as e:
    payload = {"_stdin_error": str(e)}

res = {"ok": True, "echo": payload.get("echo"), "checks": {}}

# hashlib / struct / threading
h = hashlib.sha256(b"hello").hexdigest()
tres = []
def worker():
    tres.append(1)
t = threading.Thread(target=worker); t.start(); t.join()
res["checks"]["hash"] = h[:8]
res["checks"]["thread"] = len(tres)

# subprocess (chromium/playwright launches a child)
try:
    out = subprocess.check_output(["sh", "-c", "echo child-ok"], timeout=10)
    res["checks"]["subprocess"] = out.decode().strip()
except Exception as e:
    res["checks"]["subprocess"] = "ERR:" + str(e)

res["checks"]["env"] = {
    "HOME": os.environ.get("HOME"),
    "TMPDIR": os.environ.get("TMPDIR"),
    "PATH": (os.environ.get("PATH") or "")[:80],
    "PWD": os.environ.get("PWD"),
}
import traceback  # noqa: E402

# playwright / chromium (headless) — the heaviest runtime path
if payload.get("playwright"):
    try:
        from playwright.sync_api import sync_playwright
        p = sync_playwright().start()  # plugin (walmart_search.py) uses this form
        b = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        pg = b.new_page()
        pg.set_content("<h1 id=x>chromium-ok</h1>")
        txt = pg.inner_text("#x")
        b.close()
        p.stop()
        res["checks"]["playwright"] = txt
    except Exception as e:
        res["checks"]["playwright"] = "ERR:" + str(e)[:300]
        res["checks"]["playwright_tb"] = traceback.format_exc()[-900:]
else:
    res["checks"]["playwright"] = "skipped"

# network + TLS
if payload.get("net"):
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection(("example.com", 443), timeout=10) as s:
            with ctx.wrap_socket(s, server_hostname="example.com") as ss:
                ss.sendall(b"GET / HTTP/1.0\r\nHost: example.com\r\n\r\n")
                data = ss.recv(64)
                res["checks"]["tls"] = "ok" if data else "empty"
    except Exception as e:
        res["checks"]["tls"] = "ERR:" + str(e)
    # raw socket + select
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5); s.connect(("example.com", 80))
        s.sendall(b"HEAD / HTTP/1.0\r\nHost: example.com\r\n\r\n")
        _, w, _ = select.select([s], [s], [], 5)
        got = s.recv(32); s.close()
        res["checks"]["socket"] = "ok" if got else "empty"
    except Exception as e:
        res["checks"]["socket"] = "ERR:" + str(e)

print(json.dumps(res))
`

func main() {
	if mode, target, args := reexecTarget(); target != "" {
		if mode == "--seccomp-exec" {
			sandbox.SeccompExec(target, args)
		} else {
			sandbox.NoNewPrivsExec(target, args)
		}
		return
	}

	dir, err := os.MkdirTemp("", "sentra-sbxprobe-*")
	if err != nil {
		fmt.Println("ERROR tempdir:", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	scriptPath := filepath.Join(dir, "main.py")
	if err := os.WriteFile(scriptPath, []byte(pyPlugin), 0o700); err != nil {
		fmt.Println("ERROR write:", err)
		os.Exit(1)
	}

	net := os.Getenv("SBX_NET") == "1"
	manifest := plugin.Manifest{
		Name:     "sbxprobe",
		Version:  "1.0.0",
		Filename: "main.py",
		Language: "python",
		Trusted:  true,
		Network:  net,
		Resources: plugin.PluginResources{
			MemoryMB:       256,
			CPUSeconds:     30,
			TimeoutSeconds: 45,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pw := os.Getenv("SBX_PW") == "1"
	payload := fmt.Sprintf(`{"echo":"probe","net":true,"playwright":%t}`, pw)

	start := time.Now()
	res, err := plugin.Execute(ctx, scriptPath, manifest, payload, system.DetectExecutionEnv(), nil)
	dur := time.Since(start)
	if err != nil {
		fmt.Printf("[sbxprobe] ERROR: %v (%s)\n", err, dur)
		os.Exit(2)
	}
	out := strings.TrimSpace(res.Output)
	var parsed map[string]interface{}
	if jerr := json.Unmarshal([]byte(out), &parsed); jerr != nil {
		fmt.Printf("[sbxprobe] ERROR: non-JSON output: %v: %q (%s)\n", jerr, out, dur)
		os.Exit(3)
	}
	b, _ := json.Marshal(parsed)
	fmt.Printf("[sbxprobe] OK: method=%s out=%s (%s)\n", res.Method, string(b), dur)
}
