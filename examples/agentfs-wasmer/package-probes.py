"""Bounded stock-CLI package probes; separate from the AgentFS integration."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

cli = os.environ.get("WASMER_BIN", "wasmer")
cache = tempfile.mkdtemp(prefix="agentfs-wasmer-packages-")
env = dict(os.environ, WASMER_DIR=cache + "/home", WASMER_CACHE_DIR=cache + "/cache")
probes = [
    ["wasmer/bash@=1.0.25", "--", "-c", 'printf "bash-ok\\n"; printf "x\\n" | cat'],
    ["python/python@=3.13.20", "--", "-c", 'import sys,json; print(sys.version); print(json.dumps({"ok":True}))'],
    ["wasmer/coreutils@=1.0.25", "--entrypoint", "echo", "--", "coreutils-ok"],
]
results = {"cli": subprocess.check_output([cli, "--version"], text=True).strip(), "runs": []}
for args in probes:
    started = time.monotonic()
    try:
        p = subprocess.run([cli, "run"] + args, capture_output=True, text=True, env=env, timeout=90)
        result = dict(args=args, code=p.returncode, stdout=p.stdout, stderr=p.stderr)
    except subprocess.TimeoutExpired as e:
        result = dict(args=args, timeout=True, stdout=str(e.stdout), stderr=str(e.stderr))
    result["seconds"] = time.monotonic() - started
    results["runs"].append(result)
    print(json.dumps(result), flush=True)
Path(__file__).with_name("package-results-local.json").write_text(json.dumps(results, indent=2) + "\n")
if any(r.get("code") != 0 for r in results["runs"]):
    raise SystemExit(1)
