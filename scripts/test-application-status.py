#!/usr/bin/env python3
"""Opt-in native-runtime deployment observation test using disposable Docker MinIO.

Requires CELLD_TEST_BINARY, CELLD_ESBUILD, Docker and aws CLI. No AWS account is
used: every S3 operation has an explicit loopback endpoint and fixture credentials.
"""
import concurrent.futures
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request
import uuid


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait(check, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, ValueError, subprocess.CalledProcessError) as error:
            last = error.output.decode(errors="replace") if isinstance(error, subprocess.CalledProcessError) else error
        time.sleep(0.1)
    raise AssertionError(f"condition timed out: {last}")


def run():
    binary = str(Path(os.environ["CELLD_TEST_BINARY"]).resolve())
    name = "application-status-" + uuid.uuid4().hex[:12]
    runtime = None
    with tempfile.TemporaryDirectory(prefix="celld-application-") as directory:
        root = Path(directory)
        env = os.environ | {
            "AWS_ACCESS_KEY_ID": "application", "AWS_SECRET_ACCESS_KEY": "application-local-only",
            "AWS_REGION": "us-east-1", "AWS_DEFAULT_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true",
            "AWS_EC2_METADATA_DISABLED": "true", "AWS_CONFIG_FILE": "/dev/null", "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
            "CELLD_BUCKET": "s3://application", "CELLD_NODE": "application-node",
            "CELLD_WATCH": str(root / "data"), "CELLD_TOKIO_THREADS": "2",
            "CELLD_DEPLOY_POLL_S": "1", "CELLD_DEPLOY_MAX_AGE_S": "60",
            "CELLD_SHUTDOWN_TOTAL_MS": "1000", "CELLD_REBALANCE_INTERVAL_MS": "0",
            "CELLD_UNSAFE_PUBLIC_ADVERTISE": "1", "CELLD_DURABILITY": "bucket",
        }
        for key in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_SESSION_TOKEN"):
            env.pop(key, None)
        public, internal = port(), port()
        env |= {"CELLD_ADDR": f"127.0.0.1:{public}", "CELLD_INTERNAL_ADDR": f"127.0.0.1:{internal}", "CELLD_ADVERTISE": f"127.0.0.1:{internal}"}
        def command(*args):
            return subprocess.check_output(args, env=env, stderr=subprocess.STDOUT, timeout=60)
        def fetch(url):
            with urllib.request.urlopen(url, timeout=30) as response:
                return response.read()
        def snapshot():
            return json.loads(fetch(f"http://127.0.0.1:{internal}/state?view=application"))
        log = root / "runtime.log"
        try:
            command("docker", "run", "-d", "--tmpfs", "/data:rw,size=1g", "--name", name,
                    "-p", "127.0.0.1::9000", "-e", "MINIO_ROOT_USER=application", "-e",
                    "MINIO_ROOT_PASSWORD=application-local-only",
                    "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e", "server", "/data")
            endpoint = "http://" + command("docker", "port", name, "9000/tcp").decode().strip()
            env["S3_ENDPOINT"] = endpoint
            def s3(*args):
                return command("aws", "--endpoint-url", endpoint, "s3api", *args)
            wait(lambda: (s3("create-bucket", "--bucket", "application"), True)[1])
            project = root / "worker"
            project.mkdir()
            (project / "wrangler.jsonc").write_text(json.dumps({"name":"observation-test", "main":"index.js", "compatibility_date":"2026-01-01", "durable_objects":{"bindings":[{"name":"CELL","class_name":"Cell"}]}, "migrations":[{"tag":"v1","new_sqlite_classes":["Cell"]}]}))
            def deploy(version):
                (project / "index.js").write_text('''export class Cell {
 constructor(state) { this.state = state; }
 async fetch(request) {
  if (new URL(request.url).pathname === "/slow") {
   await this.state.storage.put("entered", true);
   await new Promise(resolve => setTimeout(resolve, 10000));
  }
  return new Response("VERSION");
 }
}
export default { fetch(request, env) { return env.CELL.get(env.CELL.idFromName("one")).fetch(request); } };
'''.replace("VERSION", version))
                command(binary, "deploy", str(project))
                pointer_file = root / "pointer.json"
                s3("get-object", "--bucket", "application", "--key", "deploy/current.json", str(pointer_file))
                return json.loads(pointer_file.read_text())
            def publish(pointer):
                path = root / "publish.json"
                path.write_text(json.dumps(pointer))
                s3("put-object", "--bucket", "application", "--key", "deploy/current.json", "--body", str(path))
            def converged(pointer):
                a = snapshot()
                return a if a["snapshot_valid"] and a["pointer_status"] == "observed" and a["loaded"]["version"] == pointer["version"] and a["target"]["version"] == pointer["version"] and a["pending_cells"] == 0 and a["swapping_cells"] == 0 else None
            first = deploy("v1")
            with log.open("wb") as output:
                runtime = subprocess.Popen([binary], env=env, stdout=output, stderr=output)
            initial = wait(lambda: converged(first))
            assert initial["schema_version"] == 1 and initial["runtime_generation"]
            assert wait(lambda: fetch(f"http://127.0.0.1:{public}/") == b"v1")
            with concurrent.futures.ThreadPoolExecutor() as pool:
                slow = pool.submit(fetch, f"http://127.0.0.1:{public}/slow")
                # Observe admitted activity before publishing a new version.
                time.sleep(0.5)
                second = deploy("v2")
                def transitioning():
                    a = snapshot()
                    return a if a["snapshot_valid"] and a["loaded"]["version"] == second["version"] and a["pending_cells"] > 0 else None
                transition = wait(transitioning, timeout=8)
                assert slow.result(timeout=15) == b"v1"
            adopted = wait(lambda: converged(second))
            assert fetch(f"http://127.0.0.1:{public}/") == b"v2"
            broken = first | {"version":"broken", "prefix":"deploy/missing/"}
            publish(broken)
            def failed():
                a = snapshot()
                return a if a["adoption_status"] == "failed" and a["target"]["version"] == "broken" else None
            failure = wait(failed)
            assert failure["loaded"]["version"] == second["version"]
            publish(first)
            rollback = wait(lambda: converged(first))
            assert fetch(f"http://127.0.0.1:{public}/") == b"v1"
            # Pointer disappearance invalidates convergence without altering serving code.
            s3("delete-object", "--bucket", "application", "--key", "deploy/current.json")
            wait(lambda: snapshot()["pointer_status"] == "unavailable")
            assert fetch(f"http://127.0.0.1:{public}/") == b"v1"
            assert initial["runtime_generation"] == rollback["runtime_generation"]
            print(json.dumps({"result":"PASS", "initial":initial,"transition":transition,"adopted":adopted,"failure":failure,"rollback":rollback}, indent=2))
        except BaseException:
            if log.exists():
                print(log.read_text(errors="replace")[-16000:])
            raise
        finally:
            if runtime:
                runtime.terminate()
                try:
                    runtime.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    runtime.kill()
                    runtime.wait(timeout=5)
            subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15, check=False)


if __name__ == "__main__":
    run()
