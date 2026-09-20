# /// script
# requires-python = ">=3.11"
# dependencies = ["boto3>=1.40,<2"]
# ///
"""Disposable native-process/MinIO strict disk-removal demonstration. No kubeconfig/AWS use.

uv run examples/disk-removal/demo.py --binary target/debug/celld --esbuild /path/to/esbuild
"""

import argparse
import json
import os
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

MINIO = "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait(description, action, seconds=90):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            result = action()
            if result:
                return result
        except Exception as error:
            last = error
        time.sleep(0.3)
    raise RuntimeError(f"timed out: {description}; last error: {last}")


def http(url, method="GET", body=None):
    headers = {"Content-Type": "application/json"}
    request = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        method=method,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        text = error.read().decode()
        try:
            text = json.loads(text)
        except ValueError:
            pass
        return error.code, text


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=Path("target/debug/celld"))
    parser.add_argument("--esbuild", required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--failed-leader",
        action="store_true",
        help="kill the writer after a peer-only ack, then retire a follower",
    )
    parser.add_argument("--outage", action="store_true", help="hold MinIO unavailable across the shutdown deadline and verify no success")
    parser.add_argument("--ordinary", action="store_true", help="check preserve, ordinary shutdown and SIGTERM across restarts")
    parser.add_argument("--deadline", action="store_true", help="force a one-millisecond strict deadline and verify immutable failure")
    args = parser.parse_args()
    binary = str(args.binary.resolve())
    output = (
        args.output or Path(tempfile.mkdtemp(prefix="celld-retirement-"))
    ).resolve()
    output.mkdir(parents=True, exist_ok=True)
    name = "celld-retirement-" + secrets.token_hex(5)
    s3_port = port()
    endpoint = f"http://127.0.0.1:{s3_port}"
    credentials = dict(
        aws_access_key_id="retirement",
        aws_secret_access_key="retirement-local-only",
        region_name="us-east-1",
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        **credentials,
        config=Config(connect_timeout=2, read_timeout=5, retries={"max_attempts": 1}),
    )
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith(("AWS_", "CELLD_", "S3_"))
    }
    env.update(
        AWS_ACCESS_KEY_ID=credentials["aws_access_key_id"],
        AWS_SECRET_ACCESS_KEY=credentials["aws_secret_access_key"],
        AWS_REGION="us-east-1",
        AWS_ALLOW_HTTP="true",
        S3_ENDPOINT=endpoint,
        CELLD_BUCKET="s3://retirement",
        CELLD_ESBUILD=str(Path(args.esbuild).resolve()),
    )
    nodes, ledger, events = {}, [], []

    def event(message, **data):
        print(message, json.dumps(data) if data else "", flush=True)
        events.append({"event": message, **data})
        (output / "events.json").write_text(json.dumps(events, indent=2) + "\n")

    def start(node):
        previous = nodes.get(node)
        public, internal = (
            (previous["public"], previous["internal"]) if previous else (port(), port())
        )
        disk = output / node
        disk.mkdir(exist_ok=True)
        node_env = env | dict(
            CELLD_NODE=node,
            CELLD_WATCH=str(disk),
            CELLD_ADDR=f"127.0.0.1:{public}",
            CELLD_INTERNAL_ADDR=f"127.0.0.1:{internal}",
            CELLD_ADVERTISE=f"127.0.0.1:{internal}",
            CELLD_UNSAFE_PUBLIC_ADVERTISE="1",
            CELLD_DURABILITY="fleet",
            CELLD_TOKIO_THREADS="2",
            CELLD_SHUTDOWN_TOTAL_MS="1" if args.deadline else "30000",
            CELLD_REBALANCE_INTERVAL_MS="0",
        )
        log = open(output / f"{node}-{time.time_ns()}.log", "w")
        process = subprocess.Popen(
            [binary], env=node_env, stdout=log, stderr=subprocess.STDOUT
        )
        nodes[node] = dict(process=process, log=log, public=public, internal=internal)

    def stop(node):
        process = nodes[node]["process"]
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        nodes[node]["log"].close()

    def url(node, internal=False):
        return f"http://127.0.0.1:{nodes[node]['internal' if internal else 'public']}"

    def status(node):
        code, body = http(url(node, True) + "/state")
        assert code == 200, (code, body)
        return body["shutdown"]

    def healthy(node):
        process = nodes[node]["process"]
        assert process.poll() is None, (
            f"{node} exited {process.returncode}; see {output}"
        )
        return http(url(node) + "/.well-known/celld/health")[0] == 200

    def write(label, node, count=12):
        for index in range(count):
            identity = f"{label}-{index}"
            cell = f"{label}-cell-{index % 4}"
            path = f"/?cell={cell}&id={identity}"
            wait(
                identity,
                lambda: (
                    http(url(node) + path, "PUT", {})
                    == (200, {"id": identity, "value": identity})
                ),
            )
            ledger.append((cell, identity))
        event("Acknowledged writes", total=len(ledger), through=node)

    def verify(node):
        for cell, identity in ledger:
            wait(
                f"verify {identity}",
                lambda: (
                    http(url(node) + f"/?cell={cell}&id={identity}")
                    == (200, {"id": identity, "value": identity})
                ),
            )
        event("Verified acknowledged ledger", total=len(ledger), through=node)

    def peer_only_write(kill_leader=False):
        # Warm the route/cell before blocking S3. Receiving HTTP 200 while the
        # object store is frozen proves this response used a peer-disk ack.
        assert http(url("c") + "/?cell=initial-cell-0&id=initial-0")[0] == 200
        subprocess.run(["docker", "pause", name], check=True, stdout=subprocess.DEVNULL)
        started = time.monotonic()
        try:
            expected = {"id": "peer-only", "value": "peer-only"}
            assert http(url("c") + "/?cell=initial-cell-0&id=peer-only", "PUT", {}) == (
                200,
                expected,
            )
            ledger.append(("initial-cell-0", "peer-only"))
            event(
                "Peer-disk write acknowledged with MinIO paused",
                elapsed_ms=round((time.monotonic() - started) * 1000),
            )
            if kill_leader:
                stop("c")
                # Kill the frozen store before restarting it: buffered PUTs
                # from the killed writer cannot land after the fault boundary.
                subprocess.run(
                    ["docker", "kill", name], check=True, stdout=subprocess.DEVNULL
                )
                subprocess.run(
                    ["docker", "start", name], check=True, stdout=subprocess.DEVNULL
                )
                wait("restarted MinIO", lambda: s3.list_buckets())
                event("Writer killed before object store resumed")
        finally:
            subprocess.run(
                ["docker", "unpause", name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

    def retire(node, operation):
        before = status(node)
        assert before["schema_version"] == 1 and before["capabilities"]["strict_disk_removal"]
        request = {"operation_id": operation, "expected_generation": before["runtime_generation"]}
        endpoint = url(node, True) + "/shutdown?mode=remove-disk"
        assert http(endpoint, "POST", request | {"expected_generation": "stale"})[0] == 409
        code, body = http(endpoint, "POST", request)
        assert code == 202 and body["operation"]["phase"] == "draining", (code, body)
        assert http(endpoint, "POST", request)[0] == 202
        assert http(endpoint, "POST", request | {"operation_id": "conflict"})[0] == 409
        event("Strict shutdown accepted, not complete", node=node, operation=operation)
        last_status = None
        def completed():
            nonlocal last_status
            state = status(node)
            current = state["operation"]
            if current != last_status:
                event("Strict shutdown progress", node=node, status=state)
                last_status = current
            assert current["phase"] != "failed", current
            return state if current["phase"] == "data_safe" else None
        result = wait("strict completion " + node, completed, 90)
        assert result["control_only"]
        assert result["operation"]["operation_id"] == operation
        assert result["operation"]["expected_generation"] == request["expected_generation"]
        assert nodes[node]["process"].poll() is None
        assert status(node) == result
        (output / f"{operation}.json").write_text(json.dumps(result, indent=2) + "\n")
        stop(node)
        import shutil
        shutil.rmtree(output / node)
        event("Exact-generation data-safe result observed; process stopped and disk deleted", node=node)
        return request

    try:
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                name,
                "-p",
                f"127.0.0.1:{s3_port}:9000",
                "-e",
                "MINIO_ROOT_USER=retirement",
                "-e",
                "MINIO_ROOT_PASSWORD=retirement-local-only",
                MINIO,
                "server",
                "/data",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        wait("MinIO", lambda: s3.list_buckets())
        s3.create_bucket(Bucket="retirement")
        with open(output / "deploy.log", "w") as log:
            subprocess.run(
                [binary, "deploy", str(Path(__file__).parent.resolve())],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=True,
                timeout=60,
            )
        # Start the writer after both followers have published their leases;
        # startup recruitment can otherwise legitimately form a one-peer epoch.
        for node in ("a", "b"):
            start(node)
        for node in ("a", "b"):
            wait("healthy " + node, lambda node=node: healthy(node))
            wait(
                "published lease " + node,
                lambda node=node: s3.get_object(
                    Bucket="retirement", Key=f"nodes/{node}.json"
                ),
            )
        start("c")
        wait("healthy c", lambda: healthy("c"))
        event("Three native v0.5.1 nodes ready")
        write("initial", "c")
        if args.ordinary:
            for mode in ("/shutdown?handoff=preserve", "/shutdown", "SIGTERM"):
                if mode == "SIGTERM":
                    nodes["c"]["process"].terminate()
                else:
                    assert http(url("c", True) + mode, "POST", {}) == (200, {"ok": True})
                assert nodes["c"]["process"].wait(timeout=40) == 0
                start("c")
                wait("ordinary restart", lambda: healthy("c"))
                assert status("c")["operation"] is None
                verify("c")
                event("Ordinary shutdown and restart passed", mode=mode)
            return
        if args.outage or args.deadline:
            before = status("c")
            request = {"operation_id": "store-outage", "expected_generation": before["runtime_generation"]}
            if args.outage:
                subprocess.run(["docker", "pause", name], check=True, stdout=subprocess.DEVNULL)
            try:
                assert http(url("c", True) + "/shutdown?mode=remove-disk", "POST", request)[0] == 202
                def failed():
                    if nodes["c"]["process"].poll() is not None:
                        assert args.outage and nodes["c"]["process"].returncode == 3
                        return {"missing_completion": True, "self_fenced": True}
                    current = status("c")
                    assert current["operation"]["phase"] != "data_safe", current
                    return current if current["operation"]["phase"] == "failed" and current["control_only"] else None
                result = wait("store outage fails closed", failed, 60)
                event("Store outage/deadline failed closed", status=result)
            finally:
                if args.outage:
                    subprocess.run(["docker", "unpause", name], check=True, stdout=subprocess.DEVNULL)
            assert (output / "c").exists(), "no completion must retain the disk"
            if result.get("self_fenced"):
                event("Store outage caused existing lease self-fence; no completion, disk retained")
                return
            time.sleep(1)
            assert status("c") == result
            assert http(url("c", True) + "/shutdown?mode=remove-disk", "POST", request)[1] == result
            event("Failed terminal result remains immutable after store recovery")
            return
        if args.failed_leader:
            owner = json.loads(
                s3.get_object(Bucket="retirement", Key="nodes/c.json")["Body"].read()
            )
            assert owner["log"]["active"] and set(owner["log"]["ensemble"]) == {
                "a",
                "b",
            }, owner
            peer_only_write(kill_leader=True)
            retire("b", "failed-leader-follower")
            verify("a")
            write("single-survivor", "a", 4)
            verify("a")
            event(
                "PASS failed leader", acknowledgements=len(ledger), output=str(output)
            )
            return
        peer_only_write()
        old = retire("c", "three-to-two")
        verify("a")
        start("c")
        wait("new incarnation c", lambda: healthy("c"))
        assert http(url("c", True) + "/shutdown?mode=remove-disk", "POST", old)[0] == 409
        write("after-grow", "c")
        retire("c", "three-to-two-again")
        verify("a")
        write("before-last-follower", "a", 4)
        retire("b", "two-to-one")
        write("single-survivor", "a", 4)
        verify("a")
        own = json.loads(
            s3.get_object(Bucket="retirement", Key="nodes/a.json")["Body"].read()
        )
        assert own["log"]["active"], "cold recovery must exercise an active fleet epoch"
        assert own["log"]["bucket_complete"], own
        stop("a")
        start("a")
        wait("last survivor cold restart", lambda: healthy("a"))
        verify("a")
        losses = [
            obj["Key"]
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket="retirement", Prefix="log/"
            )
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".loss.json")
        ]
        assert not losses, losses
        event("Last survivor recovered from native folded-log proof with both retired peers offline")
        event("PASS", acknowledgements=len(ledger), output=str(output))
    finally:
        for node in nodes:
            stop(node)
        subprocess.run(
            ["docker", "rm", "-f", name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        (output / "ledger.json").write_text(json.dumps(ledger, indent=2) + "\n")


if __name__ == "__main__":
    main()
