// Requires an exclusive, empty, writable cgroup-v2 parent with cpu/memory/pids
// delegated. This is a real kernel enforcement test, never a directory mock.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, writeFile, readFile, readdir, rm } from "node:fs/promises";
import { createResources } from "../service/resources.mjs";

const parent = process.env.SANDBOX_TEST_CGROUP_PARENT;
assert.equal(process.platform, "linux");
assert.ok(parent, "SANDBOX_TEST_CGROUP_PARENT is required");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const root = await mkdtemp("/tmp/celld-cgroup-tests-");
const config = {
  runtimeDirectory: root,
  cgroupParent: parent,
  resources: { memoryBytes: 64 * 1024 * 1024, cpuMillis: 100, pids: 16 },
};
let manager;
const children = new Set();
const allocations = new Set();
const evidence = { platform: process.platform, checks: [] };
function pass(value) {
  evidence.checks.push(value);
  console.log("PASS", value);
}
async function launch(source) {
  const allocation = await manager.allocate();
  allocations.add(allocation);
  const child = spawn(
    process.execPath,
    ["-e", `process.stdin.resume();process.stdin.on('end',()=>{${source}})`],
    {
      stdio: ["pipe", "pipe", "pipe"],
      cwd: allocation.directory,
      env: allocation.env,
    },
  );
  children.add(child);
  child.on("close", () => children.delete(child));
  let output = "";
  child.stdout.on("data", (b) => {
    output += b;
    assert.ok(output.length < 65536);
  });
  child.stderr.resume();
  const exited = once(child, "close");
  await allocation.attach(child.pid);
  child.stdin.end();
  return { child, allocation, exited, output: () => output };
}
const value = async (allocation, file, key) => {
  const text = await readFile(`${parent}/${allocation.name}/${file}`, "utf8");
  return Number(
    text
      .split("\n")
      .find((l) => l.startsWith(key + " "))
      .split(" ")[1],
  );
};
try {
  manager = await createResources(config);
  // A healthy peer remains alive while a native allocation storm is OOM killed.
  const healthy = await launch(
    "console.log('healthy');setTimeout(()=>{},30000)",
  );
  const hog = await launch(
    "const keep=[];setInterval(()=>{keep.push(Buffer.alloc(16*1024*1024,1));},5)",
  );
  const end = await Promise.race([
    hog.exited,
    sleep(10000).then(() => {
      throw Error("OOM enforcement did not trigger");
    }),
  ]);
  assert.equal(end[1], "SIGKILL");
  assert.ok((await value(hog.allocation, "memory.events", "oom_kill")) > 0);
  assert.equal(healthy.child.exitCode, null);
  assert.equal(healthy.child.signalCode, null);
  assert.ok(healthy.output().includes("healthy"));
  pass(
    "per-command memory.max kills an allocating command without killing its peer",
  );
  await hog.allocation.cleanup();
  allocations.delete(hog.allocation);
  const forks =
    await launch(`const {spawn}=require('node:child_process');let denied=0;
    for(let i=0;i<40;i++){const p=spawn('/bin/sleep',['30']);p.on('error',()=>denied++);}
    setTimeout(()=>console.log('denied='+denied),500);setTimeout(()=>{},30000);`);
  for (let i = 0; i < 100 && !forks.output().includes("denied="); i++)
    await sleep(50);
  assert.match(forks.output(), /denied=[1-9]/);
  assert.ok((await value(forks.allocation, "pids.events", "max")) > 0);
  assert.ok(
    Number(
      await readFile(`${parent}/${forks.allocation.name}/pids.current`, "utf8"),
    ) <= 16,
  );
  await forks.allocation.cleanup();
  allocations.delete(forks.allocation);
  await forks.exited;
  pass(
    "pids.max rejects a fork storm; cleanup kills and reaps the entire process tree",
  );
  const cpu = await launch("console.log('spinning');while(true){};");
  for (let i = 0; i < 100 && !cpu.output().includes("spinning"); i++)
    await sleep(25);
  const before = await value(cpu.allocation, "cpu.stat", "usage_usec");
  await sleep(1200);
  const used = (await value(cpu.allocation, "cpu.stat", "usage_usec")) - before;
  assert.ok(used < 400000, `CPU quota not enforced: ${used}`);
  assert.ok((await value(cpu.allocation, "cpu.stat", "nr_throttled")) > 0);
  await cpu.allocation.cleanup();
  allocations.delete(cpu.allocation);
  await cpu.exited;
  pass("cpu.max throttles CPU-bound work independently of healthy commands");
  await healthy.allocation.cleanup();
  allocations.delete(healthy.allocation);
  await healthy.exited;
  // Drop the supervisor's OS lease while leaving a real child and private files.
  const orphan = await launch("setTimeout(()=>{},30000)");
  await writeFile(orphan.allocation.directory + "/secret", "private-orphan");
  await manager.close();
  manager = await createResources(config);
  await orphan.exited;
  allocations.delete(orphan.allocation);
  assert.equal(
    (await readdir(parent)).some((n) => n.startsWith("job-")),
    false,
  );
  assert.equal(
    (await readdir(root)).some((n) => n.startsWith("job-")),
    false,
  );
  pass(
    "restart reaping kills orphan commands before deleting private scratch or admitting new work",
  );
  await manager.close();
  manager = null;
  const crashed = spawn(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `
    import {createResources} from ${JSON.stringify(new URL("../service/resources.mjs", import.meta.url).href)};
    import {spawn} from 'node:child_process';
    import {writeFile} from 'node:fs/promises';
    const manager=await createResources(${JSON.stringify(config)}), job=await manager.allocate();
    const child=spawn(process.execPath,['-e',"process.stdin.resume();process.stdin.on('end',()=>setTimeout(()=>{},30000));"],{stdio:['pipe','ignore','ignore']});
    await job.attach(child.pid);child.stdin.end();
    await writeFile(job.directory+'/private','orphan-secret');
    console.log('ready');setTimeout(()=>{},30000);
  `,
    ],
    { stdio: ["ignore", "pipe", "inherit"] },
  );
  children.add(crashed);
  const crashExit = once(crashed, "close");
  await once(crashed.stdout, "data");
  crashed.kill("SIGKILL");
  await crashExit;
  children.delete(crashed);
  manager = await createResources(config);
  assert.equal(
    (await readdir(parent)).some((n) => n.startsWith("job-")),
    false,
  );
  assert.equal(
    (await readdir(root)).some((n) => n.startsWith("job-")),
    false,
  );
  pass(
    "SIGKILL releases the supervisor lease; restart reaps its live orphan and private files",
  );
  if (process.env.SANDBOX_RESOURCE_EVIDENCE)
    await writeFile(
      process.env.SANDBOX_RESOURCE_EVIDENCE,
      JSON.stringify(evidence, null, 2) + "\n",
    );
} finally {
  for (const allocation of allocations)
    await allocation.cleanup().catch(() => {});
  for (const child of children) child.kill("SIGKILL");
  await manager?.close();
  await rm(root, { recursive: true, force: true });
}
