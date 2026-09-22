import assert from "node:assert/strict";
export async function authorizationChecks({
  url,
  auth,
  agentToken,
  workspaceId,
  token,
  callbackToken,
  record,
}) {
  const b = await auth.sign({ sub: "agent-b" }),
    otherTenant = await auth.sign({ tenant: "tenant-b" });
  async function post(key, action, body = {}, alias = "test", direct) {
    return fetch(
      url +
        (direct
          ? `/__direct/${direct}/${alias}/${action}`
          : `/v1/workspaces/${alias}/${action}`),
      {
        method: "POST",
        headers: {
          ...(key ? { authorization: `Bearer ${key}` } : {}),
          "x-agent-id": "agent-a",
          "x-tenant-id": "tenant-a",
        },
        body: JSON.stringify(body),
      },
    );
  }
  async function call(key, action, body) {
    const r = await post(key, action, body);
    const data = await r.json();
    assert.ok(r.ok, JSON.stringify(data));
    return data;
  }
  const idB = (
    await (
      await fetch(url + "/__auth-id", {
        headers: { authorization: `Bearer ${b}` },
      })
    ).json()
  ).id;
  assert.notEqual(idB, workspaceId);
  await call(agentToken, "write", {
    path: "/workspace/private",
    data: Buffer.from("agent-a-secret").toString("base64"),
  });
  for (const key of [b, otherTenant]) {
    assert.equal(
      (await post(key, "read", { path: "/workspace/private" })).status,
      404,
    );
    await call(key, "write", {
      path: "/workspace/private",
      data: Buffer.from("different-private").toString("base64"),
    });
  }
  assert.equal(
    Buffer.from(
      (await call(agentToken, "read", { path: "/workspace/private" })).data,
      "base64",
    ).toString(),
    "agent-a-secret",
  );
  const operations = {
    read: { path: "/workspace/private" },
    write: { path: "/workspace/private", data: "eA==" },
    list: { path: "/workspace" },
    stat: { path: "/workspace/private" },
    mkdir: { path: "/workspace/evil" },
    rename: { path: "/workspace/private", to: "/workspace/stolen" },
    unlink: { path: "/workspace/private" },
    rmdir: { path: "/workspace" },
    exec: { id: "private-command", tool: "guest", args: ["exit"] },
    status: { id: "private-command" },
    cancel: { id: "private-command" },
  };
  for (const [action, body] of Object.entries(operations)) {
    for (const alias of ["other", workspaceId])
      assert.equal((await post(b, action, body, alias)).status, 401, action);
    for (const key of [b, otherTenant, undefined, token, callbackToken]) {
      assert.equal(
        (await post(key, action, body, "test", workspaceId)).status,
        401,
        `direct ${action}`,
      );
    }
    for (const key of [undefined, token, callbackToken])
      assert.equal(
        (await post(key, action, body)).status,
        401,
        `credential ${action}`,
      );
  }
  const now = Math.floor(Date.now() / 1000);
  for (const key of [
    await auth.sign({ exp: now - 1 }),
    await auth.sign({ aud: "wrong" }),
    await (await (await import("./auth-fixture.mjs")).authFixture()).sign(),
  ])
    assert.equal((await post(key, "list", { path: "/workspace" })).status, 401);
  assert.equal(
    (await post(b, "fs", { op: "heartbeat" }, workspaceId)).status,
    401,
  );
  const hexAlias = await auth.sign({ sub: "agent-b", workspace: workspaceId });
  assert.equal(
    (await post(hexAlias, "read", { path: "/workspace/private" }, workspaceId))
      .status,
    404,
  );
  assert.equal(
    (
      await post(
        hexAlias,
        "read",
        { path: "/workspace/private" },
        workspaceId,
        workspaceId,
      )
    ).status,
    401,
  );
  record(
    "real router and direct DO reject cross-agent IDs, aliases, headers and service credentials for every action",
  );
  const own = await call(agentToken, "exec", {
    id: "private-command",
    tool: "guest",
    args: ["exit"],
  });
  assert.equal(own.result.exitCode, 42);
  assert.equal(await call(b, "status", { id: "private-command" }), null);
  await call(b, "cancel", { id: "private-command" });
  assert.equal(
    (await call(agentToken, "status", { id: "private-command" })).result
      .exitCode,
    42,
  );
  assert.equal(
    (
      await call(b, "exec", {
        id: "private-command",
        tool: "guest",
        args: ["exit"],
      })
    ).result.exitCode,
    42,
  );
  record(
    "agent command results and identical command IDs are isolated; authorized execution works",
  );
  const pending = call(agentToken, "exec", {
    id: "private-running",
    tool: "guest",
    args: ["wait"],
    timeoutMs: 20000,
  });
  for (let i = 0; i < 100; i++) {
    if (
      (await call(agentToken, "status", { id: "private-running" }))?.status ===
      "running"
    )
      break;
    await new Promise((r) => setTimeout(r, 20));
  }
  assert.equal(
    (await post(b, "cancel", { id: "private-running" }, "test", workspaceId))
      .status,
    401,
  );
  await call(b, "cancel", { id: "private-running" });
  assert.equal(
    (await call(agentToken, "status", { id: "private-running" })).status,
    "running",
  );
  await call(agentToken, "cancel", { id: "private-running" });
  assert.equal((await pending).status, "cancelled");
  for (const key of [agentToken, b, otherTenant])
    await call(key, "unlink", { path: "/workspace/private" });
  record(
    "cross-agent cancellation cannot interrupt another agent; own cancellation succeeds",
  );
}
