import assert from "node:assert/strict";

export function agentClient(url, auth) {
  return async (agent, action, body = {}) => {
    const r = await fetch(`${url}/v1/workspaces/test/${action}`, {
      method: "POST",
      headers: { authorization: `Bearer ${await auth.sign({ sub: agent })}` },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(30000),
    });
    const value = await r.json();
    assert.ok(r.ok, `${r.status}: ${JSON.stringify(value)}`);
    return value;
  };
}
export const agents = ["agent-a", "agent-isolation-b"];
export async function seedAgentState(url, auth) {
  const call = agentClient(url, auth);
  for (const agent of agents)
    await call(agent, "write", {
      path: "/workspace/agent-private",
      data: Buffer.from(`canary-${agent}`).toString("base64"),
    });
}
export async function verifyAgentState(url, auth, record, label) {
  const call = agentClient(url, auth);
  for (const agent of agents) {
    const file = await call(agent, "read", {
      path: "/workspace/agent-private",
    });
    assert.equal(
      Buffer.from(file.data, "base64").toString(),
      `canary-${agent}`,
    );
  }
  record(`agent-private files remain separated ${label}`);
}
export async function concurrentAgentChecks(url, auth, record) {
  await seedAgentState(url, auth);
  const call = agentClient(url, auth);
  const execute = (agent, id, mode) =>
    call(agent, "exec", {
      id,
      tool: "guest",
      args: [mode],
      env: { AGENT_SECRET: `canary-${agent}` },
      timeoutMs: 15000,
    });
  const pending = agents.map((a) =>
    execute(a, "identical-agent-command", "agent-hold"),
  );
  // Wait for both actual guests, not merely both journal admissions, to overlap.
  await Promise.all(
    agents.map(async (agent) => {
      for (let i = 0; i < 200; i++) {
        try {
          if (
            (await call(agent, "read", { path: "/workspace/agent-ready" }))
              .data === Buffer.from("ready").toString("base64")
          )
            return;
        } catch {}
        await new Promise((r) => setTimeout(r, 25));
      }
      throw new Error("both agents did not start concurrently");
    }),
  );
  await call(agents[0], "cancel", { id: "identical-agent-command" });
  const [a, b] = await Promise.all(pending);
  assert.equal(a.status, "cancelled");
  assert.equal(b.status, "succeeded", JSON.stringify(b));
  assert.equal(b.result.stdout, `canary-${agents[1]}\n`);
  assert.equal(b.result.stderr, `private-stderr-canary-${agents[1]}\n`);
  assert.equal(
    (await call(agents[0], "status", { id: "identical-agent-command" })).status,
    "cancelled",
  );
  assert.equal(
    (await call(agents[1], "status", { id: "identical-agent-command" })).result
      .stdout,
    b.result.stdout,
  );
  for (const agent of agents) {
    const next = await execute(agent, "fresh-after-cancel", "agent-check");
    assert.equal(next.status, "succeeded", JSON.stringify(next));
    assert.equal(next.result.stdout, `canary-${agent}\n`);
  }
  record(
    "concurrent real guests isolate identical paths, private temporary/cache files, environment, command IDs, stdout/stderr and cancellation",
  );
}
