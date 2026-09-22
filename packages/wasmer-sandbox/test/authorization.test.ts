import test from "node:test";
import assert from "node:assert/strict";
// @ts-ignore Test-only Node fixture.
import { authFixture } from "./auth-fixture.mjs";
import { authorizedWorkspace, workspaceName } from "../src/authorization.ts";
const fixture = await authFixture();
const env = {
  ...fixture.vars,
  WORKSPACES: { idFromName: (s: string) => s, idFromString: (s: string) => s },
};
const req = (token?: string, workspace = "test", action = "read") =>
  new Request(`http://localhost/v1/workspaces/${workspace}/${action}`, {
    headers: token
      ? {
          authorization: `Bearer ${token}`,
          "x-tenant": "tenant-b",
          "x-agent": "agent-b",
        }
      : {},
  });
const denied = (r: Request, e = env) =>
  assert.rejects(authorizedWorkspace(r, e), { code: "EAUTH" });

test("identity binds every action to a canonical tenant/agent/workspace tuple", async () => {
  const token = await fixture.sign();
  const expected = workspaceName(
    env.SANDBOX_AUTH_ISSUER,
    "tenant-a",
    "agent-a",
    "test",
  );
  for (const action of [
    "read",
    "write",
    "list",
    "stat",
    "mkdir",
    "rename",
    "unlink",
    "rmdir",
    "exec",
    "status",
    "cancel",
  ]) {
    assert.equal(
      await authorizedWorkspace(req(token, "test", action), env),
      expected,
    );
    await denied(req(token, "other", action));
    await denied(req(token, "a".repeat(64), action));
  }
  for (const claims of [
    { tenant: "tenant-b" },
    { sub: "agent-b" },
    { workspace: "other" },
  ]) {
    assert.notEqual(
      await authorizedWorkspace(
        req(await fixture.sign(claims), claims.workspace ?? "test"),
        env,
      ),
      expected,
    );
  }
  assert.notEqual(
    workspaceName("issuer", "a_b", "c", "x"),
    workspaceName("issuer", "a", "b_c", "x"),
  );
});

test("missing, forged, malformed, expired and wrong-purpose credentials fail closed", async () => {
  await denied(req());
  await denied(req("internal-service-token".repeat(3)));
  await denied(req("e".repeat(8193)));
  for (const alg of ["none", "HS256", "RS256"]) {
    const forged = [
      Buffer.from(
        JSON.stringify({ alg, typ: "sandbox-agent+jwt", kid: "test-key" }),
      ).toString("base64url"),
      Buffer.from("{}").toString("base64url"),
      "AA",
    ].join(".");
    await denied(req(forged));
  }

  const now = Math.floor(Date.now() / 1000);
  for (const claims of [
    { exp: now - 1 },
    { nbf: now + 100 },
    { iat: now + 60 },
    { exp: now + 301 },
    { exp: undefined },
    { iat: undefined },
    { iss: "wrong" },
    { aud: "wrong" },
    { sub: undefined },
    { tenant: undefined },
    { tenant: [] },
    { workspace: undefined },
  ])
    await denied(req(await fixture.sign(claims)));
  for (const header of [{ typ: "JWT" }, { kid: "unknown" }])
    await denied(req(await fixture.sign({}, header)));
  await denied(req(await (await authFixture()).sign()));
  const token = await fixture.sign();
  const [head, , sig] = token.split(".");
  const forged = `${head}.${Buffer.from(JSON.stringify({ tenant: "tenant-b", sub: "agent-b", workspace: "test" })).toString("base64url")}.${sig}`;
  await denied(req(forged));
});

test("missing and invalid configuration fails closed; private keys are rejected", async () => {
  for (const vars of [
    { SANDBOX_AUTH_JWKS: undefined },
    { SANDBOX_AUTH_ISSUER: "" },
    { SANDBOX_AUTH_JWKS: "{}" },
    {
      SANDBOX_AUTH_JWKS: JSON.stringify({
        keys: [{ ...JSON.parse(env.SANDBOX_AUTH_JWKS).keys[0], d: "private" }],
      }),
    },
  ])
    await assert.rejects(
      authorizedWorkspace(req(await fixture.sign()), { ...env, ...vars }),
      { code: "ECONFIG" },
    );
});

test("HTTP callbacks are explicit, raw-ID-only and separate from agent credentials", async () => {
  const token = "c".repeat(64),
    id = "a".repeat(64);
  const callbackEnv = {
    ...env,
    SANDBOX_NATIVE_FILESYSTEM: "0",
    SANDBOX_CALLBACK_TOKEN: token,
  };
  assert.equal(
    await authorizedWorkspace(req(token, id, "fs"), callbackEnv),
    id,
  );
  await denied(req(token, id, "fs"), {
    ...callbackEnv,
    SANDBOX_NATIVE_FILESYSTEM: "1",
  });
  await denied(req(token, "test", "fs"), callbackEnv);
  await denied(
    req(await fixture.sign({ workspace: id }), id, "fs"),
    callbackEnv,
  );
  await denied(req(token, "test", "read"), callbackEnv);
});
