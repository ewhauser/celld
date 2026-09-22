import { createLocalJWKSet, jwtVerify } from "jose";
import { identifier } from "./protocol.ts";
import { check, SandboxError } from "./storage.ts";

export interface AuthorizationEnv {
  WORKSPACES: any;
  SANDBOX_AUTH_ISSUER?: string;
  SANDBOX_AUTH_AUDIENCE?: string;
  /** JSON public JWKS pinned by the operator, never supplied by a request. */
  SANDBOX_AUTH_JWKS?: string;
  SANDBOX_NATIVE_FILESYSTEM?: string;
  SANDBOX_CALLBACK_TOKEN?: string;
}

function verifier(env: AuthorizationEnv) {
  check(
    typeof env.SANDBOX_AUTH_ISSUER === "string" &&
      env.SANDBOX_AUTH_ISSUER.length > 0 &&
      typeof env.SANDBOX_AUTH_AUDIENCE === "string" &&
      env.SANDBOX_AUTH_AUDIENCE.length > 0 &&
      typeof env.SANDBOX_AUTH_JWKS === "string",
    "ECONFIG",
  );
  try {
    const jwks = JSON.parse(env.SANDBOX_AUTH_JWKS);
    check(
      Array.isArray(jwks.keys) &&
        jwks.keys.length > 0 &&
        jwks.keys.length <= 16,
    );
    const ids = new Set();
    for (const key of jwks.keys) {
      check(
        key.kty === "EC" &&
          key.crv === "P-256" &&
          key.alg === "ES256" &&
          key.use === "sig" &&
          typeof key.kid === "string" &&
          key.kid.length > 0 &&
          !ids.has(key.kid) &&
          !Object.hasOwn(key, "d") &&
          typeof key.x === "string" &&
          typeof key.y === "string",
      );
      ids.add(key.kid);
    }
    return createLocalJWKSet(jwks);
  } catch {
    throw new SandboxError("ECONFIG");
  }
}

/** This canonical tuple is a name, never a caller-chosen Durable Object ID. */
export function workspaceName(
  issuer: string,
  tenant: string,
  agent: string,
  workspace: string,
): string {
  return JSON.stringify(["sandbox-agent-v1", issuer, tenant, agent, workspace]);
}

export async function authorizedWorkspace(
  request: Request,
  env: AuthorizationEnv,
) {
  const parts = new URL(request.url).pathname.split("/");
  check(parts.length === 5 && parts[1] === "v1" && parts[2] === "workspaces");
  const workspace = identifier(parts[3]);
  if (parts[4] === "fs") {
    // The HTTP reference backend is an internal service surface. Native mode
    // exposes no callback route, even if a callback credential was configured.
    check(env.SANDBOX_NATIVE_FILESYSTEM === "0", "EAUTH");
    const expected = env.SANDBOX_CALLBACK_TOKEN;
    check(typeof expected === "string" && expected.length >= 32, "ECONFIG");
    check(
      request.headers.get("authorization") === `Bearer ${expected}`,
      "EAUTH",
    );
    check(/^[a-f0-9]{64}$/.test(workspace), "EAUTH");
    return env.WORKSPACES.idFromString(workspace);
  }
  const key = verifier(env);
  const header = request.headers.get("authorization");
  check(
    header && header.length <= 8192 && /^Bearer [^\s]+$/.test(header),
    "EAUTH",
  );
  try {
    const { payload, protectedHeader } = await jwtVerify(header.slice(7), key, {
      algorithms: ["ES256"],
      typ: "sandbox-agent+jwt",
      issuer: env.SANDBOX_AUTH_ISSUER,
      audience: env.SANDBOX_AUTH_AUDIENCE,
      requiredClaims: [
        "iss",
        "aud",
        "sub",
        "tenant",
        "workspace",
        "iat",
        "exp",
      ],
      maxTokenAge: 300,
      clockTolerance: 0,
    });
    check(typeof protectedHeader.kid === "string", "EAUTH");
    const tenant = identifier(payload.tenant),
      agent = identifier(payload.sub);
    check(identifier(payload.workspace) === workspace, "EAUTH");
    check(
      Number.isSafeInteger(payload.iat) &&
        Number.isSafeInteger(payload.exp) &&
        payload.exp! > payload.iat! &&
        payload.exp! - payload.iat! <= 300,
      "EAUTH",
    );
    return env.WORKSPACES.idFromName(
      workspaceName(payload.iss!, tenant, agent, workspace),
    );
  } catch {
    // No issuer, key, signature, claim or workspace existence details escape.
    throw new SandboxError("EAUTH");
  }
}
