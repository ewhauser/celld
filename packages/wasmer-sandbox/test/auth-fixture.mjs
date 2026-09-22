import { generateKeyPair, exportJWK, SignJWT } from "jose";
export async function authFixture() {
  const { publicKey, privateKey } = await generateKeyPair("ES256", {
    extractable: true,
  });
  const jwk = {
    ...(await exportJWK(publicKey)),
    kid: "test-key",
    alg: "ES256",
    use: "sig",
  };
  const vars = {
    SANDBOX_AUTH_ISSUER: "https://sandbox-test.invalid",
    SANDBOX_AUTH_AUDIENCE: "sandbox-test",
    SANDBOX_AUTH_JWKS: JSON.stringify({ keys: [jwk] }),
  };
  async function sign(claims = {}, header = {}) {
    const now = Math.floor(Date.now() / 1000);
    return new SignJWT({
      iss: vars.SANDBOX_AUTH_ISSUER,
      aud: vars.SANDBOX_AUTH_AUDIENCE,
      sub: "agent-a",
      tenant: "tenant-a",
      workspace: "test",
      iat: now,
      exp: now + 300,
      ...claims,
    })
      .setProtectedHeader({
        alg: "ES256",
        typ: "sandbox-agent+jwt",
        kid: "test-key",
        ...header,
      })
      .sign(privateKey);
  }
  return { vars, sign };
}
