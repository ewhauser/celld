# EC public key export returns SPKI for the `raw` format and omits JWK metadata

## Version and reproduction

Confirmed on celld **v0.5.0**, compatibility date 2026-07-30, no compatibility flags, with bucket durability on MinIO. Reference: workerd 1.20260730.1 through Miniflare 4.20260730.0. Evidence run `tck-0c3c37cf-d018-4e54-b741-aeec138d7628`.

Generate an extractable P-256 key pair, export the public key in both formats, and re-import each exported form as a verify-only key.

```ts
const curve = { name: "ECDSA", namedCurve: "P-256" } as const;
const pair = await crypto.subtle.generateKey(curve, true, ["sign", "verify"]);
const jwk = await crypto.subtle.exportKey("jwk", pair.publicKey);
const raw = await crypto.subtle.exportKey("raw", pair.publicKey);
await crypto.subtle.importKey("raw", raw, curve, true, ["verify"]);
```

Observed on celld, against the workerd reference:

| Observation                      | workerd                     | celld               |
| -------------------------------- | --------------------------- | ------------------- |
| `exportKey("raw")` byte length   | 65                          | 91                  |
| First exported byte              | `0x04` (uncompressed point) | not `0x04`          |
| Re-import of that value as `raw` | succeeds and verifies       | `NotSupportedError` |
| `jwk.ext`                        | `true`                      | absent              |
| `jwk.key_ops`                    | `["verify"]`                | absent              |

Signing, verification, the tampered-signature rejection, the 64-byte P-1363 signature width, the JWK coordinate sizes, key usages, key types, and the JWK export/import round trip all match the reference. 91 bytes is the DER SubjectPublicKeyInfo length for a P-256 public key, so the `raw` format appears to be served by the SPKI encoder.

Run `pnpm tck --profile local --suite core --case crypto.ecdsa-p256` to reproduce.

## Contract

The [Workers Web Crypto API](https://developers.cloudflare.com/workers/runtime-apis/web-crypto/) supports ECDSA `importKey`/`exportKey`. Under the Web Cryptography API, the `raw` format of an elliptic-curve public key is the uncompressed point encoding from SEC 1, and `spki` is the separate DER format. `ext` and `key_ops` are required members of an exported JWK for an extractable key with usages. [celld's compatibility page](https://celld.dev/docs/cloudflare-compat/) documents ECDSA support restricted to P-256 with SHA-256 and documents no export-format difference for public keys; the only documented export restriction is that a secret key cannot use `jwk` with `exportKey()` or `wrapKey()`.

## Impact and repair boundary

Code that exchanges EC public keys as uncompressed points (JWT/JWS verification helpers, key pinning, peer key registries) receives an incompatible encoding and cannot re-import its own exported key. Applications that inspect `ext` or `key_ops` on an exported JWK see them as undefined.

Fixing the `raw` encoder for EC public keys and re-enabling `raw` import is independent of the missing JWK metadata; treat them as two reviewable changes. This observation does not describe `pkcs8`, `spki`, compressed points, or curves other than P-256, and it does not establish behavior for private-key export.
