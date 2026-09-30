# SubtleCrypto does not enforce key usages or key lengths, and misclassifies malformed JWK input

## Version and reproduction

Confirmed on celld **v0.5.0**, compatibility date 2026-07-30, no compatibility flags, with bucket durability on MinIO. Reference: workerd 1.20260730.1 through Miniflare 4.20260730.0. Evidence run `tck-0c3c37cf-d018-4e54-b741-aeec138d7628`.

Each operation below is expected to reject. The fixture records only the observed error name, or `accepted` when the operation resolved.

```ts
const verifyOnly = await crypto.subtle.importKey(
  "raw",
  new TextEncoder().encode("key"),
  { name: "HMAC", hash: "SHA-256" },
  false,
  ["verify"],
);
await crypto.subtle.sign("HMAC", verifyOnly, new Uint8Array(1));
await crypto.subtle.importKey("raw", new Uint8Array(17), "AES-GCM", false, [
  "encrypt",
]);
await crypto.subtle.importKey("raw", new Uint8Array(16), "AES-GCM", false, []);
await crypto.subtle.importKey("raw", new Uint8Array(16), "AES-GCM", false, [
  "sign",
]);
await crypto.subtle.importKey(
  "jwk",
  { kty: "EC", crv: "P-256", x: "!!!", y: "!!!" },
  { name: "ECDSA", namedCurve: "P-256" },
  false,
  ["verify"],
);
```

| Operation                            | workerd              | celld    |
| ------------------------------------ | -------------------- | -------- |
| sign with a `verify`-only key        | `InvalidAccessError` | accepted |
| import a 17-byte AES-GCM key         | `DataError`          | accepted |
| import with an empty usage list      | `SyntaxError`        | accepted |
| import AES-GCM with the `sign` usage | `SyntaxError`        | accepted |
| import a malformed EC JWK            | `DataError`          | `Error`  |

Four controls behave identically on both engines and are part of the same case: an unsupported algorithm name and an unsupported digest name both reject with `NotSupportedError`, exporting a non-extractable key rejects with `InvalidAccessError`, and AES-GCM with an empty IV rejects with `OperationError`.

Run `pnpm tck --profile local --suite core --case crypto.invalid-input` to reproduce.

## Contract

The Web Cryptography API requires `sign`/`verify`/`encrypt`/`decrypt` to throw `InvalidAccessError` when the key's `usages` do not include the requested operation, requires `importKey` to throw `SyntaxError` when `keyUsages` is empty or contains a usage the algorithm does not support, and requires `DataError` when the key data is the wrong length or cannot be parsed for the requested format. The [Workers Web Crypto API](https://developers.cloudflare.com/workers/runtime-apis/web-crypto/) implements the standard operations, and [celld's compatibility page](https://celld.dev/docs/cloudflare-compat/) documents algorithm restrictions only — no relaxation of key-usage or key-length validation.

## Impact and repair boundary

Key usages stop acting as a capability restriction: a key imported only for verification can produce signatures, and a key imported for the wrong algorithm class is silently accepted. An AES key of an invalid length is accepted at import and fails later, if at all, which turns a deterministic argument error into a runtime failure further from its cause. Error-class-based handling for malformed key material does not fire because the rejection is a generic `Error`.

Usage and usage-list validation, key-length validation, and the JWK parse error class are independently reviewable. The observations here do not define behavior for RSA key lengths, wrap/unwrap usages, `deriveKey`/`deriveBits` usage checks, or non-`oct`/non-EC JWK key types.
