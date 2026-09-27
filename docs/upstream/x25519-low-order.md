# X25519 low-order rejection uses Error instead of OperationError

On celld v0.6.0, `crypto.x25519` derives the correct RFC 7748 shared secret and round-trips Bob's raw public key. Deriving against a 32-byte zero public key is rejected, but its error name is `Error`; pinned workerd 1.20260730.1 reports `OperationError`. This is an error-class compatibility defect, not acceptance of the low-order point.

Run `pnpm tck --profile local --case crypto.x25519 --known-bugs error`. The fixture imports Alice's RFC 7748 private key as PKCS8, imports the peer as raw X25519, then calls `deriveBits({ name: "X25519", public: peer }, privateKey, 256)`. See `fixtures/core/Curves.ts` for the exact vectors. Compatibility date: 2026-07-30, no flags.

Evidence: `tck-b0165822-cc98-4b73-a9f5-5ef6cb8d88fe`. Both observations remain in the report. The [Web Crypto contract](https://developers.cloudflare.com/workers/runtime-apis/web-crypto/) and the independent workerd result supply the oracle. The default registry accepts only the entire recorded candidate observation at version 0.6.0; a changed vector, accepted low-order key, or changed exception still fails. This report is a local draft, not an upstream submission.
