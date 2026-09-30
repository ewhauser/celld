# R2 listing omits a readable trailing-slash object

On celld v0.6.0, writing six distinct keys containing leading, trailing and repeated slashes, Unicode, literal percent signs and `%2F` preserves all six bodies on `get()`. Listing the prefix omits the trailing-slash key (`case/a/b/`). Pinned workerd 1.20260730.1 lists all six keys. The key is readable, so this observation does not establish data loss.

Run `pnpm tck --profile local --case r2.key-identity --known-bugs error`. The fixture writes `case/a/b`, `case/a//b`, `case/a/b/`, `case/λ/%`, `case/a/%2F`, and `/case/a/b` under a unique case namespace, reads every value, lists both prefixes without a delimiter, then deletes the first key and verifies its empty-segment sibling. Cleanup deletes all six keys. See `fixtures/core/Services.ts`.

Evidence: `tck-b0165822-cc98-4b73-a9f5-5ef6cb8d88fe`, compatibility date 2026-07-30, no flags, MinIO backend. The [R2 API contract](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/) and [v0.6.0 release claims](https://github.com/denoland/celld/releases/tag/v0.6.0) supply the semantic expectation, independently observed on workerd. The entire exact candidate observation is version-scoped in the bug registry. Wrong bodies, changed key identity, other missing keys or deletion aliasing are not waived. This report is a local draft, not an upstream submission.
