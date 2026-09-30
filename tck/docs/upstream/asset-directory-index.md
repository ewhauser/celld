# Static assets do not serve a directory index for a trailing-slash path

## Version and reproduction

Observed on celld v0.5.0, compatibility date 2026-07-30, with bucket durability on MinIO. Reference: workerd 1.20260730.1 through Miniflare 4.20260730.0.

The deployment contains `page.html` and `folder/index.html`. Inside the Worker, request each path through the `ASSETS` binding without following redirects:

```ts
env.ASSETS.fetch(
  new Request("https://fixture.test/folder/", { redirect: "manual" }),
);
```

| Request      | workerd                  | celld                   |
| ------------ | ------------------------ | ----------------------- |
| `/page`      | 200, serves `page.html`  | 200, serves `page.html` |
| `/page.html` | 307 to `/page`           | 307 to `/page`          |
| `/folder`    | 307 to `/folder/`        | 307 to `/folder/`       |
| `/folder/`   | 200, serves `index.html` | **404**                 |
| `/missing`   | 404                      | 404                     |

celld redirects `/folder` to the canonical `/folder/` and then answers 404 for that same canonical path, so a directory index is unreachable through the binding.

```sh
pnpm tck --profile reference --suite extensions --case assets.html-routing
pnpm tck --profile local --suite extensions --case assets.html-routing
```

## Contract

[HTML handling](https://developers.cloudflare.com/workers/static-assets/routing/advanced/html-handling/) with the default `auto-trailing-slash` serves `/folder/index.html` for `/folder/` and redirects `/folder` to it. celld lists static assets as supported and its [compatibility page](https://celld.dev/docs/cloudflare-compat/#static-assets) records only compression, edge cache, `_headers` protocol-header, deployment-size, and `.assetsignore` limitations; it does not document a difference in HTML handling. The trailing-slash redirect that celld already emits shows the canonical form is computed, so only the lookup for the index file appears to be missing.

No source investigation was performed for this report; the cause inside celld is unconfirmed.

## Impact

Any deployment whose pages are directory indexes (`about/index.html`) is unreachable through the assets binding, and the emitted redirect leads to a 404. Single-file pages, `_redirects` rules, `_headers` rules, and the 404 path itself behave as on workerd.

The other `assets.html-routing` observations match the reference, so a repair should keep the existing redirect behavior and add the index lookup for a trailing-slash path. Confirm `/folder/index.html` itself redirects to `/folder/` before treating the fix as complete; that request is not covered by this case.
