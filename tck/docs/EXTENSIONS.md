# Assets, dynamic Workers, and facets

The extensions fixture exercises three platform features that are not part of the core Worker or Durable Object surface: static assets, Worker Loader (dynamic Worker) bindings, and Durable Object facets. Every case is a differential API case: the same fixture runs on workerd and on celld, and the driver owns the expectations.

Expectations follow the Cloudflare reference contracts and the [celld compatibility page](https://celld.dev/docs/cloudflare-compat/). A documented celld difference is registered as a divergence rather than removed from the reference expectation.

## Static assets

The fixture ships `hello.txt`, `page.html`, `folder/index.html`, `shadowed.txt`, `asset-first/note.txt`, a `_headers` file, and a `_redirects` file. Binding observations go through the `ASSETS` binding with `redirect: "manual"` so redirect responses stay visible.

| Case                  | Required behavior                                                                                                                                                                                                                                                                            |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `assets.binding`      | `ASSETS.fetch()` serves a UTF-8 file with its `_headers` rule applied and answers 404 for a missing file.                                                                                                                                                                                    |
| `assets.html-routing` | Default [HTML handling](https://developers.cloudflare.com/workers/static-assets/routing/advanced/html-handling/): `/page` serves `page.html`, `/page.html` and `/folder` answer 307 to the canonical path, `/folder/` serves the directory index, and an unmatched path is 404 with no body. |
| `assets.redirects`    | [`_redirects`](https://developers.cloudflare.com/workers/static-assets/routing/advanced/redirects/) rules produce 301 and 302 responses with the configured `Location`, and an unlisted path is unaffected.                                                                                  |
| `assets.worker-first` | [`run_worker_first`](https://developers.cloudflare.com/workers/static-assets/binding/#run_worker_first) route patterns: a worker-first path runs the Worker even though an asset exists there, and a `!/` pattern restores asset-first order for its prefix.                                 |

### Asset configuration

`assets.worker-first` is the only case whose observations come from the public listener rather than the `ASSETS` binding, because static routing is decided before any fixture code runs. The fixture declares `"run_worker_first": ["/*", "!/asset-first/*"]`, so every other endpoint still reaches the Worker. Both `/shadowed.txt` and `/asset-first/note.txt` have a Worker route _and_ an asset; only the routing rules decide which one answers.

`FixtureConfig` now accepts `run_worker_first` as a boolean or an array, plus `html_handling` and `not_found_handling`. `ReferenceProcess` translates them the way Wrangler does: a boolean becomes `invoke_user_worker_ahead_of_assets`, an array becomes `static_routing` with the `!`-prefixed rules moved to `asset_worker`, and the two handling options become Miniflare's `assetConfig`.

## Dynamic Workers

The fixture loads one child bundle through the `LOADER` binding. A `Gateway` `WorkerEntrypoint` in the same script provides a Fetcher that the loaded Worker can reach, either as a service capability in its `env` or as its `globalOutbound`.

| Case               | Required behavior                                                                                                                                                                                                                                               |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `dynamic.fetch`    | A loaded Worker's default entrypoint answers a request.                                                                                                                                                                                                         |
| `dynamic.props`    | `getEntrypoint(undefined, { props })` delivers per-call props to the loaded Worker's `ctx.props`; a second call on the same loaded Worker sees its own props, not the first call's.                                                                             |
| `dynamic.bindings` | `WorkerCode.env` carries structured-clone values and a Service Binding capability that the loaded Worker can call.                                                                                                                                              |
| `dynamic.outbound` | [Egress control](https://developers.cloudflare.com/dynamic-workers/usage/egress-control/): `globalOutbound: null` makes a global `fetch()` throw while an `env` service capability still works, and a `globalOutbound` Fetcher intercepts the global `fetch()`. |
| `dynamic.limits`   | [Custom resource limits](https://developers.cloudflare.com/dynamic-workers/usage/limits/): a `WorkerCode.limits` declaration is accepted, and the loaded Worker still serves a request whose work stays inside the declared budget.                             |

The loaded Worker reports only whether the blocked call threw. The error class is not part of the documented contract, so it is not asserted.

`dynamic.limits` observes acceptance only. Enforcement is not observable on the pinned reference: workerd 1.20260730.1 ran five distinct loaded Workers concurrently from one Worker request (`tck-c19a7418-790d-4893-8a57-79ffd736a0ec`) and allowed three subrequests under `limits: { subRequests: 2 }` (`tck-5d4b1683-155d-4e2b-9632-6c7bde8fe185`), so neither the [concurrency limit](https://developers.cloudflare.com/dynamic-workers/platform/limits/) nor a custom subrequest budget can be asserted locally.

## Facets

Facets require a Durable Object class obtained from a Worker Loader binding, which is how the fixture creates them on both runtimes.

| Case                          | Required behavior                                                                                                                                                                             |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `facets.isolation`            | Two named facets of the same root object keep separate SQLite state.                                                                                                                          |
| `facets.transaction`          | An explicit facet transaction reads its own uncommitted write, a commit makes the new value visible to later reads, and `rollback()` discards the transaction's write entirely.               |
| `facets.outbound-transaction` | A facet writes and then calls out through a Service Binding, once with no root transaction open and once inside a root `storage.transaction()`; the facet write is still readable afterwards. |

Facet persistence across a restart is a lifecycle property, so it is covered by `recovery.facets` in the [recovery suite](RECOVERY.md) rather than here: the extensions suite has no lifecycle handle.

## Running the suite

```sh
pnpm tck --profile reference --suite extensions
pnpm tck --profile local --suite extensions
pnpm tck --profile reference --suite extensions --case assets.redirects
```

Each case has an independently captured positive observation in `test/case-oracles/observations.json` and named semantic mutations in `test/case-oracles/Mutations.ts`: a lost `_headers` rule, an ignored `_redirects` rule, a suppressed trailing-slash redirect, a worker-first path that serves the shadowed asset, a negative routing rule that fails to restore asset-first order, leaked props, a lost env capability, an outbound block that leaks or that also disables bindings, and a facet transaction that commits a rolled back write or discards a committed one. The former facet outbound restriction has a dedicated semantic mutation, so it cannot return unnoticed.

An asset error page's body and media type are presentation rather than contract, so only a served asset reports them; the not-found status itself is asserted.

## Local celld results

On celld v0.6.0 every case above passes except `assets.html-routing`, which remains the known bug [CELL-004](BUGS.md): celld redirects `/folder` to `/folder/` and then answers 404 for that canonical path, so a directory index is unreachable. Every other observation in that case matches the reference.

`facets.outbound-transaction` now matches workerd: both the control call and the call inside a root transaction reach the gateway, and the facet write remains readable afterwards. v0.6.0 gives each facet its own database and replication stream. The old v0.5.1 divergence and its waiver-only observations have been retired; the independent workerd observation and semantic negative tests remain. See [RELEASE-0.6.0.md](RELEASE-0.6.0.md) for run evidence.

## Remaining coverage

Still open, with the reason each item was not implemented:

- **Asset-only deployments.** celld documents that "an asset-only project can omit `main`", but the extensions fixture is a single Worker deployment; a second, Worker-less deployment would need its own bundle, its own celld deployment, and its own Miniflare instance. Out of scope for a differential API case on the shared fixture.
- **`not_found_handling` and `html_handling`.** The plumbing accepts both, but each is a single per-deployment setting, and the fixture's existing `assets.html-routing` case asserts the defaults. Exercising a non-default value needs a second deployment for the same reason as above.
- **`_headers` protocol-header restrictions.** celld documents that "a `_headers` file cannot change `connection`, `content-length`, or `transfer-encoding`", but Cloudflare's [`_headers` documentation](https://developers.cloudflare.com/workers/static-assets/headers/) still lists no restricted headers, so there is no reference contract to assert against. Unchanged from the previous pass.
- **Dynamic Worker limit enforcement and generation lifecycle.** See the note under the Dynamic Workers table: the pinned workerd enforces neither documented limit locally. The loader callback "may be called any number of times", so the generation lifecycle has no assertable contract either.
- **Facet eviction and multi-node ownership moves.** `recovery.facets` covers persistence across a graceful restart on a single node. Eviction while the node stays up, and a facet's behavior during an ownership move, belong to the multi-node suites.

The open items stay unticked in the [roadmap](ROADMAP.md).
