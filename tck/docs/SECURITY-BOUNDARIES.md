# Security boundaries

Five local qualification cases exercise the boundaries celld documents for the
pinned [v0.5.0](https://github.com/denoland/celld/blob/v0.5.0/docs/README.md)
release, against the [security documentation](https://celld.dev/docs/security/).
They run in a dedicated `security` CI job and results matrix column.

celld states plainly that it is an alpha product and unsuitable for mutually
distrusting tenants. These cases verify the boundaries it does document and the
status codes it publishes. They are not a claim of hostile multi-tenant
isolation, and they are not a penetration test.

## Inventory on v0.5.0

Established by reading `celld --help` from the pinned image, probing a live
three-node fleet on both listeners, and reading the fleet bucket. Everything
below is what the pinned binary actually does, not only what the docs say.

### Listeners

| Listener | Flag                                        | Serves                                                                                                 |
| -------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Public   | `--listen` (`CELLD_ADDR`)                   | The deployed Worker, plus the reserved `/.well-known/celld/health`                                     |
| Internal | `--internal-listen` (`CELLD_INTERNAL_ADDR`) | Peer protocol and the operator API; `--help` calls it the "peer and unauthenticated operator listener" |

The compose fleet publishes only `8080` to the host. `8081` exists solely inside
the Compose network, which is why every probe in this suite runs from the
sidecar container rather than from the harness process.

Observed responses on the public listener for every operator and peer path
(`/state`, `/cell/<SCOPE>`, `/evict/<SCOPE>`, `/do/<ID>`, `POST /shutdown`,
`POST /reload`, `/peer/tunnel`, `/peer/probe`, `/runtime/<SCOPE>`): `404` with
the fixture's own body, i.e. the request became an ordinary application request.
There is no operator API on the public listener to deny.

Observed responses on the internal listener for unknown paths, including
`/deployment/revision` which the application does serve publicly, and including
the public listener's own `/.well-known/celld/health`: `404` with
`{"error":"not_found"}`. Application code is never invoked.

### Operator API

| Route              | Authentication | Observed                                                            |
| ------------------ | -------------- | ------------------------------------------------------------------- |
| `/state`           | none           | `200`, node state JSON including `deployment.cells`                 |
| `/cell/<SCOPE>`    | none           | `200`, `{"route":"local","cell":"..."}`                             |
| `/evict/<SCOPE>`   | none           | `200`, `{"ok":true}`                                                |
| `/do/<SCOPE>`      | none           | Dispatches to the object; the fixture answered it                   |
| `POST /shutdown`   | none           | Graceful handoff                                                    |
| `/peer/tunnel`     | fleet HMAC     | `400 tunnel establishment must upgrade` before authentication       |
| `/peer/probe`      | fleet HMAC     | `401 peer authentication failed`                                    |
| `/runtime/<SCOPE>` | fleet HMAC     | `401 peer authentication failed` for a reserved class on every node |

`/do/<ID>` takes a scope, not a path: `/do/Recovery:<hex>/anything` is rejected
with `400 cell id is not a well-formed scope`.

### Peer authentication

The fleet HMAC key is stored in the fleet bucket at `fleet/peer-auth.json`
(`{"version":1,"key":"<64 hex>"}`); every node with bucket credentials holds it.
The request headers the binary uses are `x-cells-peer-version`,
`x-cells-peer-source`, `x-cells-peer-target`, `x-cells-peer-timestamp`,
`x-cells-peer-nonce`, `x-cells-peer-body-sha256` and `x-cells-peer-signature`,
over a `cells-peer-request-v1` domain. Rejections carry
`x-cells-peer-version: 5` and the body `peer authentication failed`.

`celld diagnose --peer NODE` issues a genuine fleet-signed probe and reports
`(signed direct probe)` with verdict `ok`. That is the authorized control for
this boundary.

### Reserved runtime classes

Reserved cells are `__`-prefixed scopes; the fixture materializes
`__Queue:<hex>` and `__Workflow.celld-tck-core:<hex>`. Observed:

- `/do/__Queue:<hex>` &rarr; `403 {"error":"__Queue is a runtime class and is not reachable over /do/; use \`celld queue\`"}`
- `/do/__Workflow.celld-tck-core:<hex>` &rarr; `403 {"error":"__Workflow.celld-tck-core is a runtime class and is not reachable over /do/; use the workflow binding"}`
- `/runtime/Recovery:<hex>` &rarr; `403 {"error":"only a runtime class is served on /runtime/; use /do/ for a Durable Object"}`
- `/runtime/__Queue:<hex>` without credentials &rarr; `401`, on all three nodes,
  owner or not. The class check runs before authentication; authentication runs
  before any routing to the cell.

### Forwarded headers and host handling

`--trust-forwarded-headers` / `CELLD_TRUST_FORWARDED_HEADERS=1`. Observed:

- Untrusted node: `X-Forwarded-Host` and `X-Forwarded-Proto` are ignored
  completely; `request.url` keeps the request's own host and `http`.
- Trusted node: the **last** value of each header wins, so
  `X-Forwarded-Host: first.example, last.example` yields `last.example`.
- Trusted node with a malformed forwarded host: the forwarded host is dropped
  and the request's own `Host` is used, while a valid forwarded proto still
  applies.
- `Host` accepts a hostname, a hostname with port, dotted IPv4, and bracketed
  IPv6, each preserved verbatim in `request.url`.
- A malformed or empty `Host` (`bad host`, `bad_host!!`, ``) falls back to
  `celld.local`.

Two values the docs' word "noncanonical" might cover are **accepted** on
v0.5.0 and preserved verbatim: an uppercase host (`APP.Example`) and a
trailing-dot host (`app.example.`). The published wording is not precise enough
to call either a deviation, so the suite records them in this inventory and does
not assert on them.

### Request body limits

`CELLD_MAX_REQUEST_BODY_BYTES`, default 1 GiB. The suite pins it to 64 KiB. The
threshold is exact and celld-side:

- 65,536 declared bytes: admitted; the application receives the body.
- 65,537 declared bytes: `413 request body too large`.
- A chunked body with no declared length that exceeds the limit while being
  read: `413 request body too large`.

## Cases

All five run on a three-node celld v0.5.0 fleet with MinIO and the storage
proxy, using the standard qualification fixture plus `infra/security.yaml`.

- `security.listener-separation`: authorized controls are `/state` on the
  internal listener and `/deployment/revision` on the public one. Nine operator
  and peer paths on the public listener must return the application's own 404,
  and three paths on the internal listener — including one the application
  serves publicly — must return celld's `{"error":"not_found"}`.
- `security.peer-authentication`: the authorized controls are a fleet-signed
  `celld diagnose --peer` probe against all three nodes and an unauthenticated
  operator `/cell/<SCOPE>` resolve. Eleven denied requests — missing
  credentials, a bearer token, a structurally complete forged `x-cells-peer-*`
  set, the same set with an hour-old timestamp, the same set aimed at another
  node, and `/runtime/<reserved>` with and without forged credentials on every
  node — must all return `401 peer authentication failed`.
- `security.reserved-classes`: the authorized control is the unauthenticated
  `/do/<Recovery scope>` route, which must reach application code. `/do/` on the
  reserved queue and workflow scopes must be refused with the documented 403 and
  its class name, and an ordinary class on `/runtime/` must be refused too.
- `security.forwarded-headers`: one fleet, two policies. `celld` and `celld2`
  use the default policy and `celld3` trusts forwarded headers, with an
  identical fixture on all three. Twenty-two probes cover plain requests,
  single and multi-valued forwarded headers, a malformed forwarded host, and
  seven raw-socket `Host` values against both policies.
- `security.body-limits`: the authorized control is an ordinary acknowledged
  ledger write. A body exactly at the limit must reach the application, which
  rejects its oversized payload with its own `400`; a body one byte over, a body
  four times over, and a chunked body with no declared length must each return
  `413 request body too large`.

Every case snapshots ownership, the acknowledged SQL history, its KV mirror, and
the durable-object event log before and after its denials, requires them to be
identical, and then runs the full ledger verification. Every probe request and
response is written to the case's evidence directory as
`<case>-requests.json` and `<case>-probes.jsonl`.

## Compose overlay

`infra/security.yaml` is layered after `infra/qualification.yaml` and makes
three minimal changes, each of which the cases depend on:

- `CELLD_MAX_REQUEST_BODY_BYTES: "65536"` on all three nodes, so the documented
  413 can be reached without streaming a gibibyte.
- `CELLD_TRUST_FORWARDED_HEADERS: "1"` on `celld3` only, so one fleet observes
  both forwarded-header policies.
- The `tool` service joins the peer network, so `celld diagnose --peer` can
  reach the advertised peer addresses and act as the signed control.

The fixture gains one observation-only Worker route, `/echo/url`, which returns
`request.url` and the `Host` header. It is identical on every node and makes no
authorization decision of its own.

## Probes

`src/SecurityProbe.ts` is bundled into the fixture directory as
`security-probe.mjs` and executed inside the sidecar container, the same way
`StorageProxy.ts` is. It is needed because the internal listener is not
published to the host, and because the `Host` and body-limit cases need control
of the raw request line and of a streamed body that the harness `Transport`
deliberately does not offer. It asserts nothing; every observation is classified
by the pure oracles in `src/SecurityOracles.ts`, which are tested against
deliberately wrong observations in `test/SecurityOracles.test.ts` — an accepted
forged request, a 2xx on a denied path, a changed owner record, and a transport
error offered as evidence of a denial all fail.

## Not testable on v0.5.0

- **Expired and replayed peer credentials.** Both require a _valid_ credential
  to age or resend. v0.5.0 documents neither the canonical signing input for
  `cells-peer-request-v1` nor any supported way to mint, inject, or age a test
  credential, and it exposes no facility to capture one. The binary does carry
  `peer replay rejected` and `peer replay cache is full` strings, so the
  mechanism exists; reaching it would mean reverse-engineering the wire format
  from a stripped release binary, which would test an assumption rather than a
  documented contract. Missing and forged credentials are covered.
- **Noncanonical host rejection.** See the forwarded-header inventory above: the
  published wording does not define which forms are noncanonical, and the forms
  most likely meant are accepted on v0.5.0.
- **Operator-listener exposure policy.** `--unsafe-public-advertise` and the
  guidance to restrict the internal listener to trusted operators are deployment
  requirements, not behaviours the binary enforces; there is no rejection to
  assert.

## Running

```sh
pnpm tck --profile local --suite security
pnpm tck --profile local --suite security --case security.body-limits
```

Local validation uses three celld v0.5.0 nodes and MinIO on Docker. It does not
qualify AWS, managed Cloudflare, or any deployment whose internal listener is
reachable from an untrusted network.
