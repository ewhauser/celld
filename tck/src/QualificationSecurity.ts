// Security-boundary qualification.
//
// Every case pairs an authorized control that must succeed with the denials the
// pinned celld v0.5.0 release documents, then proves that nothing protected
// changed across the denial. The boundaries under test, and the ones that are
// not testable on this release, are recorded in docs/SECURITY-BOUNDARIES.md.
// This is documented-boundary coverage, not a claim of hostile multi-tenant
// isolation.
import { Effect, Schema } from "effect";
import { decodeJson } from "./Artifacts.js";
import { TckError } from "./Domain.js";
import type { Node } from "./FleetControls.js";
import { equal } from "./Oracle.js";
import {
  acknowledgedBatch,
  events,
  sidecarProbe,
  type QualificationContext,
} from "./QualificationContext.js";
import type { Probe } from "./SecurityProbe.js";
import {
  checkAllowed,
  checkApplicationFallthrough,
  checkBodyLimit,
  checkDenied,
  checkForwarded,
  checkInternalNotFound,
  checkPeerDenied,
  checkReservedRefusal,
  checkUnchanged,
  observation,
} from "./SecurityOracles.js";

/** The fixture's own 404 body. Its presence proves application code answered. */
const APPLICATION_NOT_FOUND = "not found";
const APPLICATION_REVISION = '{"revision":"qualification-v1"}';
/** infra/security.yaml pins this ingress limit on every node. */
const BODY_LIMIT = 65536;
/** The one node infra/security.yaml gives a trusted-proxy configuration. */
const TRUSTED: Node = "celld3";
const UNTRUSTED: Node = "celld";
const ECHO = "/echo/url";

const internal = (node: Node) => `http://${node}:8081`;
const publicListener = (node: Node) => `http://${node}:8080`;

const State = Schema.Struct({
  deployment: Schema.Struct({
    cells: Schema.Record(Schema.String, Schema.Int),
  }),
});

/**
 * The state a denial must leave alone: recorded ownership, the acknowledged SQL
 * history, its KV mirror, and the durable-object event log.
 */
const protectedState = (ctx: QualificationContext) =>
  Effect.all({
    owner: ctx.owner(),
    history: ctx.state(),
    kv: ctx.json("/history/kv"),
    events: events(ctx),
  });

const withUnchangedState = <A>(
  ctx: QualificationContext,
  label: string,
  work: Effect.Effect<A, TckError>,
) =>
  Effect.gen(function* () {
    const before = yield* protectedState(ctx);
    const value = yield* work;
    const after = yield* protectedState(ctx);
    yield* ctx.artifacts.json(`${label}-protected-state.json`, {
      before,
      after,
    });
    yield* checkUnchanged(label, before, after);
    yield* ctx.verify();
    return value;
  });

/** Waits until an asynchronous fixture effect has landed, so snapshots are stable. */
const settled = (ctx: QualificationContext, key: string) =>
  ctx.poll(events(ctx), (observed) => observed.some(([name]) => name === key));

/** Operator and peer route prefixes the public listener must never serve. */
const RESERVED_PATHS = [
  "/state",
  "/cell/Recovery:probe",
  "/evict/Recovery:probe",
  "/do/Recovery:probe",
  "/shutdown",
  "/reload",
  "/peer/tunnel",
  "/peer/probe",
  "/runtime/__Queue:probe",
] as const;

const listenerSeparation = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* acknowledgedBatch(ctx, 6);
    return yield* withUnchangedState(
      ctx,
      "listener-separation",
      Effect.gen(function* () {
        const results = yield* sidecarProbe(ctx, "listener-separation", [
          {
            kind: "http",
            label: "control-operator-state",
            url: `${internal(UNTRUSTED)}/state`,
          },
          {
            kind: "http",
            label: "control-application",
            url: `${publicListener(UNTRUSTED)}/deployment/revision`,
          },
          ...RESERVED_PATHS.map((path): Probe => ({
            kind: "http",
            label: `public${path}`,
            url: `${publicListener(UNTRUSTED)}${path}?name=${ctx.name}`,
            ...(path === "/shutdown" || path === "/reload"
              ? { method: "POST" }
              : {}),
          })),
          {
            kind: "http",
            label: "internal-application-path",
            url: `${internal(UNTRUSTED)}/deployment/revision`,
          },
          {
            kind: "http",
            label: "internal-unknown-path",
            url: `${internal(UNTRUSTED)}/tck-unknown-boundary-path`,
          },
          {
            kind: "http",
            label: "internal-public-health",
            url: `${internal(UNTRUSTED)}/.well-known/celld/health`,
          },
        ]);
        // Authorized controls: the operator API answers on its own listener and
        // the application answers on the public one.
        yield* checkAllowed(
          yield* observation(results, "control-operator-state"),
          { status: 200, bodyIncludes: '"deployment"' },
        );
        yield* checkAllowed(
          yield* observation(results, "control-application"),
          {
            status: 200,
            body: APPLICATION_REVISION,
          },
        );
        // The public listener has no operator or peer API at all, so these
        // paths must produce the application's own 404 instead.
        for (const path of RESERVED_PATHS)
          yield* checkApplicationFallthrough(
            yield* observation(results, `public${path}`),
            APPLICATION_NOT_FOUND,
          );
        // An unknown internal path must not invoke application code, including
        // a path the application does serve on the public listener.
        for (const label of [
          "internal-application-path",
          "internal-unknown-path",
          "internal-public-health",
        ])
          yield* checkInternalNotFound(
            yield* observation(results, label),
            APPLICATION_NOT_FOUND,
          );
        return results.length;
      }),
    );
  });

/** Resident cell scopes, read from every node's operator state. */
const residentScopes = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    const results = yield* sidecarProbe(
      ctx,
      "resident-scopes",
      ctx.nodes.map((node): Probe => ({
        kind: "http",
        label: node,
        url: `${internal(node)}/state`,
      })),
    );
    const scopes = new Set<string>();
    for (const node of ctx.nodes) {
      const observed = yield* observation(results, node);
      yield* checkAllowed(observed, { status: 200 });
      const state = yield* decodeJson(State, observed.body ?? "");
      for (const cell of Object.keys(state.deployment.cells)) scopes.add(cell);
    }
    return [...scopes];
  });

/** Materializes the reserved queue cell and returns its scope. */
const queueScope = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* ctx.json("/queue/send?count=1");
    yield* settled(ctx, "event:queue:0");
    const scopes = yield* residentScopes(ctx);
    const scope = scopes.find((cell) => cell.startsWith("__Queue:"));
    return scope === undefined
      ? yield* Effect.fail(
          new TckError({
            phase: "security",
            message: "No resident reserved queue cell",
            detail: scopes.join(","),
          }),
        )
      : scope;
  });

const forgedPeerHeaders = (
  source: Node,
  target: Node,
  timestamp: number,
): Record<string, string> => ({
  "x-cells-peer-version": "5",
  "x-cells-peer-source": source,
  "x-cells-peer-target": target,
  "x-cells-peer-timestamp": String(timestamp),
  "x-cells-peer-nonce": "0".repeat(32),
  "x-cells-peer-body-sha256":
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "x-cells-peer-signature": "0".repeat(64),
});

const PeerCheck = Schema.Struct({
  check: Schema.String,
  detail: Schema.String,
  verdict: Schema.Literal("ok"),
});

const peerAuthentication = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* acknowledgedBatch(ctx, 6);
    const scope = yield* queueScope(ctx);
    yield* ctx.artifacts.json("reserved-scope.json", { scope });
    return yield* withUnchangedState(
      ctx,
      "peer-authentication",
      Effect.gen(function* () {
        // Authorized control: a fleet-signed direct probe against every node.
        const diagnosis = yield* ctx.runtime.controls.compose([
          "run",
          "--rm",
          "-T",
          "--env",
          "S3_ENDPOINT=http://proxy:8082",
          "tool",
          "diagnose",
          "--json",
          ...ctx.nodes.flatMap((node) => ["--peer", node]),
        ]);
        yield* ctx.artifacts.text("signed-peer-probe.jsonl", diagnosis.stdout);
        const signed = diagnosis.stdout
          .split("\n")
          .filter((line) => line.includes('"check":"peer '));
        yield* equal(signed.length, ctx.nodes.length);
        for (const line of signed) {
          const check = yield* decodeJson(PeerCheck, line);
          yield* equal(check.detail.includes("(signed direct probe)"), true);
        }
        const now = Date.now();
        const denied = [
          "probe-missing",
          "probe-bearer",
          "probe-forged",
          "probe-forged-stale",
          "probe-forged-wrong-target",
          ...ctx.nodes.flatMap((node) => [
            `runtime-missing-${node}`,
            `runtime-forged-${node}`,
          ]),
        ];
        const results = yield* sidecarProbe(ctx, "peer-authentication", [
          {
            kind: "http",
            label: "control-unauthenticated-operator",
            url: `${internal(UNTRUSTED)}/cell/Recovery:${ctx.identity.cell}`,
          },
          {
            kind: "http",
            label: "probe-missing",
            url: `${internal(UNTRUSTED)}/peer/probe`,
          },
          {
            kind: "http",
            label: "probe-bearer",
            url: `${internal(UNTRUSTED)}/peer/probe`,
            headers: { authorization: "Bearer tck-not-a-fleet-credential" },
          },
          {
            kind: "http",
            label: "probe-forged",
            url: `${internal(UNTRUSTED)}/peer/probe`,
            headers: forgedPeerHeaders("celld2", UNTRUSTED, now),
          },
          {
            kind: "http",
            label: "probe-forged-stale",
            url: `${internal(UNTRUSTED)}/peer/probe`,
            headers: forgedPeerHeaders("celld2", UNTRUSTED, now - 3_600_000),
          },
          {
            kind: "http",
            label: "probe-forged-wrong-target",
            url: `${internal(UNTRUSTED)}/peer/probe`,
            headers: forgedPeerHeaders("celld2", TRUSTED, now),
          },
          ...ctx.nodes.flatMap((node): Probe[] => [
            {
              kind: "http",
              label: `runtime-missing-${node}`,
              url: `${internal(node)}/runtime/${scope}`,
            },
            {
              kind: "http",
              label: `runtime-forged-${node}`,
              url: `${internal(node)}/runtime/${scope}`,
              headers: forgedPeerHeaders("celld2", node, now),
            },
          ]),
        ]);
        yield* checkAllowed(
          yield* observation(results, "control-unauthenticated-operator"),
          {
            status: 200,
            bodyIncludes: `"cell":"Recovery:${ctx.identity.cell}"`,
          },
        );
        for (const label of denied)
          yield* checkPeerDenied(yield* observation(results, label));
        return { scope, signed: signed.length, denied: denied.length };
      }),
    );
  });

const reservedClasses = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* acknowledgedBatch(ctx, 6);
    const queue = yield* queueScope(ctx);
    // The workflow parks in waitForEvent, so its cell stays resident and its
    // recorded effects stay stable for the duration of the case.
    yield* ctx.json("/flow/start");
    yield* settled(ctx, "event:flow-first");
    const scopes = yield* residentScopes(ctx);
    const workflow = scopes.find((cell) => cell.startsWith("__Workflow."));
    if (workflow === undefined)
      return yield* Effect.fail(
        new TckError({
          phase: "security",
          message: "No resident reserved workflow cell",
          detail: scopes.join(","),
        }),
      );
    const reserved = [queue, workflow];
    yield* ctx.artifacts.json("reserved-scopes.json", { reserved, scopes });
    return yield* withUnchangedState(
      ctx,
      "reserved-classes",
      Effect.gen(function* () {
        const results = yield* sidecarProbe(ctx, "reserved-classes", [
          // Authorized control: the ordinary-object route is unauthenticated
          // and does reach application code.
          {
            kind: "http",
            label: "control-ordinary-object",
            url: `${internal(UNTRUSTED)}/do/Recovery:${ctx.identity.cell}`,
          },
          {
            kind: "http",
            label: "control-application",
            url: `${publicListener(UNTRUSTED)}/deployment/revision`,
          },
          ...reserved.map((scope, index): Probe => ({
            kind: "http",
            label: `reserved-${index}`,
            url: `${internal(UNTRUSTED)}/do/${scope}`,
          })),
          {
            kind: "http",
            label: "ordinary-on-runtime-route",
            url: `${internal(UNTRUSTED)}/runtime/Recovery:${ctx.identity.cell}`,
          },
        ]);
        // The fixture's own 404 proves the unauthenticated ordinary-object
        // route did reach application code.
        yield* checkApplicationFallthrough(
          yield* observation(results, "control-ordinary-object"),
          APPLICATION_NOT_FOUND,
        );
        yield* checkAllowed(
          yield* observation(results, "control-application"),
          {
            status: 200,
            body: APPLICATION_REVISION,
          },
        );
        for (const [index, scope] of reserved.entries())
          yield* checkReservedRefusal(
            yield* observation(results, `reserved-${index}`),
            scope,
          );
        // The converse: an ordinary class is not served on the reserved route.
        yield* checkDenied(
          yield* observation(results, "ordinary-on-runtime-route"),
          {
            status: 403,
            bodyIncludes:
              "only a runtime class is served on /runtime/; use /do/ for a Durable Object",
          },
        );
        return { reserved };
      }),
    );
  });

const HOSTS = [
  { label: "hostname", value: "app.example" },
  { label: "hostname-port", value: "app.example:8080" },
  { label: "ipv4", value: "10.1.2.3:8080" },
  { label: "ipv6", value: "[::1]:8080" },
  { label: "space", value: "bad host" },
  { label: "punctuation", value: "bad_host!!" },
  { label: "empty", value: "" },
] as const;

const forwardedHeaders = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* acknowledgedBatch(ctx, 6);
    return yield* withUnchangedState(
      ctx,
      "forwarded-headers",
      Effect.gen(function* () {
        const results = yield* sidecarProbe(
          ctx,
          "forwarded-headers",
          [UNTRUSTED, TRUSTED].flatMap((node): Probe[] => [
            {
              kind: "http",
              label: `${node}-plain`,
              url: `${publicListener(node)}${ECHO}`,
            },
            {
              kind: "http",
              label: `${node}-forwarded`,
              url: `${publicListener(node)}${ECHO}`,
              headers: {
                "x-forwarded-host": "forwarded.example",
                "x-forwarded-proto": "https",
              },
            },
            {
              kind: "http",
              label: `${node}-forwarded-list`,
              url: `${publicListener(node)}${ECHO}`,
              headers: {
                "x-forwarded-host": "first.example, last.example",
                "x-forwarded-proto": "http, https",
              },
            },
            {
              kind: "http",
              label: `${node}-forwarded-malformed`,
              url: `${publicListener(node)}${ECHO}`,
              headers: {
                "x-forwarded-host": "bad host!!",
                "x-forwarded-proto": "https",
              },
            },
            ...HOSTS.map((host): Probe => ({
              kind: "raw",
              label: `${node}-host-${host.label}`,
              host: node,
              port: 8080,
              path: ECHO,
              hostHeader: host.value,
            })),
          ]),
        );
        for (const node of [UNTRUSTED, TRUSTED]) {
          const trustsForwarded = node === TRUSTED;
          const hostHeader = `${node}:8080`;
          yield* checkForwarded(
            yield* observation(results, `${node}-plain`),
            ECHO,
            { trustsForwarded, hostHeader },
          );
          yield* checkForwarded(
            yield* observation(results, `${node}-forwarded`),
            ECHO,
            {
              trustsForwarded,
              hostHeader,
              forwardedHost: "forwarded.example",
              forwardedProto: "https",
            },
          );
          yield* checkForwarded(
            yield* observation(results, `${node}-forwarded-list`),
            ECHO,
            {
              trustsForwarded,
              hostHeader,
              forwardedHost: "first.example, last.example",
              forwardedProto: "http, https",
            },
          );
          yield* checkForwarded(
            yield* observation(results, `${node}-forwarded-malformed`),
            ECHO,
            {
              trustsForwarded,
              hostHeader,
              forwardedHost: "bad host!!",
              forwardedProto: "https",
            },
          );
          for (const host of HOSTS)
            yield* checkForwarded(
              yield* observation(results, `${node}-host-${host.label}`),
              ECHO,
              { trustsForwarded, hostHeader: host.value },
            );
        }
        return results.length;
      }),
    );
  });

const bodyLimits = (ctx: QualificationContext) =>
  Effect.gen(function* () {
    yield* acknowledgedBatch(ctx, 6);
    // Authorized control on the same route the denials use, recorded in the
    // external ledger so ctx.verify() can prove it survived.
    yield* ctx.writeAcknowledged();
    const id = `${ctx.name}-limit`;
    // JSON.stringify({id,payload}) is exactly 22 punctuation bytes plus both values.
    const payloadBytes = BODY_LIMIT - 22 - id.length;
    return yield* withUnchangedState(
      ctx,
      "body-limits",
      Effect.gen(function* () {
        const write = `${publicListener(UNTRUSTED)}/history/write?name=${ctx.name}`;
        const results = yield* sidecarProbe(ctx, "body-limits", [
          // Exactly at the limit: celld admits the body, and the application —
          // not celld — is what rejects the oversized payload inside it.
          {
            kind: "http",
            label: "at-limit",
            url: write,
            method: "POST",
            headers: { "content-type": "application/json" },
            bodyJson: { id, payloadBytes },
          },
          {
            kind: "http",
            label: "declared-over-limit",
            url: write,
            method: "POST",
            headers: { "content-type": "application/json" },
            bodyJson: { id, payloadBytes: payloadBytes + 1 },
          },
          {
            kind: "http",
            label: "declared-far-over-limit",
            url: write,
            method: "POST",
            headers: { "content-type": "application/json" },
            bodyBytes: BODY_LIMIT * 4,
          },
          {
            kind: "http",
            label: "streamed-over-limit",
            url: write,
            method: "POST",
            headers: { "content-type": "application/json" },
            chunks: { bytes: BODY_LIMIT, count: 4 },
          },
        ]);
        yield* checkDenied(yield* observation(results, "at-limit"), {
          status: 400,
          body: "too large",
        });
        for (const label of [
          "declared-over-limit",
          "declared-far-over-limit",
          "streamed-over-limit",
        ])
          yield* checkBodyLimit(yield* observation(results, label));
        return { limit: BODY_LIMIT, payloadBytes };
      }),
    );
  });

export const securityCases = [
  { id: "security.listener-separation" as const, run: listenerSeparation },
  { id: "security.peer-authentication" as const, run: peerAuthentication },
  { id: "security.reserved-classes" as const, run: reservedClasses },
  { id: "security.forwarded-headers" as const, run: forwardedHeaders },
  { id: "security.body-limits" as const, run: bodyLimits },
].map((test) => ({ ...test, security: true }));
