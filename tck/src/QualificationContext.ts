import { pollUntil, waitForReady } from "./Polling.js";
import { Effect, Schema } from "effect";
import { Artifacts, decodeJson } from "./Artifacts.js";
import {
  attemptRequest,
  leaseLapse,
  TckError,
  Transport,
  type Target,
} from "./Domain.js";
import type { Probe } from "./SecurityProbe.js";
import { ProbeResult } from "./SecurityOracles.js";
import type { acquireLocal } from "./Local.js";
import { equal } from "./Oracle.js";
import {
  checkHistory,
  checkHistoryKv,
  readHistory,
  readHistoryKv,
  makeLedger,
  Receipt,
} from "./History.js";
import type { Node } from "./FleetControls.js";
export type QualificationContext = Effect.Success<
  ReturnType<typeof makeContext>
>;
export const makeContext = (
  runtime: Effect.Success<ReturnType<typeof acquireLocal>>,
  name: string,
  seed: number,
  /**
   * The node the cell is activated on. A new cell is placed where its first
   * request arrives, so this is how a scenario decides which node owns it.
   */
  home: Node = "celld",
) =>
  Effect.gen(function* () {
    const transport = yield* Transport;
    const artifacts = yield* Artifacts;
    const fleet = runtime.fleet;
    const nodes: Node[] = ["celld", "celld2", "celld3"];
    const targets: Record<Node, Target> = {
      celld: yield* fleet.target("celld"),
      celld2: yield* fleet.target("celld2"),
      celld3: yield* fleet.target("celld3"),
    };
    const request = (
      path: string,
      node: Node = "celld",
      method: "GET" | "POST" = "GET",
      body?: string,
    ) =>
      transport.request(targets[node], {
        path: `${path}${path.includes("?") ? "&" : "?"}name=${name}`,
        method,
        ...(body === undefined
          ? {}
          : { body, headers: { "content-type": "application/json" } }),
      });
    const json = (
      path: string,
      node: Node = "celld",
      method: "GET" | "POST" = "GET",
    ) =>
      request(path, node, method).pipe(
        Effect.tap((r) => equal(r.status, 200)),
        Effect.map((r) => r.body),
      );
    const ready = (node: Node) =>
      waitForReady(
        transport.request(targets[node], { path: "/.well-known/celld/health" }),
        { interval: "500 millis", attempts: 61, timeout: "45 seconds" },
      ).pipe(Effect.asVoid);
    for (const node of nodes) yield* ready(node);
    const identity = yield* json("/fleet/id", home).pipe(
      Effect.flatMap(
        Schema.decodeUnknownEffect(
          Schema.Struct({
            cell: Schema.String,
            activation: Schema.String,
            revision: Schema.String,
          }),
        ),
      ),
    );
    const owner = () => fleet.owner(identity.cell);
    const ledger = yield* makeLedger(name);
    let nextId = 0;
    /**
     * One ledgered write, reporting what was observed rather than only whether
     * it was acknowledged: an operational scenario has to say which request it
     * sent, when, and how it ended.
     *
     * `delayMs` holds the request open inside the owning object before it
     * commits, so an operation can be started while this write is in flight.
     */
    const attemptWrite = (node: Node = "celld", delayMs = 0) =>
      Effect.gen(function* () {
        const id = `${name}-${++nextId}`;
        const payload = `payload:${seed}:${id}:λ`;
        const startedAtMs = Date.now();
        yield* ledger.append({
          kind: "intent",
          id,
          payload,
          at: startedAtMs,
          node,
        });
        const result = yield* attemptRequest(
          request(
            delayMs > 0 ? `/history/write?delay=${delayMs}` : "/history/write",
            node,
            "POST",
            JSON.stringify({ id, payload }),
          ),
        );
        const completedAtMs = Date.now();
        const base = { id, payload, node, startedAtMs, completedAtMs };
        if (
          "response" in result &&
          result.response.status >= 200 &&
          result.response.status < 300
        ) {
          yield* equal(result.response.status, 200);
          const receipt = yield* Schema.decodeUnknownEffect(Receipt)(
            result.response.body,
          );
          yield* equal(
            { id: receipt.id, payload: receipt.payload },
            { id, payload },
          );
          yield* ledger.append({
            kind: "ack",
            ...receipt,
            at: completedAtMs,
            node,
          });
          return {
            ...base,
            acknowledged: true,
            status: result.response.status,
            receiptId: receipt.id,
          };
        }
        yield* ledger.append({
          kind: "uncertain",
          id,
          payload,
          at: completedAtMs,
          node,
        });
        return {
          ...base,
          acknowledged: false,
          ...("response" in result
            ? { status: result.response.status }
            : { error: result.error }),
        };
      });
    const write = (node: Node = "celld", delayMs = 0) =>
      attemptWrite(node, delayMs).pipe(
        Effect.map((result) => result.acknowledged),
      );
    const state = (node: Node = "celld") =>
      readHistory((path) => json(path, node));
    const verify = (node: Node = "celld") =>
      Effect.gen(function* () {
        // Reopen and parse persisted evidence instead of trusting the in-memory writer.
        const events = yield* ledger.read();
        const rows = yield* state(node);
        const counts = yield* checkHistory(events, rows);
        // Persist read evidence before returning: uncertainty ends once a write is observed.
        yield* Effect.forEach(
          rows,
          (row) =>
            ledger.append({
              kind: "observed",
              id: row.id,
              payload: row.payload,
              seq: row.seq,
              at: Date.now(),
              node,
            }),
          { discard: true },
        ).pipe(Effect.uninterruptible);
        const kv = yield* readHistoryKv((path) => json(path, node));
        yield* checkHistoryKv(rows, kv);
        const intents = new Map(
          events.filter((e) => e.kind === "intent").map((e) => [e.id, e]),
        );
        const latencies = events
          .filter((e) => e.kind === "ack")
          .map((e) => e.at - intents.get(e.id)!.at)
          .sort((a, b) => a - b);
        const summary = {
          ...counts,
          latencyMs: {
            p50: latencies[Math.floor(latencies.length * 0.5)] ?? null,
            p95: latencies[Math.floor(latencies.length * 0.95)] ?? null,
            max: latencies.at(-1) ?? null,
          },
          durationMs: events.length ? events.at(-1)!.at - events[0]!.at : 0,
        };
        yield* artifacts.json(`${name}-${node}-history.json`, {
          summary,
          rows,
          kv,
        });
        return summary;
      });
    const start = (node: Node) =>
      Effect.gen(function* () {
        targets[node] = yield* fleet.start(node);
        yield* ready(node);
      });
    const crash = (node: Node) =>
      Effect.gen(function* () {
        yield* fleet.kill(node);
        yield* leaseLapse;
        yield* start(node);
      });
    const startAll = () =>
      Effect.forEach(nodes, start, { concurrency: "unbounded", discard: true });
    // Fault scenarios may already have lost nodes; stop whatever survived,
    // let every lease lapse, then bring the whole fleet back together.
    const restartAll = () =>
      Effect.gen(function* () {
        for (const node of nodes)
          if ((yield* fleet.inspect(node)).State.Running)
            yield* fleet.kill(node);
        yield* leaseLapse;
        yield* startAll();
      });
    const writeAcknowledged = (node: Node = "celld", delayMs = 0) =>
      write(node, delayMs).pipe(Effect.flatMap((ack) => equal(ack, true)));
    const poll = <A>(
      effect: Effect.Effect<A, unknown>,
      done: (a: A) => boolean,
    ) =>
      pollUntil(effect, done, {
        interval: "500 millis",
        attempts: 121,
        timeout: "90 seconds",
        message: "Waiting for asynchronous completion",
      });
    const proxy = yield* runtime.controls.proxy();
    const mode = (mode: string, ms: number) =>
      transport
        .request(proxy, { path: `/mode?mode=${mode}&ms=${ms}` })
        .pipe(Effect.tap((r) => equal(r.status, 200)));
    return {
      runtime,
      fleet,
      nodes,
      targets,
      name,
      seed,
      artifacts,
      request,
      json,
      identity,
      owner,
      ledger,
      write,
      attemptWrite,
      writeAcknowledged,
      state,
      verify,
      start,
      startAll,
      restartAll,
      crash,
      ready,
      poll,
      mode,
      proxy,
      transport,
    };
  });

/**
 * Runs probes inside the sidecar container. celld's peer and operator listener
 * is never published to the host, so this is the only way to reach it, and the
 * sidecar is also where a raw request line or a streamed body can be produced.
 * The probe asserts nothing; pure oracles classify every observation.
 */
export const sidecarProbe = (
  ctx: QualificationContext,
  name: string,
  probes: readonly Probe[],
) =>
  Effect.gen(function* () {
    const output = yield* ctx.runtime.controls.compose([
      "exec",
      "-T",
      "proxy",
      "node",
      "/fixture/security-probe.mjs",
      JSON.stringify(probes),
    ]);
    yield* ctx.artifacts.json(`${name}-requests.json`, probes);
    yield* ctx.artifacts.text(`${name}-probes.jsonl`, output.stdout);
    const results: ProbeResult[] = [];
    for (const line of output.stdout.split("\n"))
      if (line.trim()) results.push(yield* decodeJson(ProbeResult, line));
    if (results.length !== probes.length)
      return yield* Effect.fail(
        new TckError({
          phase: "probe",
          message: `Expected ${probes.length} observations, saw ${results.length}`,
        }),
      );
    return results as readonly ProbeResult[];
  });

export const events = (ctx: QualificationContext) =>
  ctx
    .json("/events")
    .pipe(
      Effect.flatMap(
        Schema.decodeUnknownEffect(
          Schema.Array(Schema.Tuple([Schema.String, Schema.Int])),
        ),
      ),
    );
export const acknowledgedBatch = (ctx: QualificationContext, count: number) =>
  Effect.forEach(
    Array.from({ length: count }),
    (_, i) => ctx.writeAcknowledged(ctx.nodes[i % 3]!),
    { concurrency: 4, discard: true },
  );
export const traffic = (ctx: QualificationContext, count: number) =>
  Effect.forEach(
    Array.from({ length: count }),
    (_, i) =>
      ctx
        .write(ctx.nodes[i % 3]!)
        .pipe(Effect.andThen(Effect.sleep("30 millis"))),
    { concurrency: 4, discard: true },
  );
