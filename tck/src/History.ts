import { Effect, FileSystem, Schema, Semaphore } from "effect";
import { Artifacts, decodeJson } from "./Artifacts.js";
import { equal } from "./Oracle.js";
import { TckError } from "./Domain.js";
export const HistoryRow = Schema.Struct({
  seq: Schema.Int,
  id: Schema.String,
  payload: Schema.String,
  kv: Schema.String,
});
export const Receipt = Schema.Struct({
  seq: Schema.Int,
  id: Schema.String,
  payload: Schema.String,
  activation: Schema.String,
});
export const LedgerEvent = Schema.Struct({
  kind: Schema.Literals(["intent", "ack", "uncertain", "observed"]),
  id: Schema.String,
  payload: Schema.String,
  at: Schema.Number,
  seq: Schema.optionalKey(Schema.Int),
  activation: Schema.optionalKey(Schema.String),
  node: Schema.String,
});
export type LedgerEvent = typeof LedgerEvent.Type;
export const readLedger = (
  path: string,
  options: { allowEmpty?: boolean } = {},
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const text = yield* fs.readFileString(path);
    if (!text.endsWith("\n") && !(options.allowEmpty && text.length === 0))
      return yield* Effect.fail(
        new TckError({
          phase: "ledger",
          message: "Truncated ledger; preserve and inspect before resuming",
        }),
      );
    return yield* Effect.forEach(text.split("\n").filter(Boolean), (line) =>
      decodeJson(LedgerEvent, line),
    );
  });

// Transport and decoding are shared; callers own semantic checks and evidence writes.
export const readHistory = <E, R>(
  json: (path: string) => Effect.Effect<unknown, E, R>,
) =>
  Effect.gen(function* () {
    const rows: Array<typeof HistoryRow.Type> = [];
    for (let page = 0; page < 100; page++) {
      const next = yield* json(
        `/history/state?after=${rows.at(-1)?.seq ?? 0}`,
      ).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(HistoryRow))),
      );
      rows.push(...next);
      if (next.length < 128) return rows;
    }
    return yield* Effect.fail(
      new TckError({
        phase: "history",
        message: "History exceeded 12800 rows",
      }),
    );
  });
export const readHistoryKv = <E, R>(
  json: (path: string) => Effect.Effect<unknown, E, R>,
) =>
  json("/history/kv").pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(HistoryKv)),
  );

// A surviving JSONL file is the authority. Flush each entry before proceeding.
export const makeLedger = (name: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const artifacts = yield* Artifacts;
    const lock = yield* Semaphore.make(1);
    const path = `${artifacts.directory}/${name}.jsonl`;
    const append = (entry: LedgerEvent) =>
      lock.withPermit(
        Effect.scoped(
          Effect.gen(function* () {
            const file = yield* fs.open(path, { flag: "a" });
            yield* file.writeAll(
              new TextEncoder().encode(JSON.stringify(entry) + "\n"),
            );
            yield* file.sync;
          }),
        ),
      );
    const read = () =>
      readLedger(path, { allowEmpty: true }).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
      );
    return { append, read, path };
  });
export const checkHistory = (
  events: readonly LedgerEvent[],
  rows: readonly (typeof HistoryRow.Type)[],
) =>
  Effect.gen(function* () {
    const intents = new Map<string, LedgerEvent>();
    const completed = new Map<string, LedgerEvent>();
    for (const event of events) {
      if (event.kind === "intent") {
        yield* equal(intents.has(event.id), false);
        intents.set(event.id, event);
      } else if (event.kind === "observed") {
        yield* equal(intents.has(event.id), true);
        yield* equal(event.payload, intents.get(event.id)!.payload);
      } else {
        yield* equal(intents.has(event.id), true);
        yield* equal(completed.has(event.id), false);
        yield* equal(event.payload, intents.get(event.id)!.payload);
        completed.set(event.id, event);
      }
    }
    const seen = new Set<string>();
    for (const [index, row] of rows.entries()) {
      yield* equal(row.seq, index + 1);
      yield* equal(seen.has(row.id), false);
      seen.add(row.id);
      const intent = intents.get(row.id);
      yield* equal(!!intent, true);
      yield* equal(row.payload, intent!.payload);
      yield* equal(row.kv, row.payload);
    }
    const acknowledged = [...completed.values()].filter(
      (event) => event.kind === "ack",
    );
    for (const ack of [
      ...acknowledged,
      ...events.filter((event) => event.kind === "observed"),
    ]) {
      yield* equal(Number.isInteger(ack.seq) && ack.seq! > 0, true);
      yield* equal(ack.at >= intents.get(ack.id)!.at, true);
      const row = rows.find((row) => row.id === ack.id);
      yield* equal(row?.seq, ack.seq);
      yield* equal(row?.payload, ack.payload);
      if (ack.kind === "ack") yield* equal(typeof ack.activation, "string");
      // Respect real-time precedence: an operation begun after an acknowledgment or read
      // must occupy a later position. Concurrent operations may serialize either way.
      for (const later of rows)
        if (intents.get(later.id)!.at > ack.at)
          yield* equal(later.seq > ack.seq!, true);
    }
    return {
      attempted: intents.size,
      acknowledged: acknowledged.length,
      recovered: rows.length,
      uncertain: intents.size - acknowledged.length,
    };
  });

export const checkFencedReceipts = (
  events: readonly LedgerEvent[],
  oldActivation: string,
  takeoverAt: number,
) =>
  equal(
    events.filter(
      (event) =>
        event.kind === "ack" &&
        event.at >= takeoverAt &&
        event.activation === oldActivation,
    ),
    [],
  );

export const HistoryKv = Schema.Array(
  Schema.Tuple([Schema.String, Schema.String]),
);
export const checkHistoryKv = (
  rows: readonly (typeof HistoryRow.Type)[],
  kv: readonly (readonly string[])[],
) =>
  equal(
    [...kv].sort((a, b) => a[0]!.localeCompare(b[0]!)),
    rows
      .map((row) => [`history:${row.id}`, row.payload])
      .sort((a, b) => a[0]!.localeCompare(b[0]!)),
  );
