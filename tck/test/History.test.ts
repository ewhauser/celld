import { NodeServices } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, FileSystem } from "effect";
import { artifactsLayer } from "../src/Artifacts.js";
import {
  checkHistory,
  readHistory,
  checkFencedReceipts,
  checkHistoryKv,
  makeLedger,
  type LedgerEvent,
} from "../src/History.js";
const intent = (id: string, at: number): LedgerEvent => ({
  kind: "intent",
  id,
  at,
  payload: `p-${id}`,
  node: "celld",
});
const ack = (id: string, seq: number, at: number): LedgerEvent => ({
  ...intent(id, at),
  kind: "ack",
  seq,
  activation: "owner",
});
const row = (id: string, seq: number) => ({
  id,
  seq,
  payload: `p-${id}`,
  kv: `p-${id}`,
});
it.effect(
  "history oracle rejects lost acknowledgments, stale ordering, duplicates and partial transactions",
  () =>
    Effect.gen(function* () {
      const history = [
        intent("a", 1),
        ack("a", 1, 2),
        intent("b", 3),
        ack("b", 2, 4),
        intent("c", 5),
      ];
      yield* checkHistory(history, [row("a", 1), row("b", 2)]);
      yield* checkHistory(history, [row("a", 1), row("b", 2), row("c", 3)]);
      for (const rows of [
        [row("a", 1)],
        [row("b", 1), row("a", 2)],
        [row("a", 1), row("a", 2)],
        [row("a", 1), { ...row("b", 2), kv: "bad" }],
        [row("a", 1), row("b", 2), row("unknown", 3)],
      ])
        expect((yield* Effect.exit(checkHistory(history, rows)))._tag).toBe(
          "Failure",
        );
      // Uncertain writes are still ordered after completed earlier operations.
      expect(
        (yield* Effect.exit(
          checkHistory(history, [row("c", 1), row("a", 2), row("b", 3)]),
        ))._tag,
      ).toBe("Failure");
    }),
);
it.effect(
  "ledger reopens persisted intents and acknowledgments and rejects truncation",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      yield* Effect.gen(function* () {
        const first = yield* makeLedger("history");
        yield* first.append(intent("a", 1));
        yield* first.append(ack("a", 1, 2));
        yield* first.append(intent("b", 3));
        const restarted = yield* makeLedger("history");
        yield* checkHistory(yield* restarted.read(), [
          row("a", 1),
          row("b", 2),
        ]);
        yield* first.append({ ...intent("b", 4), kind: "observed", seq: 2 });
        const audited = yield* makeLedger("history");
        expect(
          (yield* Effect.exit(
            checkHistory(yield* audited.read(), [row("a", 1)]),
          ))._tag,
        ).toBe("Failure");
        yield* fs.writeFileString(first.path, '{"partial":', { flag: "a" });
        expect((yield* Effect.exit(restarted.read()))._tag).toBe("Failure");
      }).pipe(Effect.provide(artifactsLayer(directory)));
    }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect(
  "a resumed old activation cannot acknowledge after takeover even if data happens to survive",
  () =>
    Effect.gen(function* () {
      yield* checkFencedReceipts(
        [ack("a", 1, 2), { ...ack("b", 2, 5), activation: "new-owner" }],
        "owner",
        4,
      );
      expect(
        (yield* Effect.exit(
          checkFencedReceipts([ack("a", 1, 2), ack("b", 2, 5)], "owner", 4),
        ))._tag,
      ).toBe("Failure");
    }),
);

it.effect(
  "history KV scan rejects orphaned, missing, duplicate and corrupt keys",
  () =>
    Effect.gen(function* () {
      const rows = [row("a", 1)];
      yield* checkHistoryKv(rows, [["history:a", "p-a"]]);
      for (const kv of [
        [],
        [["history:a", "wrong"]],
        [
          ["history:a", "p-a"],
          ["history:b", "p-b"],
        ],
        [
          ["history:a", "p-a"],
          ["history:a", "p-a"],
        ],
      ])
        expect((yield* Effect.exit(checkHistoryKv(rows, kv)))._tag).toBe(
          "Failure",
        );
    }),
);

it.effect("observed uncertain writes cannot disappear or change sequence", () =>
  Effect.gen(function* () {
    const events: LedgerEvent[] = [
      intent("a", 1),
      { ...intent("a", 2), kind: "uncertain" },
      intent("b", 3),
    ];
    yield* checkHistory(events, []);
    const observed = [
      ...events,
      { ...intent("a", 4), kind: "observed" as const, seq: 1 },
    ];
    yield* checkHistory(observed, [row("a", 1)]);
    for (const rows of [[], [row("b", 1), row("a", 2)]])
      expect((yield* Effect.exit(checkHistory(observed, rows)))._tag).toBe(
        "Failure",
      );
    expect(
      (yield* Effect.exit(
        checkHistory(
          [
            ...observed,
            { ...intent("a", 5), kind: "observed" as const, seq: 2 },
          ],
          [row("a", 1)],
        ),
      ))._tag,
    ).toBe("Failure");
  }),
);

it.effect(
  "history reader follows cursors, rejects malformed pages and bounds endless pagination",
  () =>
    Effect.gen(function* () {
      const paths: string[] = [];
      const first = Array.from({ length: 128 }, (_, i) =>
        row(String(i), i + 1),
      );
      const read = (path: string) =>
        Effect.sync(() => {
          paths.push(path);
          return paths.length === 1 ? first : [row("last", 129)];
        });
      const rows = yield* readHistory(read);
      expect(rows).toHaveLength(129);
      expect(paths).toEqual([
        "/history/state?after=0",
        "/history/state?after=128",
      ]);
      let calls = 0;
      const invalid = yield* readHistory(() =>
        Effect.sync(() => {
          calls++;
          return [{ seq: "bad" }];
        }),
      ).pipe(Effect.exit);
      expect(invalid._tag).toBe("Failure");
      expect(calls).toBe(1);
      calls = 0;
      const endless = yield* readHistory(() =>
        Effect.sync(() => {
          calls++;
          return first;
        }),
      ).pipe(Effect.exit);
      expect(endless._tag).toBe("Failure");
      expect(calls).toBe(100);
    }),
);
