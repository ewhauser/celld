import { NodeServices } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, FileSystem } from "effect";
import { auditLedger } from "../src/Audit.js";
import { artifactsLayer } from "../src/Artifacts.js";
import { Transport } from "../src/Domain.js";
it.effect(
  "resumed audit reads ledger and target without issuing mutations",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped();
      const ledger = `${directory}/ledger.jsonl`;
      yield* fs.writeFileString(
        ledger,
        [
          {
            kind: "intent",
            id: "op",
            payload: "payload",
            at: 1,
            node: "celld",
          },
          {
            kind: "observed",
            id: "op",
            payload: "payload",
            at: 2,
            node: "celld",
            seq: 1,
            activation: "before-crash",
          },
        ]
          .map((e) => JSON.stringify(e))
          .join("\n") + "\n",
      );
      const reads: string[] = [];
      const audit = auditLedger({
        ledger,
        endpoint: "http://dedicated-test.invalid",
        name: "owned-object",
      }).pipe(
        Effect.provide(artifactsLayer(directory)),
        Effect.provideService(Transport, {
          request: (_target, spec) =>
            Effect.sync(() => {
              expect(spec.method ?? "GET").toBe("GET");
              reads.push(spec.path);
              return {
                status: 200,
                headers: {},
                body: spec.path.startsWith("/history/kv")
                  ? [["history:op", "payload"]]
                  : [{ seq: 1, id: "op", payload: "payload", kv: "payload" }],
              };
            }),
          websocket: () => Effect.die("audit must not open sockets"),
        }),
      );
      yield* audit;
      expect(reads).toEqual([
        "/history/state?name=owned-object&after=0",
        "/history/kv?name=owned-object",
      ]);
      expect(yield* fs.exists(`${directory}/ledger-audit.json`)).toBe(true);
      const recorded = yield* fs.readFileString(ledger);
      yield* fs.writeFileString(ledger, recorded.replace('"seq":1', '"seq":2'));
      expect((yield* Effect.exit(audit))._tag).toBe("Failure");
      const beforeTruncation = reads.length;
      yield* fs.writeFileString(ledger, '{"truncated":');
      expect((yield* Effect.exit(audit))._tag).toBe("Failure");
      expect(reads.length).toBe(beforeTruncation);
    }).pipe(Effect.provide(NodeServices.layer)),
);
