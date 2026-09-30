import { Console, Effect } from "effect";
import { Artifacts } from "./Artifacts.js";
import { Transport, TckError } from "./Domain.js";
import {
  checkHistory,
  checkHistoryKv,
  readHistory,
  readHistoryKv,
  readLedger,
} from "./History.js";
import { equal } from "./Oracle.js";
// Read-only recovery of the driver's audit after its process was interrupted.
export const auditLedger = (options: {
  ledger: string;
  endpoint: string;
  name: string;
}) =>
  Effect.gen(function* () {
    if (!options.ledger || !options.endpoint || !options.name)
      return yield* Effect.fail(
        new TckError({
          phase: "arguments",
          message:
            "Audit requires --ledger, --endpoint and --name; use the recorded dedicated test target",
        }),
      );
    const transport = yield* Transport;
    const artifacts = yield* Artifacts;
    const ledger = yield* readLedger(options.ledger);
    const target = { name: "audit", baseUrl: options.endpoint };
    const json = (path: string) => {
      const url = new URL(path, target.baseUrl);
      const query = new URLSearchParams({ name: options.name });
      for (const [key, value] of url.searchParams) query.set(key, value);
      return transport
        .request(target, { path: `${url.pathname}?${query}` })
        .pipe(
          Effect.tap((response) => equal(response.status, 200)),
          Effect.map((response) => response.body),
        );
    };
    const rows = yield* readHistory(json);
    const summary = yield* checkHistory(ledger, rows);
    const kv = yield* readHistoryKv(json);
    yield* checkHistoryKv(rows, kv);
    yield* artifacts.json("ledger-audit.json", {
      ...options,
      summary,
      rows,
      kv,
    });
    yield* Console.log(JSON.stringify(summary));
  });
