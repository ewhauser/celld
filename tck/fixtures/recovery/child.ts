import { DurableObject } from "cloudflare:workers";
import { Effect } from "effect";
import { platform } from "../shared/Platform.js";

// Facet class for the lifecycle suite. A facet needs a class from a Worker
// Loader binding, so this bundle is loaded by the recovery Worker.
export class Ledger extends DurableObject {
  fetch(request: Request) {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        const storage = this.ctx.storage;
        if (new URL(request.url).pathname === "/seed") {
          yield* platform(() => storage.put("balance", 10));
          yield* Effect.sync(() => {
            storage.sql.exec(
              "CREATE TABLE IF NOT EXISTS entries (id INTEGER PRIMARY KEY, value TEXT)",
            );
            storage.transactionSync(() => {
              storage.sql.exec("INSERT INTO entries VALUES (1, 'facet λ')");
            });
          });
          return Response.json({ seeded: true });
        }
        return Response.json({
          balance:
            (yield* platform(() => storage.get<number>("balance"))) ?? null,
          rows: storage.sql.exec("SELECT * FROM entries ORDER BY id").toArray(),
        });
      }),
    );
  }
}
export default {
  fetch() {
    return Effect.runPromise(
      Effect.sync(() => new Response("recovery facet loader")),
    );
  },
};
