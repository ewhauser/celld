import { DurableObject } from "cloudflare:workers";
import { Effect } from "effect";
import { platform, ready } from "../shared/Platform.js";
declare const __DYNAMIC_CODE__: string;
interface RecoveryEnv {
  PROBE: DurableObjectNamespace<Recovery>;
  LOADER: WorkerLoader;
}
const facetCode = () => ({
  compatibilityDate: "2026-07-30",
  mainModule: "worker.js",
  modules: { "worker.js": __DYNAMIC_CODE__ },
});
export class Recovery extends DurableObject<RecoveryEnv> {
  private readonly activation = crypto.randomUUID();
  alarm() {
    return Effect.runPromise(
      platform(() => this.ctx.storage.put("fired", true)),
    );
  }
  fetch(request: Request) {
    const storage = this.ctx.storage;
    const activation = this.activation;
    const cell = this.ctx.id.toString();
    // A facet needs a class from a Worker Loader binding on both runtimes.
    const ledger = (path: string) =>
      platform(() =>
        this.ctx.facets
          .get("ledger", () => ({
            class: this.env.LOADER.get(
              "recovery-facet-v1",
              facetCode,
            ).getDurableObjectClass("Ledger"),
          }))
          .fetch(`https://fixture.test${path}`),
      ).pipe(
        Effect.flatMap((response) =>
          platform(() => response.json<Record<string, unknown>>()),
        ),
      );
    return Effect.runPromise(
      Effect.gen(function* () {
        const path = new URL(request.url).pathname;
        if (path === "/ready") return ready();
        if (path === "/fleet/id") return Response.json({ cell });
        if (path === "/facet/seed")
          return Response.json({ ...(yield* ledger("/seed")), activation });
        if (path === "/facet/state")
          return Response.json({ ...(yield* ledger("/state")), activation });
        if (path === "/outage/write") {
          const id = Number(new URL(request.url).searchParams.get("id"));
          if (!Number.isInteger(id) || id < 1 || id > 256)
            return new Response("invalid id", { status: 400 });
          yield* Effect.sync(() =>
            storage.transactionSync(() => {
              storage.sql
                .exec(
                  "CREATE TABLE IF NOT EXISTS outage (id INTEGER PRIMARY KEY, value TEXT)",
                )
                .toArray();
              storage.sql
                .exec("INSERT INTO outage VALUES (?, ?)", id, `value-${id}`)
                .toArray();
              storage.kv.put(`op:${id}`, `value-${id}`);
            }),
          );
          if (new URL(request.url).searchParams.get("hold") === "1") {
            yield* Effect.log(`tck-interrupted-write id=${id}`);
            yield* Effect.sleep("5 seconds");
          }
          return Response.json({ acknowledged: id });
        }
        if (path === "/outage/state")
          return Response.json({
            kv: [...storage.kv.list({ prefix: "op:" })],
            sql: storage.sql.exec("SELECT * FROM outage ORDER BY id").toArray(),
          });
        if (path === "/seed") {
          yield* platform(() =>
            storage.put({
              retained: "durable λ",
              deleted: "remove",
              fired: false,
            }),
          );
          yield* platform(() => storage.delete("deleted"));
          yield* Effect.sync(() => {
            storage.sql.exec(
              "CREATE TABLE IF NOT EXISTS records (id INTEGER PRIMARY KEY, value TEXT)",
            );
            storage.transactionSync(() => {
              storage.sql.exec("INSERT INTO records VALUES (1, 'committed')");
              storage.kv.put("transaction", "committed");
            });
          });
          // Roll back both stores in one transaction, then check again after restart.
          yield* Effect.try({
            try: () =>
              storage.transactionSync(() => {
                storage.sql.exec("INSERT INTO records VALUES (2, 'rollback')");
                storage.kv.put("transaction", "rollback");
                throw new Error("intentional rollback");
              }),
            catch: (e) => e,
          }).pipe(
            Effect.catch((e) =>
              e instanceof Error && e.message === "intentional rollback"
                ? Effect.void
                : Effect.fail(e),
            ),
          );
          return Response.json({ acknowledged: true, activation });
        }
        if (path === "/arm") {
          const deadline = Date.now() + 5000;
          yield* platform(() => storage.setAlarm(deadline));
          return Response.json({ deadline });
        }
        return Response.json({
          activation,
          retained: (yield* platform(() => storage.get("retained"))) ?? null,
          deleted: (yield* platform(() => storage.get("deleted"))) ?? null,
          transaction:
            (yield* platform(() => storage.get("transaction"))) ?? null,
          rows: storage.sql.exec("SELECT * FROM records ORDER BY id").toArray(),
          fired: (yield* platform(() => storage.get("fired"))) ?? false,
        });
      }),
    );
  }
}
export default {
  fetch(request: Request, env: RecoveryEnv) {
    return Effect.runPromise(
      platform(() =>
        env.PROBE.getByName(
          new URL(request.url).searchParams.get("name") ?? "ready",
        ).fetch(request),
      ),
    );
  },
} satisfies ExportedHandler<RecoveryEnv>;
