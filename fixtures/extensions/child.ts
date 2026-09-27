import { DurableObject } from "cloudflare:workers";
import { Effect } from "effect";
import { platform } from "../shared/Platform.js";

interface ChildEnv {
  readonly TOKEN?: string;
  readonly WASM?: WebAssembly.Module;
  readonly COUNT?: number;
  readonly UPSTREAM?: Fetcher;
}
const text = (response: Response) => platform(() => response.text());
const upstream = (env: ChildEnv, path: string) => {
  const binding = env.UPSTREAM;
  return binding === undefined
    ? Effect.succeed("no binding")
    : platform(() => binding.fetch(`https://gateway.invalid${path}`)).pipe(
        Effect.flatMap(text),
      );
};
export class Counter extends DurableObject {
  fetch() {
    return Effect.runPromise(
      Effect.sync(() => {
        this.ctx.storage.sql.exec(
          "CREATE TABLE IF NOT EXISTS counter (n INTEGER)",
        );
        this.ctx.storage.sql.exec(
          "INSERT INTO counter SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM counter)",
        );
        const row = this.ctx.storage.sql
          .exec<{ n: number }>("UPDATE counter SET n = n + 1 RETURNING n")
          .one();
        return Response.json(row);
      }),
    );
  }
}
export class Ledger extends DurableObject<ChildEnv> {
  fetch(request: Request) {
    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        const storage = this.ctx.storage;
        const env = this.env;
        switch (new URL(request.url).searchParams.get("op")) {
          case "outbound": {
            // Write in the facet, then call out from it. The caller decides
            // whether a root storage transaction is open around this request.
            // Only the outcome is reported: where the call is refused, the
            // error class is not part of the documented contract.
            yield* platform(() => storage.put("balance", 40));
            const outbound = yield* upstream(env, "/facet").pipe(
              Effect.catch(() => Effect.succeed("rejected")),
            );
            return Response.json({
              balance:
                (yield* platform(() => storage.get<number>("balance"))) ?? null,
              outbound,
            });
          }
          case "seed":
            yield* platform(() => storage.put("balance", 10));
            return Response.json({ seeded: true });
          case "commit":
            return Response.json({
              inside: yield* platform(() =>
                storage.transaction((tx) =>
                  Effect.runPromise(
                    Effect.gen(function* () {
                      yield* platform(() => tx.put("balance", 20));
                      return yield* platform(() => tx.get<number>("balance"));
                    }),
                  ),
                ),
              ),
            });
          case "rollback":
            return Response.json({
              inside: yield* platform(() =>
                storage.transaction((tx) =>
                  Effect.runPromise(
                    Effect.gen(function* () {
                      yield* platform(() => tx.put("balance", 30));
                      const read = yield* platform(() =>
                        tx.get<number>("balance"),
                      );
                      tx.rollback();
                      return read;
                    }),
                  ),
                ),
              ),
            });
          default:
            return Response.json({
              balance:
                (yield* platform(() => storage.get<number>("balance"))) ?? null,
            });
        }
      }),
    );
  }
}
export default {
  fetch(request: Request, env: ChildEnv, ctx: ExecutionContext) {
    return Effect.runPromise(
      Effect.gen(function* () {
        switch (new URL(request.url).pathname) {
          case "/wasm": {
            const instance = yield* platform(() =>
              WebAssembly.instantiate(env.WASM!),
            );
            const add = instance.exports.add as (
              a: number,
              b: number,
            ) => number;
            return Response.json({ sum: add(19, 23) });
          }
          case "/props":
            return Response.json({ props: ctx.props ?? null });
          case "/env":
            return Response.json({
              token: env.TOKEN ?? null,
              count: env.COUNT ?? null,
              upstream: yield* upstream(env, "/binding"),
            });
          case "/outbound":
            return Response.json({
              global: yield* platform(() =>
                fetch("https://outbound.invalid/global"),
              ).pipe(
                Effect.flatMap(text),
                // The documented contract is that the call throws; the error
                // class is not part of it, so only the outcome is observed.
                Effect.catch(() => Effect.succeed("blocked")),
              ),
              binding: yield* upstream(env, "/binding"),
            });
          default:
            return new Response("dynamic λ");
        }
      }),
    );
  },
};
