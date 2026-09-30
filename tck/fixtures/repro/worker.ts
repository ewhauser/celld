import { DurableObject } from "cloudflare:workers";
import { Effect } from "effect";

const attempt = <A>(fn: () => PromiseLike<A>) =>
  Effect.tryPromise({
    try: () => Promise.resolve(fn()),
    catch: (error) => error,
  });
const capture = (effect: Effect.Effect<unknown, unknown>) =>
  effect.pipe(
    Effect.as({
      outcome: "accepted",
      name: null as string | null,
      domException: false,
    }),
    Effect.catch((error) =>
      Effect.succeed({
        outcome: "rejected",
        name: error instanceof Error ? error.name : "Unknown",
        domException: error instanceof DOMException,
      }),
    ),
  );
const sync = (fn: () => unknown) =>
  Effect.try({ try: fn, catch: (error) => error });

// No framework, service bindings, or application code is involved. The same
// fixture bytes run on each engine; the driver owns all expectations.
export class Repro extends DurableObject<ReproEnv> {
  fetch(request: Request) {
    const storage = this.ctx.storage;
    return Effect.runPromise(
      Effect.gen(function* () {
        if (new URL(request.url).pathname === "/ready") {
          yield* attempt(() => storage.put("ready", "ok"));
          return Response.json({
            value: yield* attempt(() => storage.get("ready")),
          });
        }
        const observations = {
          asyncLimit: yield* capture(
            attempt(() => storage.list({ limit: -1 })),
          ),
          syncLimit: yield* capture(
            sync(() => [...storage.kv.list({ limit: -1 })]),
          ),
          asyncPut: yield* capture(attempt(() => storage.put("fn", () => 1))),
          syncPut: yield* capture(
            sync(() => storage.kv.put("sync-fn", () => 1)),
          ),
          batchPut: yield* capture(
            attempt(() =>
              storage.put({ first: "must-not-commit", second: () => 1 }),
            ),
          ),
          // These checks distinguish error-type differences from partial writes.
          absent:
            (yield* attempt(() =>
              storage.get(["fn", "sync-fn", "first", "second"]),
            )).size === 0,
          validLimit: [
            ...(yield* attempt(() =>
              storage.list({ prefix: "nonexistent", limit: 1 }),
            )),
          ],
        };
        return Response.json(observations);
      }),
    );
  }
}
export default {
  fetch(request: Request, env: ReproEnv) {
    return Effect.runPromise(
      Effect.gen(function* () {
        const url = new URL(request.url);
        if (url.pathname !== "/body")
          return yield* attempt(() =>
            env.PROBE.getByName(url.searchParams.get("name") ?? "repro").fetch(
              request,
            ),
          );
        const observations: unknown[] = [];
        for (const kind of ["Request", "Response"] as const) {
          for (const method of [
            "text",
            "json",
            "arrayBuffer",
            "blob",
            "formData",
          ] as const) {
            const payload =
              method === "formData" ? "value=hello" : '{"value":"hello"}';
            const headers = {
              "content-type":
                method === "formData"
                  ? "application/x-www-form-urlencoded"
                  : "application/json",
            };
            const body =
              kind === "Request"
                ? new Request("https://example.test/", {
                    method: "POST",
                    body: payload,
                    headers,
                  })
                : new Response(payload, { headers });
            const before = body.bodyUsed;
            yield* attempt<unknown>(() => body[method]());
            observations.push({
              kind,
              method,
              before,
              after: body.bodyUsed,
              second: yield* capture(attempt<unknown>(() => body[method]())),
            });
          }
          const empty =
            kind === "Request"
              ? new Request("https://example.test/")
              : new Response(null);
          const first = yield* attempt(() => empty.text());
          const second = yield* attempt(() => empty.text());
          observations.push({
            kind,
            method: "null-body-control",
            first,
            second,
            used: empty.bodyUsed,
          });
        }
        return Response.json(observations);
      }),
    );
  },
} satisfies ExportedHandler<ReproEnv>;
