import { Cause, Effect, Layer, Schema, Stream } from "effect";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import { converse } from "./WebSocket.js";
import { Artifacts, decodeJson } from "./Artifacts.js";
import { Transport, TckError, toTckError, type Observation } from "./Domain.js";

export const transportLayer = Layer.effect(
  Transport,
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;
    const artifacts = yield* Artifacts;
    let sequence = 0;
    return {
      websocket: (target, path, mode) => {
        const url = new URL(path, target.baseUrl);
        url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
        return converse(url.href, artifacts.json, mode);
      },
      request: (target, spec) =>
        Effect.suspend(() => {
          const id = ++sequence;
          return Effect.gen(function* () {
            yield* artifacts.text(
              "http.jsonl",
              JSON.stringify({ id, target: target.name, request: spec }) + "\n",
              true,
            );
            let request = HttpClientRequest.make(spec.method ?? "GET")(
              new URL(spec.path, target.baseUrl),
              { headers: spec.headers },
            );
            if (spec.body !== undefined)
              request = HttpClientRequest.bodyText(
                request,
                spec.body,
                spec.headers?.["content-type"] ?? "text/plain",
              );
            const response = yield* client.execute(request);
            const bytes = yield* response.stream.pipe(
              Stream.runFoldEffect(
                () => new Uint8Array(0),
                (previous, next) => {
                  if (previous.length + next.length > 1024 * 1024)
                    return Effect.fail(
                      new TckError({
                        phase: "http",
                        message: "Response exceeded 1 MiB",
                      }),
                    );
                  const merged = new Uint8Array(previous.length + next.length);
                  merged.set(previous);
                  merged.set(next, previous.length);
                  return Effect.succeed(merged);
                },
              ),
            );
            const text = new TextDecoder().decode(bytes);
            yield* artifacts.text(
              "http.jsonl",
              JSON.stringify({
                id,
                target: target.name,
                response: {
                  status: response.status,
                  headers: response.headers,
                  bodyBase64: Buffer.from(bytes).toString("base64"),
                },
              }) + "\n",
              true,
            );
            const headers: Record<string, string> = {};
            for (const name of ["content-type", "x-tck-response"]) {
              const value = response.headers[name];
              if (value !== undefined) headers[name] = value;
            }
            const body = headers["content-type"]?.includes("application/json")
              ? yield* decodeJson(Schema.Unknown, text).pipe(
                  Effect.catch(() => Effect.succeed({ invalidJson: text })),
                )
              : headers["content-type"] === "application/octet-stream"
                ? [...bytes]
                : text;
            return {
              status: response.status,
              headers,
              body,
            } satisfies Observation;
          }).pipe(
            Effect.timeout(spec.timeoutMs ?? 10_000),
            Effect.tapCause((cause) =>
              artifacts.text(
                "http.jsonl",
                JSON.stringify({ id, error: Cause.pretty(cause) }) + "\n",
                true,
              ),
            ),
            Effect.mapError(toTckError("http")),
          );
        }),
    };
  }),
);
