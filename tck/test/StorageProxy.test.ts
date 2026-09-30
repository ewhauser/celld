import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
// oxlint-disable-next-line effect/use-http-client-service -- The proxy test needs a real Node HTTP upstream to verify socket faults.
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { acquireStorageProxy } from "../src/StorageProxy.js";
const request = (url: string, method = "GET") =>
  Effect.tryPromise({
    try: (signal) => fetch(url, { method, signal }),
    catch: (error) => error,
  });
it.live(
  "storage proxy throttles without writing and drops responses only after upstream success",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        let writes = 0;
        const upstream = createServer((req, res) => {
          req.resume();
          req.on("end", () => {
            if (req.method === "PUT") writes++;
            res.end("stored");
          });
        });
        yield* Effect.acquireRelease(
          Effect.callback<void>((resume) => {
            upstream.listen(0, "127.0.0.1", () => resume(Effect.void));
          }),
          () =>
            Effect.sync(() => {
              upstream.closeAllConnections();
              upstream.close();
            }),
        );
        const proxy = yield* acquireStorageProxy({
          upstreamHostname: "127.0.0.1",
          upstreamPort: (upstream.address() as AddressInfo).port,
          dataPort: 0,
          controlPort: 0,
        });
        const data = `http://127.0.0.1:${(proxy.data.address() as AddressInfo).port}/object`;
        const control = `http://127.0.0.1:${(proxy.admin.address() as AddressInfo).port}`;
        expect((yield* request(data, "PUT")).status).toBe(200);
        expect(writes).toBe(1);
        yield* request(control + "/mode?mode=throttle&ms=1000");
        expect((yield* request(data, "PUT")).status).toBe(503);
        expect(writes).toBe(1);
        yield* request(
          control + "/mode?mode=throttle&ms=1000&pathContains=object",
        );
        expect((yield* request(data, "PUT")).status).toBe(503);
        expect(
          (yield* request(data.replace("/object", "/nodes/lease"))).status,
        ).toBe(200);
        expect(writes).toBe(1);
        yield* request(control + "/mode?mode=latency&ms=1000");
        const began = Date.now();
        expect((yield* request(data)).status).toBe(200);
        expect(Date.now() - began).toBeGreaterThanOrEqual(700);
        yield* request(control + "/mode?mode=timeout&ms=1000");
        expect(
          (yield* Effect.exit(
            request(data, "PUT").pipe(Effect.timeout("100 millis")),
          ))._tag,
        ).toBe("Failure");
        expect(writes).toBe(1);
        yield* request(control + "/mode?mode=drop-response&ms=1000");
        expect((yield* Effect.exit(request(data, "PUT")))._tag).toBe("Failure");
        expect(writes).toBe(2);
        const statsResponse = yield* request(control + "/stats");
        const stats = yield* Effect.tryPromise(() => statsResponse.text());
        expect(stats).toContain('"upstreamStatus":200,"dropped":true');
        yield* request(control + "/mode?mode=normal&ms=0");
        expect((yield* request(data)).status).toBe(200);
        expect(
          (yield* request(control + "/mode?mode=invalid&ms=1000")).status,
        ).toBe(400);
      }),
    ).pipe(Effect.timeout("10 seconds")),
);

it.live(
  "uses a fresh upstream connection after a rejected conditional write without retrying",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const sockets = new Set<unknown>();
        const bodies: string[] = [];
        const upstream = createServer((req, res) => {
          sockets.add(req.socket);
          let body = "";
          req.setEncoding("utf8");
          req.on("data", (chunk: string) => {
            body += chunk;
          });
          req.on("end", () => {
            bodies.push(body);
            res.writeHead(req.headers["if-none-match"] === "*" ? 412 : 200);
            res.end("observed");
          });
        });
        yield* Effect.acquireRelease(
          Effect.callback<void>((resume) => {
            upstream.listen(0, "127.0.0.1", () => resume(Effect.void));
          }),
          () =>
            Effect.sync(() => {
              upstream.closeAllConnections();
              upstream.close();
            }),
        );
        const proxy = yield* acquireStorageProxy({
          upstreamHostname: "127.0.0.1",
          upstreamPort: (upstream.address() as AddressInfo).port,
          dataPort: 0,
          controlPort: 0,
        });
        const url = `http://127.0.0.1:${(proxy.data.address() as AddressInfo).port}/object`;
        for (const conditional of [true, false]) {
          const response = yield* Effect.tryPromise((signal) =>
            fetch(url, {
              method: "PUT",
              signal,
              body: conditional ? "rejected" : "updated",
              headers: conditional
                ? { "if-none-match": "*" }
                : { "if-match": "etag" },
            }),
          );
          expect(response.status).toBe(conditional ? 412 : 200);
          expect(yield* Effect.tryPromise(() => response.text())).toBe(
            "observed",
          );
        }
        expect(bodies).toEqual(["rejected", "updated"]);
        expect(sockets.size).toBe(2);
      }),
    ),
);
