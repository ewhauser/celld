import { ed25519, x25519 } from "./Curves.js";
import { Effect } from "effect";
import { operation, outcome, platform, rejection } from "./Platform.js";
import { encode } from "../shared/Codec.js";
import { channelMessages, sseBody } from "../shared/Messaging.js";

const hex = (bytes: ArrayBuffer) =>
  [...new Uint8Array(bytes)]
    .map((x) => x.toString(16).padStart(2, "0"))
    .join("");
const utf8 = (value: string) => new TextEncoder().encode(value);
// JWK coordinates are unpadded base64url; the contract is the decoded byte length.
const base64urlBytes = (value: string) =>
  atob(value.replaceAll("-", "+").replaceAll("_", "/")).length;
// The Workers typings give these calls a union return; the fixture selects the
// concrete shape that the requested format or algorithm actually produces.
const exportRaw = (key: CryptoKey) =>
  platform(() => crypto.subtle.exportKey("raw", key) as Promise<ArrayBuffer>);
const exportJwk = (key: CryptoKey) =>
  platform(() => crypto.subtle.exportKey("jwk", key) as Promise<JsonWebKey>);
export const web = (request: Request) =>
  Effect.gen(function* () {
    const path = new URL(request.url).pathname;
    switch (path) {
      case "/crypto/ed25519":
        return yield* ed25519;
      case "/crypto/x25519":
        return yield* x25519;
      case "/web/cache": {
        const key = new Request(
          "https://cache.test/" + new URL(request.url).searchParams.get("name"),
        );
        yield* platform(() =>
          caches.default.put(
            key,
            new Response("cached", {
              headers: { "cache-control": "public, max-age=60" },
            }),
          ),
        );
        const cached = yield* platform(() => caches.default.match(key));
        return {
          value: cached ? yield* platform(() => cached.text()) : null,
          deleted: yield* platform(() => caches.default.delete(key)),
        };
      }
      case "/web/body": {
        const original = new Request("https://example.test/", {
          method: "POST",
          body: "λ🌍",
        });
        const clone = original.clone();
        const before = original.bodyUsed;
        const text = yield* platform(() => original.text());
        const cloned = yield* platform(() => clone.text());
        const twice = yield* rejection(platform(() => original.text()));
        const lateClone = yield* rejection(operation(() => original.clone()));
        return {
          before,
          after: original.bodyUsed,
          text,
          cloned,
          twice,
          lateClone,
        };
      }
      case "/web/headers": {
        const headers = new Headers({ "X-Value": " first " });
        headers.append("x-value", "second");
        const joined = headers.get("X-VALUE");
        headers.set("x-value", "replacement");
        const replaced = headers.get("x-value");
        headers.delete("X-VALUE");
        return {
          joined,
          replaced,
          deleted: !headers.has("x-value"),
          invalid: yield* rejection(
            operation(() => headers.set("bad\nname", "x")),
          ),
        };
      }
      case "/web/url-form": {
        const url = new URL(
          "../a%20b?x=1&x=2#fragment",
          "https://example.test/root/path",
        );
        const form = new FormData();
        form.append("x", "first");
        form.append("x", "λ");
        form.append(
          "file",
          new File([new Uint8Array([0, 128, 255])], "bytes.bin", {
            type: "application/octet-stream",
          }),
        );
        const parsed = yield* platform(() => new Response(form).formData());
        const file = parsed.get("file");
        return {
          pathname: url.pathname,
          query: url.searchParams.getAll("x"),
          values: parsed.getAll("x"),
          file:
            file instanceof File
              ? {
                  name: file.name,
                  type: file.type,
                  bytes: [
                    ...new Uint8Array(
                      yield* platform(() => file.arrayBuffer()),
                    ),
                  ],
                }
              : null,
        };
      }
      case "/web/encoding": {
        const bytes = new TextEncoder().encode("λ🌍");
        const decoder = new TextDecoder();
        const a = decoder.decode(bytes.slice(0, 3), { stream: true });
        return {
          bytes: [...bytes],
          decoded: a + decoder.decode(bytes.slice(3)),
          base64: btoa("\x00\xff"),
          decodedBase64: [...atob("AP8=")].map((c) => c.charCodeAt(0)),
          invalid: yield* rejection(
            operation(() =>
              new TextDecoder("utf-8", {
                fatal: true,
                ignoreBOM: false,
              }).decode(new Uint8Array([255])),
            ),
          ),
        };
      }
      case "/web/fetch-abort": {
        const controller = new AbortController();
        controller.abort();
        return {
          error: yield* rejection(
            platform(() =>
              fetch("https://never-contact.invalid/", {
                signal: controller.signal,
              }),
            ),
          ),
        };
      }
      case "/web/abort": {
        const controller = new AbortController();
        let calls = 0;
        controller.signal.addEventListener("abort", () => {
          calls++;
        });
        controller.abort(new Error("test abort"));
        controller.abort();
        return {
          aborted: controller.signal.aborted,
          calls,
          reason: controller.signal.reason.message,
          thrown: yield* rejection(
            operation(() => controller.signal.throwIfAborted()),
          ),
        };
      }
      case "/web/redirect": {
        const response = Response.redirect("https://example.test/next", 307);
        return {
          status: response.status,
          location: response.headers.get("location"),
          invalid: yield* rejection(
            operation(() => Response.redirect("https://example.test", 200)),
          ),
        };
      }
      case "/web/crypto": {
        const bytes = new TextEncoder().encode("abc");
        const digest = yield* platform(() =>
          crypto.subtle.digest("SHA-256", bytes),
        );
        const key = yield* platform(() =>
          crypto.subtle.importKey(
            "raw",
            new TextEncoder().encode("key"),
            { name: "HMAC", hash: "SHA-256" },
            false,
            ["sign", "verify"],
          ),
        );
        const data = new TextEncoder().encode(
          "The quick brown fox jumps over the lazy dog",
        );
        const signature = yield* platform(() =>
          crypto.subtle.sign("HMAC", key, data),
        );
        return {
          digest: hex(digest),
          hmac: hex(signature),
          verified: yield* platform(() =>
            crypto.subtle.verify("HMAC", key, signature, data),
          ),
          rejected: yield* platform(() =>
            crypto.subtle.verify("HMAC", key, signature, bytes),
          ),
          uuid: /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(
            crypto.randomUUID(),
          ),
        };
      }
      case "/web/aes": {
        const key = yield* platform(() =>
          crypto.subtle.importKey("raw", new Uint8Array(16), "AES-GCM", false, [
            "encrypt",
            "decrypt",
          ]),
        );
        const algorithm = { name: "AES-GCM", iv: new Uint8Array(12) };
        const encrypted = yield* platform(() =>
          crypto.subtle.encrypt(algorithm, key, new Uint8Array(0)),
        );
        const plain = yield* platform(() =>
          crypto.subtle.decrypt(algorithm, key, encrypted),
        );
        const corrupt = new Uint8Array(encrypted.slice(0));
        corrupt[0] = corrupt[0]! ^ 1;
        return {
          ciphertext: [...new Uint8Array(encrypted)]
            .map((x) => x.toString(16).padStart(2, "0"))
            .join(""),
          plaintext: [...new Uint8Array(plain)],
          tampered: yield* rejection(
            platform(() => crypto.subtle.decrypt(algorithm, key, corrupt)),
          ),
        };
      }
      case "/web/ecdsa": {
        const curve = { name: "ECDSA", namedCurve: "P-256" } as const;
        const signing = { name: "ECDSA", hash: "SHA-256" } as const;
        const pair = yield* platform(
          () =>
            crypto.subtle.generateKey(curve, true, [
              "sign",
              "verify",
            ]) as Promise<CryptoKeyPair>,
        );
        const data = utf8("The quick brown fox jumps over the lazy dog");
        const signature = yield* platform(() =>
          crypto.subtle.sign(signing, pair.privateKey, data),
        );
        const jwk = yield* exportJwk(pair.publicKey);
        const raw = yield* exportRaw(pair.publicKey);
        // Re-importing the exported public key must reproduce a key that verifies
        // the same signature. A runtime that cannot import the format reports the
        // rejection here instead of failing the whole observation.
        const reimportVerified = (
          imported: Effect.Effect<CryptoKey, unknown>,
        ) =>
          outcome(
            imported.pipe(
              Effect.flatMap((key) =>
                platform(() =>
                  crypto.subtle.verify(signing, key, signature, data),
                ),
              ),
            ),
          );
        const tampered = new Uint8Array(signature.slice(0));
        tampered[0] = tampered[0]! ^ 1;
        return {
          // ECDSA signatures are randomized, so the contract is the fixed P-1363
          // signature width plus verification of the actual signature.
          signatureBytes: signature.byteLength,
          verified: yield* platform(() =>
            crypto.subtle.verify(signing, pair.publicKey, signature, data),
          ),
          tampered: yield* platform(() =>
            crypto.subtle.verify(signing, pair.publicKey, tampered, data),
          ),
          jwkVerified: yield* reimportVerified(
            platform(() =>
              crypto.subtle.importKey("jwk", jwk, curve, true, ["verify"]),
            ),
          ),
          rawVerified: yield* reimportVerified(
            platform(() =>
              crypto.subtle.importKey("raw", raw, curve, true, ["verify"]),
            ),
          ),
          jwk: {
            kty: jwk.kty,
            crv: jwk.crv,
            ext: jwk.ext,
            keyOps: jwk.key_ops,
            xBytes: base64urlBytes(jwk.x ?? ""),
            yBytes: base64urlBytes(jwk.y ?? ""),
            privateOmitted: jwk.d === undefined,
          },
          raw: {
            bytes: raw.byteLength,
            uncompressed: new Uint8Array(raw)[0] === 0x04,
          },
          usages: {
            private: pair.privateKey.usages,
            public: pair.publicKey.usages,
          },
          algorithm: pair.publicKey.algorithm,
          types: [pair.privateKey.type, pair.publicKey.type],
        };
      }
      case "/web/derive": {
        const password = yield* platform(() =>
          crypto.subtle.importKey("raw", utf8("password"), "PBKDF2", false, [
            "deriveBits",
            "deriveKey",
          ]),
        );
        const pbkdf2 = {
          name: "PBKDF2",
          salt: utf8("salt"),
          iterations: 4096,
          hash: "SHA-256",
        } as const;
        // RFC 5869 test case 1 inputs; the expected output is the published vector.
        const secret = yield* platform(() =>
          crypto.subtle.importKey(
            "raw",
            new Uint8Array(22).fill(0x0b),
            "HKDF",
            false,
            ["deriveBits"],
          ),
        );
        const hkdf = {
          name: "HKDF",
          hash: "SHA-256",
          salt: new Uint8Array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
          info: new Uint8Array([
            0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9,
          ]),
        } as const;
        const derived = yield* platform(() =>
          crypto.subtle.deriveKey(
            pbkdf2,
            password,
            { name: "HMAC", hash: "SHA-256", length: 256 },
            true,
            ["sign"],
          ),
        );
        return {
          pbkdf2: hex(
            yield* platform(() =>
              crypto.subtle.deriveBits(pbkdf2, password, 256),
            ),
          ),
          hkdf: hex(
            yield* platform(() =>
              crypto.subtle.deriveBits(hkdf, secret, 42 * 8),
            ),
          ),
          derivedMac: hex(
            yield* platform(() =>
              crypto.subtle.sign("HMAC", derived, utf8("abc")),
            ),
          ),
          derivedAlgorithm: derived.algorithm,
          unalignedLength: yield* rejection(
            platform(() => crypto.subtle.deriveBits(hkdf, secret, 7)),
          ),
          zeroIterations: yield* rejection(
            platform(() =>
              crypto.subtle.deriveBits(
                { ...pbkdf2, iterations: 0 },
                password,
                256,
              ),
            ),
          ),
        };
      }
      case "/web/key-export": {
        const hmac = yield* platform(() =>
          crypto.subtle.importKey(
            "raw",
            utf8("key"),
            { name: "HMAC", hash: "SHA-256" },
            true,
            ["sign", "verify"],
          ),
        );
        const aesBytes = new Uint8Array(
          Array.from({ length: 16 }, (_, i) => i),
        );
        const aes = yield* platform(() =>
          crypto.subtle.importKey("raw", aesBytes, "AES-GCM", true, [
            "encrypt",
            "decrypt",
          ]),
        );
        const secretJwk = (key: CryptoKey) =>
          outcome(
            exportJwk(key).pipe(
              Effect.map((jwk) => ({
                kty: jwk.kty,
                alg: jwk.alg,
                keyOps: jwk.key_ops,
                ext: jwk.ext,
                k: jwk.k,
              })),
            ),
          );
        return {
          hmacRaw: [...new Uint8Array(yield* exportRaw(hmac))],
          aesRaw: [...new Uint8Array(yield* exportRaw(aes))],
          hmacJwk: yield* secretJwk(hmac),
          aesJwk: yield* secretJwk(aes),
          algorithms: [hmac.algorithm, aes.algorithm],
        };
      }
      case "/web/crypto-invalid": {
        const verifyOnly = yield* platform(() =>
          crypto.subtle.importKey(
            "raw",
            utf8("key"),
            { name: "HMAC", hash: "SHA-256" },
            false,
            ["verify"],
          ),
        );
        const opaque = yield* platform(() =>
          crypto.subtle.importKey("raw", new Uint8Array(16), "AES-GCM", false, [
            "encrypt",
          ]),
        );
        return {
          wrongUsage: yield* rejection(
            platform(() =>
              crypto.subtle.sign("HMAC", verifyOnly, new Uint8Array(1)),
            ),
          ),
          unknownAlgorithm: yield* rejection(
            platform(() =>
              crypto.subtle.importKey(
                "raw",
                new Uint8Array(16),
                { name: "AES-NOPE" },
                false,
                ["encrypt"],
              ),
            ),
          ),
          unknownHash: yield* rejection(
            platform(() => crypto.subtle.digest("SHA-42", new Uint8Array(1))),
          ),
          badKeyLength: yield* rejection(
            platform(() =>
              crypto.subtle.importKey(
                "raw",
                new Uint8Array(17),
                "AES-GCM",
                false,
                ["encrypt"],
              ),
            ),
          ),
          malformedJwk: yield* rejection(
            platform(() =>
              crypto.subtle.importKey(
                "jwk",
                { kty: "EC", crv: "P-256", x: "!!!", y: "!!!" },
                { name: "ECDSA", namedCurve: "P-256" },
                false,
                ["verify"],
              ),
            ),
          ),
          emptyUsages: yield* rejection(
            platform(() =>
              crypto.subtle.importKey(
                "raw",
                new Uint8Array(16),
                "AES-GCM",
                false,
                [],
              ),
            ),
          ),
          mismatchedUsage: yield* rejection(
            platform(() =>
              crypto.subtle.importKey(
                "raw",
                new Uint8Array(16),
                "AES-GCM",
                false,
                ["sign"],
              ),
            ),
          ),
          nonExtractable: yield* rejection(
            platform(() => crypto.subtle.exportKey("raw", opaque)),
          ),
          emptyIv: yield* rejection(
            platform(() =>
              crypto.subtle.encrypt(
                { name: "AES-GCM", iv: new Uint8Array(0) },
                opaque,
                new Uint8Array(1),
              ),
            ),
          ),
        };
      }
      case "/messaging/channel": {
        const channel = new MessageChannel();
        const sent = channelMessages();
        const received: unknown[] = [];
        const delivered = new Promise<void>((resolve) => {
          channel.port2.onmessage = (event: MessageEvent) => {
            received.push(event.data);
            if (received.length === sent.length) resolve();
          };
        });
        for (const message of sent) channel.port1.postMessage(message);
        // Nothing is delivered synchronously: ports queue until the task yields.
        const synchronous = received.length;
        yield* platform(() => delivered);
        const unserializable = yield* rejection(
          operation(() => channel.port1.postMessage(() => 1)),
        );
        channel.port2.close();
        const afterClose = yield* rejection(
          operation(() => channel.port1.postMessage("late")),
        );
        yield* platform(() => scheduler.wait(10));
        return {
          synchronous,
          messages: encode(received),
          unserializable,
          afterClose,
          droppedAfterClose: received.length === sent.length,
        };
      }
      case "/messaging/event-source": {
        const source = EventSource.from(new Response(sseBody()).body!);
        const events: {
          type: string;
          data: string;
          lastEventId: string;
        }[] = [];
        const trace: string[] = [];
        const record = (event: Event) => {
          const message = event as MessageEvent;
          events.push({
            type: message.type,
            data: message.data as string,
            lastEventId: message.lastEventId,
          });
        };
        yield* platform(
          () =>
            new Promise<void>((resolve) => {
              source.addEventListener("open", () => trace.push("open"));
              source.addEventListener("greeting", record);
              source.addEventListener("message", record);
              source.addEventListener("error", () => {
                trace.push("error");
                resolve();
              });
            }),
        );
        const exhausted = source.readyState;
        source.close();
        return {
          events,
          trace,
          exhausted,
          closed: source.readyState,
          states: [
            EventSource.CONNECTING,
            EventSource.OPEN,
            EventSource.CLOSED,
          ],
          withCredentials: source.withCredentials,
        };
      }
      case "/web/html": {
        const response = new HTMLRewriter()
          .on("p", {
            element(element) {
              element.setAttribute("data-test", "yes");
              element.setInnerContent("λ & text");
            },
          })
          .transform(new Response("<p>before</p>"));
        return { html: yield* platform(() => response.text()) };
      }
      case "/streams/tee": {
        const [a, b] = new ReadableStream<Uint8Array>({
          start(c) {
            c.enqueue(new Uint8Array([0, 128]));
            c.enqueue(new Uint8Array([255, 10]));
            c.close();
          },
        }).tee();
        return {
          a: [
            ...new Uint8Array(
              yield* platform(() => new Response(a).arrayBuffer()),
            ),
          ],
          b: [
            ...new Uint8Array(
              yield* platform(() => new Response(b).arrayBuffer()),
            ),
          ],
        };
      }
      case "/streams/cancel": {
        let reason: unknown;
        let pulls = 0;
        const stream = new ReadableStream(
          {
            pull(c) {
              pulls++;
              c.enqueue("first");
            },
            cancel(value) {
              reason = value;
            },
          },
          { highWaterMark: 0 },
        );
        const reader = stream.getReader();
        const first = yield* platform(() => reader.read());
        yield* platform(() => reader.cancel("stop"));
        return {
          first,
          reason,
          pulls,
          after: yield* platform(() => reader.read()),
        };
      }
      case "/streams/error": {
        const stream = new ReadableStream({
          start(c) {
            c.error(new TypeError("broken"));
          },
        });
        return {
          error: yield* rejection(platform(() => stream.getReader().read())),
        };
      }
      case "/streams/transform": {
        const source = new ReadableStream<string>({
          start(c) {
            c.enqueue("a");
            c.enqueue("λ");
            c.close();
          },
        });
        const transformed = source.pipeThrough(
          new TransformStream<string, Uint8Array>({
            transform(chunk, c) {
              c.enqueue(new TextEncoder().encode(chunk.toUpperCase()));
            },
          }),
        );
        return {
          text: yield* platform(() => new Response(transformed).text()),
        };
      }
      case "/streams/backpressure": {
        const trace: string[] = [];
        let release: (() => void) | undefined;
        const sink = new WritableStream<string>(
          {
            write(value) {
              trace.push(value);
              return new Promise<void>((resolve) => {
                release = resolve;
              });
            },
          },
          { highWaterMark: 1 },
        );
        const writer = sink.getWriter();
        const initial = writer.desiredSize;
        const pending = writer.write("one");
        const blocked = writer.desiredSize;
        yield* platform(() => Promise.resolve());
        if (!release)
          return yield* Effect.fail(new Error("write was not started"));
        release();
        yield* platform(() => pending);
        yield* platform(() => writer.ready);
        const resumed = writer.desiredSize;
        yield* platform(() => writer.close());
        return { initial, blocked, resumed, trace };
      }
      default:
        return undefined;
    }
  });
