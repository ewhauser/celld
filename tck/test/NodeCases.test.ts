import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { gzipSync, deflateSync } from "node:zlib";
import { checkCompression } from "../src/NodeCases.js";

it.effect(
  "independently rejects identity compressors and broken decoders",
  () =>
    Effect.gen(function* () {
      const body = {
        sha256:
          "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        gzip: "hello λ",
        deflate: "hello λ",
        gzipBytes: gzipSync("hello λ").toString("base64"),
        deflateBytes: deflateSync("hello λ").toString("base64"),
      };
      yield* checkCompression({
        status: 200,
        headers: { "content-type": "application/json" },
        body,
      });
      for (const corrupt of [
        { gzipBytes: Buffer.from("hello λ").toString("base64") },
        { deflateBytes: Buffer.from("hello λ").toString("base64") },
        { gzip: "compressed input" },
        { deflate: "compressed input" },
      ])
        expect(
          (yield* Effect.exit(
            checkCompression({
              status: 200,
              headers: { "content-type": "application/json" },
              body: { ...body, ...corrupt },
            }),
          ))._tag,
        ).toBe("Failure");
    }),
);
