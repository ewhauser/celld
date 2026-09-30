import { expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { decodeJsonc } from "../src/Jsonc.js";

it.effect("accepts formatted Wrangler JSONC comments and trailing commas", () =>
  Effect.gen(function* () {
    const value = yield* decodeJsonc(
      Schema.Struct({ name: Schema.String }),
      '{\n // fixture\n "name": "core",\n}',
    );
    expect(value.name).toBe("core");
  }),
);
it.effect(
  "rejects malformed JSONC instead of accepting a recovered partial parse",
  () =>
    Effect.gen(function* () {
      const result = yield* Effect.exit(
        decodeJsonc(
          Schema.Struct({ name: Schema.String }),
          '{"name":"core" "broken":}',
        ),
      );
      expect(result._tag).toBe("Failure");
    }),
);
it.effect("validates the decoded configuration shape", () =>
  Effect.gen(function* () {
    const result = yield* Effect.exit(
      decodeJsonc(Schema.Struct({ name: Schema.String }), '{"name":42}'),
    );
    expect(result._tag).toBe("Failure");
  }),
);
