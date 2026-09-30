import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { encode } from "../fixtures/shared/Codec.js";
it.effect("preserves distinctions JSON loses", () =>
  Effect.sync(() => {
    expect(
      [undefined, null, -0, 0, NaN, Infinity, -Infinity, 1n].map((x) =>
        encode(x),
      ),
    ).toEqual([
      { type: "undefined" },
      { type: "null" },
      { type: "number", value: "-0" },
      { type: "number", value: 0 },
      { type: "number", value: "NaN" },
      { type: "number", value: "+Infinity" },
      { type: "number", value: "-Infinity" },
      { type: "bigint", value: "1" },
    ]);
    expect(encode({})).not.toEqual(encode({ missing: undefined }));
    // oxlint-disable-next-line unicorn/no-new-array -- A sparse hole must differ from an explicit undefined element.
    expect(encode([undefined])).not.toEqual(encode(new Array(1)));
  }),
);
it.effect("preserves bytes, view offsets, and container order", () =>
  Effect.sync(() => {
    expect(encode(new Uint8Array([9, 0, 255, 9]).subarray(1, 3))).toEqual({
      type: "Uint8Array",
      value: [0, 255],
    });
    expect(encode(new Uint8Array([0, 255]).buffer)).toEqual({
      type: "ArrayBuffer",
      value: [0, 255],
    });
    expect(encode(new Set([2, 1]))).toEqual({
      type: "Set",
      value: [
        { type: "number", value: 2 },
        { type: "number", value: 1 },
      ],
    });
    expect(
      encode(
        new Map([
          ["b", 2],
          ["a", 1],
        ]),
      ),
    ).not.toEqual(
      encode(
        new Map([
          ["a", 1],
          ["b", 2],
        ]),
      ),
    );
  }),
);
it.effect(
  "canonicalizes only plain object keys and cannot confuse tags with values",
  () =>
    Effect.sync(() => {
      expect(encode({ b: 2, a: 1 })).toEqual(encode({ a: 1, b: 2 }));
      expect(encode({ type: "undefined" })).not.toEqual(encode(undefined));
      expect(encode(new Date("2020-01-01T00:00:00Z"))).toEqual({
        type: "Date",
        value: "2020-01-01T00:00:00.000Z",
      });
    }),
);
it.effect("rejects unsupported values instead of dropping them", () =>
  Effect.sync(() => {
    const cycle: unknown[] = [];
    cycle.push(cycle);
    expect(() => encode(cycle)).toThrow("Cyclic");
    expect(() => encode(() => 1)).toThrow("Unsupported");
    expect(() => encode(new Error("lost error"))).toThrow("Unsupported");
    const shared = { x: 1 };
    expect(() => encode([shared, shared])).not.toThrow();
  }),
);
