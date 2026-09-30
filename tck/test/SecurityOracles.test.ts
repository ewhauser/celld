import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import {
  BODY_LIMIT_DENIAL,
  FALLBACK_HOST,
  INTERNAL_NOT_FOUND,
  PEER_DENIAL,
  type ProbeResult,
  checkAllowed,
  checkApplicationFallthrough,
  checkBodyLimit,
  checkDenied,
  checkForwarded,
  checkInternalNotFound,
  checkPeerDenied,
  checkReservedRefusal,
  checkUnchanged,
  expectedOrigin,
  observation,
  reservedClass,
  validHost,
} from "../src/SecurityOracles.js";

const APPLICATION_NOT_FOUND = "not found";
const fails = <A, E>(effect: Effect.Effect<A, E>) =>
  Effect.map(Effect.exit(effect), (exit) => exit._tag === "Failure");

it.effect(
  "a denied probe must be a real rejection, never a transport error",
  () =>
    Effect.gen(function* () {
      const denied: ProbeResult = {
        label: "peer",
        status: 401,
        headers: { "x-cells-peer-version": "5" },
        body: PEER_DENIAL,
      };
      yield* checkPeerDenied(denied);
      for (const wrong of [
        // An accepted forged request is the failure this suite exists to catch.
        { ...denied, status: 200, body: "{}" },
        { ...denied, status: 204, body: "" },
        // A connection refused or timed out says nothing about authentication.
        { label: "peer", error: "TimeoutError: aborted" },
        { label: "peer", error: "TypeError: fetch failed" },
        // Right status, wrong reason.
        { ...denied, body: "not found" },
        { ...denied, status: 404 },
        // The peer listener must be the responder.
        { ...denied, headers: {} },
      ] as ProbeResult[])
        expect(yield* fails(checkPeerDenied(wrong))).toBe(true);
    }),
);

it.effect("an oracle may not be pointed at the wrong status class", () =>
  Effect.gen(function* () {
    const ok: ProbeResult = { label: "x", status: 200, body: "{}" };
    yield* checkAllowed(ok, { status: 200 });
    // A denial expectation that describes a success would pass on anything.
    expect(yield* fails(checkDenied(ok, { status: 200 }))).toBe(true);
    expect(yield* fails(checkDenied(ok, { status: 302 }))).toBe(true);
    // And a control oracle may not certify a rejection as success.
    expect(
      yield* fails(checkAllowed({ label: "x", status: 401 }, { status: 401 })),
    ).toBe(true);
  }),
);

it.effect("a missing or duplicated probe is not an observation", () =>
  Effect.gen(function* () {
    const results: ProbeResult[] = [
      { label: "a", status: 200 },
      { label: "b", status: 401 },
      { label: "b", status: 401 },
    ];
    expect((yield* observation(results, "a")).status).toBe(200);
    expect(yield* fails(observation(results, "b"))).toBe(true);
    expect(yield* fails(observation(results, "missing"))).toBe(true);
  }),
);

it.effect(
  "public-listener denial requires the application's own response, not any 404",
  () =>
    Effect.gen(function* () {
      yield* checkApplicationFallthrough(
        { label: "p", status: 404, body: APPLICATION_NOT_FOUND },
        APPLICATION_NOT_FOUND,
      );
      for (const wrong of [
        // The operator API answering publicly is the boundary failure.
        { label: "p", status: 200, body: '{"deployment":{}}' },
        // celld's own 404 here would mean the operator router is reachable.
        { label: "p", status: 404, body: INTERNAL_NOT_FOUND },
        { label: "p", status: 404, body: "" },
        { label: "p", error: "connect ECONNREFUSED" },
      ] as ProbeResult[])
        expect(
          yield* fails(
            checkApplicationFallthrough(wrong, APPLICATION_NOT_FOUND),
          ),
        ).toBe(true);
      // The oracle cannot distinguish the two responders if they are the same.
      expect(
        yield* fails(
          checkApplicationFallthrough(
            { label: "p", status: 404, body: INTERNAL_NOT_FOUND },
            INTERNAL_NOT_FOUND,
          ),
        ),
      ).toBe(true);
    }),
);

it.effect(
  "an unknown internal path must not be answered by application code",
  () =>
    Effect.gen(function* () {
      yield* checkInternalNotFound(
        { label: "i", status: 404, body: INTERNAL_NOT_FOUND },
        APPLICATION_NOT_FOUND,
      );
      for (const wrong of [
        // The application answering an internal path is the boundary failure.
        { label: "i", status: 404, body: APPLICATION_NOT_FOUND },
        { label: "i", status: 200, body: '{"revision":"qualification-v1"}' },
        { label: "i", error: "socket hang up" },
      ] as ProbeResult[])
        expect(
          yield* fails(checkInternalNotFound(wrong, APPLICATION_NOT_FOUND)),
        ).toBe(true);
    }),
);

it.effect("reserved runtime classes are named, not assumed", () =>
  Effect.gen(function* () {
    const scope = "__Queue:abc";
    expect(reservedClass(scope)).toBe("__Queue");
    expect(reservedClass("Recovery:abc")).toBeUndefined();
    expect(reservedClass("__Queue")).toBeUndefined();
    yield* checkReservedRefusal(
      {
        label: "r",
        status: 403,
        body: '{"error":"__Queue is a runtime class and is not reachable over /do/; use `celld queue`"}',
      },
      scope,
    );
    // An ordinary class may never be used to satisfy this oracle.
    expect(
      yield* fails(
        checkReservedRefusal(
          { label: "r", status: 403, body: "x" },
          "Recovery:abc",
        ),
      ),
    ).toBe(true);
    for (const wrong of [
      // Reaching a reserved class over /do/ is the boundary failure.
      { label: "r", status: 200, body: "{}" },
      { label: "r", status: 404, body: APPLICATION_NOT_FOUND },
      // A refusal about a different class is not evidence about this one.
      {
        label: "r",
        status: 403,
        body: '{"error":"__Workflow is a runtime class and is not reachable over /do/"}',
      },
      { label: "r", error: "aborted" },
    ] as ProbeResult[])
      expect(yield* fails(checkReservedRefusal(wrong, scope))).toBe(true);
  }),
);

it.effect("body limits are enforced by celld, with the documented status", () =>
  Effect.gen(function* () {
    yield* checkBodyLimit({
      label: "b",
      status: 413,
      body: BODY_LIMIT_DENIAL,
    });
    for (const wrong of [
      // An accepted oversized body is the boundary failure.
      { label: "b", status: 200, body: '{"seq":1}' },
      // The application rejecting it is not the ingress limit.
      { label: "b", status: 400, body: "too large" },
      { label: "b", status: 500, body: "Worker failed" },
      { label: "b", error: "ECONNRESET" },
    ] as ProbeResult[])
      expect(yield* fails(checkBodyLimit(wrong))).toBe(true);
  }),
);

it("host validity follows the documented forms", () => {
  for (const valid of [
    "app.example",
    "app.example:8080",
    "10.1.2.3:8080",
    "[::1]:8080",
    "celld:8080",
    "celld.local",
  ])
    expect(validHost(valid)).toBe(true);
  for (const invalid of ["bad host", "bad_host!!", "", "a b:80", "[::1"])
    expect(validHost(invalid)).toBe(false);
});

it("forwarded headers are ignored without a trusted proxy and last-wins with one", () => {
  const forwarded = {
    forwardedHost: "first.example, last.example",
    forwardedProto: "http, https",
  };
  expect(
    expectedOrigin({
      trustsForwarded: false,
      hostHeader: "celld:8080",
      ...forwarded,
    }),
  ).toBe("http://celld:8080");
  expect(
    expectedOrigin({
      trustsForwarded: true,
      hostHeader: "celld:8080",
      ...forwarded,
    }),
  ).toBe("https://last.example");
  // A malformed forwarded host falls back to the request's own host.
  expect(
    expectedOrigin({
      trustsForwarded: true,
      hostHeader: "celld:8080",
      forwardedHost: "bad host!!",
      forwardedProto: "https",
    }),
  ).toBe("https://celld:8080");
  // With no usable host source at all, celld substitutes its own.
  expect(
    expectedOrigin({ trustsForwarded: false, hostHeader: "bad host" }),
  ).toBe(`http://${FALLBACK_HOST}`);
  expect(expectedOrigin({ trustsForwarded: true, hostHeader: "" })).toBe(
    `http://${FALLBACK_HOST}`,
  );
});

it.effect("a forwarded-header observation is checked against the policy", () =>
  Effect.gen(function* () {
    const policy = {
      trustsForwarded: false,
      hostHeader: "celld:8080",
      forwardedHost: "evil.example",
      forwardedProto: "https",
    };
    yield* checkForwarded(
      {
        label: "f",
        status: 200,
        body: '{"url":"http://celld:8080/echo/url","host":"celld:8080"}',
      },
      "/echo/url",
      policy,
    );
    for (const wrong of [
      // An untrusted node honouring the header is the boundary failure.
      {
        label: "f",
        status: 200,
        body: '{"url":"https://evil.example/echo/url","host":"celld:8080"}',
      },
      // A silent scheme upgrade counts too.
      {
        label: "f",
        status: 200,
        body: '{"url":"https://celld:8080/echo/url","host":"celld:8080"}',
      },
      { label: "f", status: 500, body: "Worker failed" },
      { label: "f", status: 200, body: "not json" },
      { label: "f", error: "aborted" },
    ] as ProbeResult[])
      expect(yield* fails(checkForwarded(wrong, "/echo/url", policy))).toBe(
        true,
      );
    // The trusted node must honour it, and an ignored header now fails.
    expect(
      yield* fails(
        checkForwarded(
          {
            label: "f",
            status: 200,
            body: '{"url":"http://celld:8080/echo/url","host":"celld:8080"}',
          },
          "/echo/url",
          { ...policy, trustsForwarded: true },
        ),
      ),
    ).toBe(true);
  }),
);

it.effect("protected state must be identical across a denial", () =>
  Effect.gen(function* () {
    const before = {
      owner: { node: "celld", epoch: 1 },
      history: [{ seq: 1, id: "a", payload: "p", kv: "p" }],
      kv: [["history:a", "p"]],
      events: [["event:queue:0", 1]],
    };
    yield* checkUnchanged("case", before, structuredClone(before));
    for (const after of [
      // A changed owner record means the denial moved ownership.
      { ...before, owner: { node: "celld2", epoch: 2 } },
      { ...before, owner: { node: "celld", epoch: 2 } },
      // A new or mutated row means the denied request wrote.
      {
        ...before,
        history: [
          ...before.history,
          { seq: 2, id: "b", payload: "q", kv: "q" },
        ],
      },
      { ...before, history: [{ seq: 1, id: "a", payload: "x", kv: "x" }] },
      { ...before, history: [] },
      { ...before, kv: [] },
      { ...before, events: [["event:queue:0", 2]] },
    ])
      expect(yield* fails(checkUnchanged("case", before, after))).toBe(true);
  }),
);
