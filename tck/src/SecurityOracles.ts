// Pure oracles for the security-boundary suite.
//
// Every observation these classify is produced by SecurityProbe.ts. Keeping the
// classification here, away from the orchestration, is what makes a deliberately
// wrong observation — an accepted forged request, a 2xx on a denied path, a
// changed owner record — testable without a fleet.
import { Effect, Schema } from "effect";
import { TckError } from "./Domain.js";
import { equal } from "./Oracle.js";

export const ProbeResult = Schema.Struct({
  label: Schema.String,
  status: Schema.optionalKey(Schema.Int),
  headers: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  body: Schema.optionalKey(Schema.String),
  error: Schema.optionalKey(Schema.String),
});
export type ProbeResult = typeof ProbeResult.Type;

/** celld's own body for an unknown path on the peer/operator listener. */
export const INTERNAL_NOT_FOUND = '{"error":"not_found"}';
/** Documented rejection body for an unauthenticated peer route. */
export const PEER_DENIAL = "peer authentication failed";
/** Documented rejection body for an over-limit ingress body. */
export const BODY_LIMIT_DENIAL = "request body too large";
/** Host celld substitutes when no valid host source exists. */
export const FALLBACK_HOST = "celld.local";

const refuse = (message: string, detail?: string) =>
  Effect.fail(
    new TckError({
      phase: "security",
      message,
      ...(detail === undefined ? {} : { detail }),
    }),
  );

/** Exactly one observation must carry this label; a missing probe is a failure. */
export const observation = (
  results: readonly ProbeResult[],
  label: string,
): Effect.Effect<ProbeResult, TckError> => {
  const found = results.filter((result) => result.label === label);
  return found.length === 1
    ? Effect.succeed(found[0]!)
    : refuse(
        `Expected exactly one observation labelled ${label}`,
        `found ${found.length}`,
      );
};

export interface ResponseExpectation {
  readonly status: number;
  readonly body?: string;
  readonly bodyIncludes?: string;
  readonly bodyExcludes?: readonly string[];
  readonly headerPresent?: string;
}

/**
 * A transport failure is never evidence about a boundary: an unreachable port
 * and an enforced denial look nothing alike, so `error` fails here.
 */
export const checkResponse = (
  result: ProbeResult,
  expectation: ResponseExpectation,
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    if (result.error !== undefined)
      return yield* refuse(
        `Probe ${result.label} did not complete`,
        result.error,
      );
    if (result.status !== expectation.status)
      return yield* refuse(
        `Probe ${result.label} returned ${result.status}, expected ${expectation.status}`,
        result.body,
      );
    const body = result.body ?? "";
    if (expectation.body !== undefined && body !== expectation.body)
      return yield* refuse(
        `Probe ${result.label} returned an unexpected body`,
        body,
      );
    if (
      expectation.bodyIncludes !== undefined &&
      !body.includes(expectation.bodyIncludes)
    )
      return yield* refuse(
        `Probe ${result.label} is missing ${expectation.bodyIncludes}`,
        body,
      );
    for (const excluded of expectation.bodyExcludes ?? [])
      if (body.includes(excluded))
        return yield* refuse(
          `Probe ${result.label} must not contain ${excluded}`,
          body,
        );
    if (
      expectation.headerPresent !== undefined &&
      (result.headers ?? {})[expectation.headerPresent] === undefined
    )
      return yield* refuse(
        `Probe ${result.label} is missing header ${expectation.headerPresent}`,
        JSON.stringify(result.headers ?? {}),
      );
  });

/** Guards the oracle itself: a denial expectation may never describe a success. */
export const checkDenied = (
  result: ProbeResult,
  expectation: ResponseExpectation,
): Effect.Effect<void, TckError> =>
  expectation.status < 400
    ? refuse(
        "A denial oracle requires a rejection status",
        String(expectation.status),
      )
    : checkResponse(result, expectation);

/** The authorized control of every denial case. */
export const checkAllowed = (
  result: ProbeResult,
  expectation: ResponseExpectation,
): Effect.Effect<void, TckError> =>
  expectation.status < 200 || expectation.status >= 300
    ? refuse(
        "An authorized-control oracle requires a success status",
        String(expectation.status),
      )
    : checkResponse(result, expectation);

export const checkPeerDenied = (
  result: ProbeResult,
): Effect.Effect<void, TckError> =>
  checkDenied(result, {
    status: 401,
    body: PEER_DENIAL,
    headerPresent: "x-cells-peer-version",
  });

export const checkBodyLimit = (
  result: ProbeResult,
): Effect.Effect<void, TckError> =>
  checkDenied(result, { status: 413, bodyIncludes: BODY_LIMIT_DENIAL });

/**
 * The public listener must answer an operator path with the application's own
 * response. A bare 404 would not distinguish "celld refused" from "celld has no
 * operator API here", so the application's marker body is required.
 */
export const checkApplicationFallthrough = (
  result: ProbeResult,
  marker: string,
): Effect.Effect<void, TckError> =>
  marker === INTERNAL_NOT_FOUND
    ? refuse("The application marker must differ from celld's not-found body")
    : checkDenied(result, {
        status: 404,
        body: marker,
        bodyExcludes: ["not_found"],
      });

/** An unknown internal path must be answered by celld, never by application code. */
export const checkInternalNotFound = (
  result: ProbeResult,
  marker: string,
): Effect.Effect<void, TckError> =>
  marker === INTERNAL_NOT_FOUND
    ? refuse("The application marker must differ from celld's not-found body")
    : checkDenied(result, { status: 404, body: INTERNAL_NOT_FOUND });

/** `__`-prefixed scopes are celld's reserved runtime classes. */
export const reservedClass = (scope: string) =>
  scope.startsWith("__") && scope.includes(":")
    ? scope.slice(0, scope.indexOf(":"))
    : undefined;

export const checkReservedRefusal = (
  result: ProbeResult,
  scope: string,
): Effect.Effect<void, TckError> => {
  const className = reservedClass(scope);
  return className === undefined
    ? refuse("Not a reserved runtime-class scope", scope)
    : checkDenied(result, {
        status: 403,
        bodyIncludes: `${className} is a runtime class and is not reachable over /do/`,
      });
};

// Hostname, dotted IPv4, or bracketed IPv6, each with an optional port.
const HOST = new RegExp(
  "^(?:" +
    "[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)*\\.?" +
    "|\\[[0-9A-Fa-f:.]+\\]" +
    ")(?::\\d{1,5})?$",
);
export const validHost = (value: string) => HOST.test(value);

const lastValue = (value: string | undefined) =>
  value === undefined ? undefined : value.split(",").at(-1)!.trim();

/**
 * The documented forwarded-header policy, as an expectation rather than a
 * restatement of an observation: forwarded headers are ignored without a
 * trusted proxy, the last value wins with one, and an unusable host falls back.
 */
export const expectedOrigin = (policy: {
  readonly trustsForwarded: boolean;
  readonly hostHeader: string;
  readonly forwardedHost?: string;
  readonly forwardedProto?: string;
}) => {
  const base = validHost(policy.hostHeader) ? policy.hostHeader : FALLBACK_HOST;
  if (!policy.trustsForwarded) return `http://${base}`;
  const host = lastValue(policy.forwardedHost);
  const proto = lastValue(policy.forwardedProto);
  return `${proto === "https" || proto === "http" ? proto : "http"}://${
    host !== undefined && validHost(host) ? host : base
  }`;
};

const Echo = Schema.Struct({ url: Schema.String, host: Schema.String });

export const checkForwarded = (
  result: ProbeResult,
  path: string,
  policy: Parameters<typeof expectedOrigin>[0],
): Effect.Effect<void, TckError> =>
  Effect.gen(function* () {
    yield* checkAllowed(result, { status: 200 });
    const echo = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Echo))(
      result.body ?? "",
    ).pipe(
      Effect.mapError(
        (error) =>
          new TckError({
            phase: "security",
            message: `Probe ${result.label} did not echo a request URL`,
            detail: String(error),
          }),
      ),
    );
    const expected = expectedOrigin(policy) + path;
    if (echo.url !== expected)
      return yield* refuse(
        `Probe ${result.label} observed ${echo.url}, expected ${expected}`,
      );
  });

/** Protected state must be identical across a denial. */
export const checkUnchanged = (
  label: string,
  before: unknown,
  after: unknown,
): Effect.Effect<void, TckError> =>
  equal(after, before).pipe(
    Effect.mapError(
      (error) =>
        new TckError({
          phase: "security",
          message: `Protected state changed across a denied request: ${label}`,
          detail: error.message,
        }),
    ),
  );
