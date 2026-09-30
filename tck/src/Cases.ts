import { Effect } from "effect";
import {
  Transport,
  type CaseInput,
  type Observation,
  type Target,
  type TestCase,
} from "./Domain.js";
import { equal } from "./Oracle.js";

const jsonHeaders = { "content-type": "application/json" };
export const response = (
  body: unknown,
  status = 200,
  headers: Record<string, string> = jsonHeaders,
): Observation => ({ status, headers, body });
export const call = (
  target: Target,
  input: CaseInput,
  path: string,
  value?: unknown,
  suffix = "a",
) =>
  Effect.gen(function* () {
    const transport = yield* Transport;
    const url = new URL(path, "http://fixture.invalid");
    url.searchParams.set("name", `${input.namespace}-${suffix}`);
    return yield* transport.request(target, {
      path: `${url.pathname}${url.search}`,
      ...(value === undefined
        ? {}
        : {
            method: "POST" as const,
            headers: jsonHeaders,
            body: JSON.stringify(value),
          }),
    });
  });
export const define = (
  id: string,
  run: TestCase["run"],
  expected: (input: CaseInput) => unknown,
): TestCase => ({
  id,
  contract:
    "https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/",
  run,
  check: (value, input) => equal(value, expected(input)),
  compare: equal,
});
const storedValue = (input: CaseInput) => ({
  text: "λ",
  empty: "",
  zero: 0,
  flag: false,
  nil: null,
  nested: [input.seed, "x"],
});

export const initialCases: ReadonlyArray<TestCase> = [
  {
    ...define(
      "http.request-response",
      (target, input) =>
        Effect.gen(function* () {
          const transport = yield* Transport;
          return yield* transport.request(target, {
            path: "/http/echo?q=first&q=second",
            method: "POST",
            headers: {
              "x-tck-value": `seed-${input.seed}`,
              "content-type": "text/plain; charset=utf-8",
            },
            body: "hello λ 🌍\n",
          });
        }),
      (input) =>
        response(
          {
            method: "POST",
            query: ["first", "second"],
            header: `seed-${input.seed}`,
            body: "hello λ 🌍\n",
          },
          201,
          { "content-type": "application/json", "x-tck-response": "echo" },
        ),
    ),
    contract: "https://developers.cloudflare.com/workers/runtime-apis/request/",
  },
  define(
    "storage.round-trip",
    (target, input) =>
      Effect.gen(function* () {
        const observations: Observation[] = [];
        observations.push(
          yield* call(target, input, "/storage/get?key=item:a"),
        );
        observations.push(
          yield* call(
            target,
            input,
            "/storage/put?key=item:b",
            storedValue(input),
          ),
        );
        observations.push(
          yield* call(target, input, "/storage/put?key=item:a", "first"),
        );
        observations.push(
          yield* call(target, input, "/storage/get?key=item:b"),
        );
        observations.push(yield* call(target, input, "/storage/list"));
        observations.push(
          yield* call(
            target,
            input,
            "/storage/get?key=item:b",
            undefined,
            "isolated",
          ),
        );
        observations.push(
          yield* call(target, input, "/storage/delete?key=item:b"),
        );
        observations.push(
          yield* call(target, input, "/storage/get?key=item:b"),
        );
        observations.push(
          yield* call(target, input, "/storage/delete?key=item:b"),
        );
        return observations;
      }),
    (input) => [
      response({ present: false }),
      response({ stored: true }),
      response({ stored: true }),
      response({ present: true, value: storedValue(input) }),
      response([
        ["item:a", "first"],
        ["item:b", storedValue(input)],
      ]),
      response({ present: false }),
      response({ deleted: true }),
      response({ present: false }),
      response({ deleted: false }),
    ],
  ),
  define(
    "storage.transaction-rollback",
    (target, input) =>
      Effect.gen(function* () {
        return [
          yield* call(target, input, "/storage/put?key=balance", 7),
          yield* call(target, input, "/storage/rollback"),
          yield* call(target, input, "/storage/get?key=balance"),
          yield* call(target, input, "/storage/get?key=uncommitted"),
        ];
      }),
    () => [
      response({ stored: true }),
      response({ error: "tck intentional rollback" }),
      response({ present: true, value: 7 }),
      response({ present: false }),
    ],
  ),
  define(
    "sql.transaction-rollback",
    (target, input) =>
      Effect.gen(function* () {
        return [
          yield* call(target, input, "/sql/prepare"),
          yield* call(target, input, "/sql/rollback"),
          yield* call(target, input, "/sql/read"),
        ];
      }),
    () => [
      response({ prepared: true }),
      response({ error: "tck intentional rollback" }),
      response([{ id: 1, value: "committed" }]),
    ],
  ),
];
