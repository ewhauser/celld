import { Effect } from "effect";
import { endpoint } from "./CoreCases.js";
import { define, response } from "./Cases.js";
import { Transport, type Target } from "./Domain.js";
const assetDoc = "https://developers.cloudflare.com/workers/static-assets/";
const loaderDoc = "https://developers.cloudflare.com/dynamic-workers/";
// Static routing is decided by the platform before any fixture code runs, so
// these observations come from the public listener rather than a binding.
const direct = (target: Target, path: string) =>
  Effect.gen(function* () {
    const transport = yield* Transport;
    return yield* transport.request(target, { path });
  });
export const extensionCases = [
  endpoint(
    "wasm.module",
    "/wasm/add",
    { positive: 42, negative: -5, overflow: -2147483648 },
    "https://developers.cloudflare.com/workers/runtime-apis/webassembly/",
  ),
  endpoint(
    "assets.binding",
    "/assets/fetch",
    { status: 200, text: "hello asset λ\n", header: "present", missing: 404 },
    `${assetDoc}binding/`,
  ),
  endpoint(
    "assets.html-routing",
    "/assets/routing",
    {
      page: {
        status: 200,
        location: null,
        contentType: "text/html; charset=utf-8",
        page: "html",
        body: "<!doctype html>\n<title>tck page</title>",
      },
      pageHtml: {
        status: 307,
        location: "/page",
        contentType: null,
        page: null,
        body: null,
      },
      folder: {
        status: 307,
        location: "/folder/",
        contentType: null,
        page: null,
        body: null,
      },
      folderSlash: {
        status: 200,
        location: null,
        contentType: "text/html; charset=utf-8",
        page: null,
        body: "<!doctype html>\n<title>tck folder index</title>",
      },
      missing: {
        status: 404,
        location: null,
        contentType: null,
        page: null,
        body: null,
      },
    },
    `${assetDoc}routing/advanced/html-handling/`,
  ),
  endpoint(
    "assets.redirects",
    "/assets/redirects",
    {
      permanent: {
        status: 301,
        location: "/page",
        contentType: null,
        page: null,
        body: null,
      },
      temporary: {
        status: 302,
        location: "/page",
        contentType: null,
        page: null,
        body: null,
      },
      unmatched: {
        status: 404,
        location: null,
        contentType: null,
        page: null,
        body: null,
      },
    },
    `${assetDoc}routing/advanced/redirects/`,
  ),
  {
    ...define(
      "assets.worker-first",
      (target) =>
        Effect.gen(function* () {
          // A Worker route and an asset exist at both paths; only the static
          // routing rules decide which one answers.
          return [
            yield* direct(target, "/shadowed.txt"),
            yield* direct(target, "/asset-first/note.txt"),
          ];
        }),
      () => [
        response({ served: "worker", path: "/shadowed.txt" }),
        {
          status: 200,
          headers: { "content-type": "text/plain; charset=utf-8" },
          body: "asset-first λ\n",
        },
      ],
    ),
    contract: `${assetDoc}binding/#run_worker_first`,
  },
  endpoint(
    "dynamic.fetch",
    "/dynamic/fetch",
    { status: 200, text: "dynamic λ" },
    "https://developers.cloudflare.com/workers/runtime-apis/bindings/worker-loader/",
  ),
  endpoint(
    "dynamic.props",
    "/dynamic/props",
    {
      tenant: { props: { tenant: "alpha", seed: 7 } },
      other: { props: { tenant: "beta", seed: 9 } },
    },
    `${loaderDoc}api-reference/`,
  ),
  endpoint(
    "dynamic.bindings",
    "/dynamic/bindings",
    { token: "token λ", count: 3, upstream: "gateway /binding" },
    `${loaderDoc}usage/bindings/`,
  ),
  endpoint(
    "dynamic.outbound",
    "/dynamic/outbound",
    {
      blocked: { global: "blocked", binding: "gateway /binding" },
      gateway: { global: "gateway /global", binding: "gateway /binding" },
    },
    `${loaderDoc}usage/egress-control/`,
  ),
  endpoint(
    "dynamic.limits",
    "/dynamic/limits",
    { token: "limited λ", count: 1, upstream: "gateway /binding" },
    `${loaderDoc}usage/limits/`,
  ),
  endpoint(
    "facets.outbound-transaction",
    "/facets/outbound",
    {
      control: { balance: 40, outbound: "gateway /facet" },
      inTransaction: { balance: 40, outbound: "gateway /facet" },
      after: { balance: 40 },
    },
    "https://celld.dev/docs/cloudflare-compat/#durable-object-facets",
  ),
  endpoint(
    "facets.isolation",
    "/facets/isolation",
    { a: { n: 1 }, b: { n: 2 }, c: { n: 1 } },
    "https://celld.dev/docs/cloudflare-compat/#durable-object-facets",
  ),
  endpoint(
    "facets.transaction",
    "/facets/transaction",
    {
      seeded: { seeded: true },
      initial: { balance: 10 },
      commit: { inside: 20 },
      committed: { balance: 20 },
      rollback: { inside: 30 },
      restored: { balance: 20 },
    },
    "https://celld.dev/docs/cloudflare-compat/#durable-object-facets",
  ),
].map((test) => ({ ...test, fixture: "extensions" as const }));
