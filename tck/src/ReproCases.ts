import { endpoint } from "./CoreCases.js";
const rejected = (name: string, domException = false) => ({
  outcome: "rejected",
  name,
  domException,
});
export const reproCases = [
  endpoint(
    "repro.body-readers",
    "/body",
    ["Request", "Response"].flatMap((kind) => [
      ...["text", "json", "arrayBuffer", "blob", "formData"].map((method) => ({
        kind,
        method,
        before: false,
        after: true,
        second: rejected("TypeError"),
      })),
      { kind, method: "null-body-control", first: "", second: "", used: false },
    ]),
    "https://fetch.spec.whatwg.org/#concept-body-consume-body",
  ),
  endpoint(
    "repro.storage-errors",
    "/storage",
    {
      asyncLimit: rejected("TypeError"),
      syncLimit: rejected("TypeError"),
      asyncPut: rejected("DataCloneError", true),
      syncPut: rejected("DataCloneError", true),
      batchPut: rejected("DataCloneError", true),
      absent: true,
      validLimit: [],
    },
    "https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/",
  ),
].map((test) => ({ ...test, fixture: "repro" as const }));
