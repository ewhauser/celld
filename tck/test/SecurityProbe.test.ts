import { expect, it } from "@effect/vitest";
import { parseHttp, parseRaw } from "../src/SecurityProbe.js";

const head = (status: string, headers: string) =>
  `HTTP/1.1 ${status}\r\n${headers}\r\n\r\n`;

it("a declared-length response is complete only once every body byte arrived", () => {
  const partial =
    head("413 Payload Too Large", "content-length: 22") + "request body";
  expect(parseHttp("probe", partial)).toBeUndefined();
  expect(parseHttp("probe", partial + " too large")).toEqual({
    label: "probe",
    status: 413,
    headers: { "content-length": "22" },
    body: "request body too large",
  });
});

it("a chunked response is decoded and incomplete until the final chunk", () => {
  const wire =
    head("404 Not Found", "transfer-encoding: chunked") +
    "3\r\nnot\r\n6\r\n found\r\n";
  expect(parseHttp("probe", wire)).toBeUndefined();
  expect(parseHttp("probe", wire + "0\r\n\r\n")?.body).toBe("not found");
});

it("a wire without a complete head is not a response", () => {
  expect(parseHttp("probe", "HTTP/1.1 413 Payload")).toBeUndefined();
  expect(parseRaw("probe", "garbage\r\n\r\n").error).toContain("unparsable");
});
