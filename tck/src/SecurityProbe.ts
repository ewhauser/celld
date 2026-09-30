// In-network security probe.
//
// Two boundary probes cannot be driven from the host through the harness
// Transport: celld's peer and operator listener is never published outside the
// Compose network, and the Host/forwarded-header and body-limit cases need
// control of the request line and of a streamed body. This module is bundled
// into the fixture directory and executed inside the sidecar container, the
// same way StorageProxy.ts is.
//
// It performs no assertions. Every observation is printed as one JSON line and
// classified by the pure oracles in SecurityOracles.ts.
import { Effect } from "effect";
// oxlint-disable-next-line effect/use-http-client-service -- The probe deliberately controls raw sockets and request lines.
import { connect } from "node:net";

export interface HttpProbe {
  readonly kind: "http";
  readonly label: string;
  readonly url: string;
  readonly method?: string;
  readonly headers?: Readonly<Record<string, string>>;
  readonly body?: string;
  /** A declared body of this many filler bytes, generated here rather than carried in argv. */
  readonly bodyBytes?: number;
  /** A declared, well-formed fixture write whose total length is exact. */
  readonly bodyJson?: {
    readonly id: string;
    readonly payloadBytes: number;
  };
  /** Sends a chunked body of `count` frames of `bytes` each, without a declared length. */
  readonly chunks?: { readonly bytes: number; readonly count: number };
  readonly timeoutMs?: number;
}
export interface RawProbe {
  readonly kind: "raw";
  readonly label: string;
  readonly host: string;
  readonly port: number;
  readonly path: string;
  /** Written verbatim, so malformed and noncanonical values reach celld intact. */
  readonly hostHeader: string;
  readonly timeoutMs?: number;
}
export type Probe = HttpProbe | RawProbe;
export interface ProbeResult {
  readonly label: string;
  readonly status?: number;
  readonly headers?: Record<string, string>;
  readonly body?: string;
  readonly error?: string;
}

// Large enough for the operator listener's `/state`, whose cell inventory
// grows with the fleet, and still bounded so a runaway body cannot be carried
// back through the probe's stdout.
const BODY_LIMIT = 1024 * 1024;
const clamp = (value: string) => value.slice(0, BODY_LIMIT);

/** Decodes a chunked transfer body; returns undefined until the final chunk arrives. */
const dechunk = (raw: string): string | undefined => {
  let offset = 0;
  let out = "";
  for (;;) {
    const lineEnd = raw.indexOf("\r\n", offset);
    if (lineEnd === -1) return undefined;
    const size = Number.parseInt(raw.slice(offset, lineEnd), 16);
    if (!Number.isInteger(size)) return undefined;
    if (size === 0) return out;
    const dataStart = lineEnd + 2;
    if (raw.length < dataStart + size + 2) return undefined;
    out += raw.slice(dataStart, dataStart + size);
    offset = dataStart + size + 2;
  }
};

/** A parsed response, or undefined while the wire is still incomplete. */
export const parseHttp = (
  label: string,
  wire: string,
): ProbeResult | undefined => {
  const separator = wire.indexOf("\r\n\r\n");
  if (separator === -1) return undefined;
  const parsed = parseRaw(label, wire);
  if (parsed.error !== undefined || parsed.headers === undefined) return parsed;
  const raw = wire.slice(separator + 4);
  if (parsed.headers["transfer-encoding"]?.includes("chunked")) {
    const body = dechunk(raw);
    return body === undefined ? undefined : { ...parsed, body: clamp(body) };
  }
  const declared = Number(parsed.headers["content-length"] ?? "0");
  return Buffer.byteLength(raw) < declared ? undefined : parsed;
};

const chunkFrame = (data: Buffer) =>
  Buffer.concat([
    Buffer.from(`${data.length.toString(16)}\r\n`),
    data,
    Buffer.from("\r\n"),
  ]);

// A raw socket rather than fetch or node:http: celld answers an oversized
// upload with 413 and closes while the client is still sending. Both HTTP
// clients turn the resulting write error into a failed request and discard the
// response they had already received. Here the request head is sent first,
// the body follows in paced slices only while no response has arrived, and
// whatever the server wrote is parsed even if a later write fails.
const httpProbe = (probe: HttpProbe) =>
  Effect.callback<ProbeResult>((resume) => {
    const url = new URL(probe.url);
    const port = Number(url.port || (url.protocol === "https:" ? 443 : 80));
    const stream = probe.chunks;
    const body =
      probe.bodyBytes !== undefined
        ? Buffer.alloc(probe.bodyBytes, 120)
        : probe.bodyJson
          ? Buffer.from(
              JSON.stringify({
                id: probe.bodyJson.id,
                payload: "x".repeat(probe.bodyJson.payloadBytes),
              }),
            )
          : probe.body === undefined
            ? undefined
            : Buffer.from(probe.body);
    const headers: Record<string, string> = {
      host: url.host,
      connection: "close",
      ...Object.fromEntries(
        Object.entries(probe.headers ?? {}).map(([name, value]) => [
          name.toLowerCase(),
          value,
        ]),
      ),
      ...(stream
        ? { "transfer-encoding": "chunked" }
        : body
          ? { "content-length": String(body.length) }
          : {}),
    };
    const head =
      `${probe.method ?? "GET"} ${url.pathname}${url.search} HTTP/1.1\r\n` +
      Object.entries(headers)
        .map(([name, value]) => `${name}: ${value}\r\n`)
        .join("") +
      "\r\n";
    let settled = false;
    let wire = "";
    const timers: NodeJS.Timeout[] = [];
    const settle = (result: ProbeResult) => {
      if (settled) return;
      settled = true;
      for (const timer of timers) clearTimeout(timer);
      socket.destroy();
      resume(Effect.succeed(result));
    };
    const later = (ms: number, work: () => void) => {
      timers.push(setTimeout(work, ms));
    };
    // Slices are sent only while the server is silent; the first response byte
    // ends the upload so the denial is never raced by further writes.
    const send = (frames: readonly Buffer[], index: number) => {
      if (settled || wire.length > 0 || socket.destroyed) return;
      // Never half-close: celld drops a request whose client sends FIN before
      // the response, so `connection: close` leaves the hang-up to the server.
      if (index === frames.length) {
        if (stream) socket.write("0\r\n\r\n");
        return;
      }
      socket.write(frames[index]!, () =>
        later(stream ? 20 : 0, () => send(frames, index + 1)),
      );
    };
    const frames: Buffer[] = stream
      ? Array.from({ length: stream.count }, () =>
          chunkFrame(Buffer.alloc(stream.bytes, 120)),
        )
      : body
        ? Array.from({ length: Math.ceil(body.length / 16384) }, (_, i) =>
            body.subarray(i * 16384, (i + 1) * 16384),
          )
        : [];
    const socket = connect(port, url.hostname, () => {
      socket.write(head, () => {
        // Give a length-based rejection a moment to arrive before uploading.
        if (frames.length) later(100, () => send(frames, 0));
      });
    });
    socket.setTimeout(probe.timeoutMs ?? 20000, () =>
      settle({ label: probe.label, error: "timeout" }),
    );
    socket.on("data", (chunk) => {
      wire += String(chunk);
      const complete = parseHttp(probe.label, wire);
      if (complete) settle(complete);
    });
    socket.on("error", (error) => {
      // A write failure after the server responded is the server hanging up on
      // the rest of a rejected upload; the response already on the wire wins.
      if (wire.length === 0)
        settle({ label: probe.label, error: String(error) });
    });
    socket.on("close", () =>
      settle(
        wire.length === 0
          ? { label: probe.label, error: "closed without a response" }
          : (parseHttp(probe.label, wire) ?? parseRaw(probe.label, wire)),
      ),
    );
    return Effect.sync(() => {
      for (const timer of timers) clearTimeout(timer);
      socket.destroy();
    });
  });

export const parseRaw = (label: string, wire: string): ProbeResult => {
  const separator = wire.indexOf("\r\n\r\n");
  if (separator === -1) return { label, error: `truncated response: ${wire}` };
  const head = wire.slice(0, separator).split("\r\n");
  const status = Number(head[0]?.split(" ")[1]);
  if (!Number.isInteger(status))
    return { label, error: `unparsable status line: ${head[0]}` };
  const headers: Record<string, string> = {};
  for (const line of head.slice(1)) {
    const colon = line.indexOf(":");
    if (colon > 0)
      headers[line.slice(0, colon).trim().toLowerCase()] = line
        .slice(colon + 1)
        .trim();
  }
  return { label, status, headers, body: clamp(wire.slice(separator + 4)) };
};

const rawProbe = (probe: RawProbe) =>
  Effect.callback<ProbeResult>((resume) => {
    let settled = false;
    let wire = "";
    const socket = connect(probe.port, probe.host, () => {
      socket.write(
        `GET ${probe.path} HTTP/1.1\r\nHost: ${probe.hostHeader}\r\nconnection: close\r\n\r\n`,
      );
    });
    const settle = (result: ProbeResult) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resume(Effect.succeed(result));
    };
    socket.setTimeout(probe.timeoutMs ?? 20000, () =>
      settle({ label: probe.label, error: "timeout" }),
    );
    socket.on("data", (chunk) => {
      wire += String(chunk);
    });
    socket.on("error", (error) =>
      settle({ label: probe.label, error: String(error) }),
    );
    socket.on("close", () => settle(parseRaw(probe.label, wire)));
    return Effect.sync(() => socket.destroy());
  });

export const runProbes = (probes: readonly Probe[]) =>
  // Sequential on purpose: several probes observe the same node's state.
  Effect.forEach(probes, (probe) =>
    probe.kind === "raw" ? rawProbe(probe) : httpProbe(probe),
  );

const specification = process.argv[2];
if (specification)
  // oxlint-disable-next-line effect/effect-run-in-body -- Standalone probe entrypoint.
  Effect.runFork(
    runProbes(JSON.parse(specification) as readonly Probe[]).pipe(
      Effect.flatMap((results) =>
        Effect.sync(() => {
          for (const result of results) console.log(JSON.stringify(result));
        }),
      ),
    ),
  );
