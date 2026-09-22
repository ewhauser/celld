import { readFile } from "node:fs/promises";
import { createSupervisor } from "./server.mjs";
const path = process.env.CELLD_SANDBOX_CONFIG;
if (!path) throw new Error("CELLD_SANDBOX_CONFIG is required");
const config = JSON.parse(await readFile(path, "utf8"));
config.token = process.env.CELLD_SANDBOX_TOKEN;
config.callbackToken = process.env.CELLD_SANDBOX_CALLBACK_TOKEN;
const service = await createSupervisor(config);
service.server.on("runnerDiagnostic", (event) =>
  console.error(JSON.stringify({ event: "runner_diagnostic", ...event })),
);
service.server.listen(config.port ?? 19877, config.host ?? "127.0.0.1");
for (const signal of ["SIGINT", "SIGTERM"])
  process.once(signal, () => {
    void service.close().then(() => process.exit(0));
  });
