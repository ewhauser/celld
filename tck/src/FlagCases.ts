import { endpoint } from "./CoreCases.js";
import type { TestCase } from "./Domain.js";

/**
 * Two compatibility switches whose default is already on at the pinned
 * compatibility date 2026-07-30, exercised against their documented disable
 * counterparts. The identical `core` fixture is rebuilt as the `flags` fixture
 * with `delete_all_preserves_alarm` and `no_websocket_standard_binary_type`
 * (see `Build.ts`), so both variants answer the same `/flags/report` request and
 * only the flag-controlled observations differ.
 */
const contract =
  "https://developers.cloudflare.com/workers/configuration/compatibility-flags/";

const report = (
  id: string,
  binaryType: string,
  alarmPresent: boolean,
): TestCase =>
  endpoint(
    id,
    "/flags/report",
    { binaryType, deleteAll: { values: [], alarmPresent } },
    contract,
  );

export const flagCases: ReadonlyArray<TestCase> = [
  report("flags.enabled-defaults", "blob", false),
  {
    ...report("flags.disabled-counterparts", "arraybuffer", true),
    fixture: "flags" as const,
  },
];
