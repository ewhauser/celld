import { coreCases } from "./CoreCases.js";
import { serviceCases } from "./ServiceCases.js";
import { nodeCases } from "./NodeCases.js";
import { flagCases } from "./FlagCases.js";
import { extensionCases } from "./ExtensionCases.js";
import { reproCases } from "./ReproCases.js";
import type { ApiSuite } from "./Suites.js";
const conformanceCases = [
  ...coreCases,
  ...serviceCases,
  ...nodeCases,
  ...flagCases,
  ...extensionCases,
];
export const cases = [...conformanceCases, ...reproCases];
export type Suite = ApiSuite;
export const suites = {
  all: conformanceCases,
  repros: reproCases,
  core: coreCases,
  bindings: serviceCases,
  node: nodeCases,
  flags: flagCases,
  extensions: extensionCases,
};
