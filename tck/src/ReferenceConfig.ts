import { Schema } from "effect";
import { FixtureConfig } from "./FixtureConfig.js";

// Leaf module: the spawned Miniflare process decodes its config from here, so
// this file must not pull in the driver's layers (Artifacts, FileSystem, ...).
export const ReferenceConfig = Schema.Struct({
  config: FixtureConfig,
  modules: Schema.Record(Schema.String, Schema.String),
  name: Schema.String,
  scriptPath: Schema.String,
  directory: Schema.String,
  compatibilityDate: Schema.String,
  sha256: Schema.String,
  binding: Schema.Struct({ name: Schema.String, className: Schema.String }),
});
export const ReferenceReady = Schema.Struct({
  target: Schema.Struct({ name: Schema.String, baseUrl: Schema.String }),
  metadata: Schema.Record(Schema.String, Schema.Unknown),
});
