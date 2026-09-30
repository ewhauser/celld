import { Schema } from "effect";
export const FixtureConfig = Schema.Struct({
  name: Schema.String,
  main: Schema.String,
  compatibility_date: Schema.String,
  compatibility_flags: Schema.Array(Schema.String),
  durable_objects: Schema.Struct({
    bindings: Schema.Array(
      Schema.Struct({ name: Schema.String, class_name: Schema.String }),
    ),
  }),
  migrations: Schema.Array(
    Schema.Struct({
      tag: Schema.String,
      new_sqlite_classes: Schema.Array(Schema.String),
    }),
  ),
  kv_namespaces: Schema.optionalKey(
    Schema.Array(Schema.Struct({ binding: Schema.String, id: Schema.String })),
  ),
  d1_databases: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        binding: Schema.String,
        database_name: Schema.String,
        database_id: Schema.String,
      }),
    ),
  ),
  r2_buckets: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({ binding: Schema.String, bucket_name: Schema.String }),
    ),
  ),
  services: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        binding: Schema.String,
        service: Schema.String,
        entrypoint: Schema.String,
      }),
    ),
  ),
  queues: Schema.optionalKey(
    Schema.Struct({
      producers: Schema.Array(
        Schema.Struct({ binding: Schema.String, queue: Schema.String }),
      ),
      consumers: Schema.Array(
        Schema.Struct({
          queue: Schema.String,
          max_batch_size: Schema.Int,
          max_batch_timeout: Schema.Int,
          max_retries: Schema.Int,
          retry_delay: Schema.Int,
        }),
      ),
    }),
  ),
  workflows: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        binding: Schema.String,
        name: Schema.String,
        class_name: Schema.String,
      }),
    ),
  ),
  assets: Schema.optionalKey(
    Schema.Struct({
      directory: Schema.String,
      binding: Schema.String,
      // Wrangler accepts `true`/`false` or an array of route patterns, where a
      // `!/`-prefixed pattern keeps the default asset-first order for a path.
      run_worker_first: Schema.Union([
        Schema.Boolean,
        Schema.Array(Schema.String),
      ]),
      html_handling: Schema.optionalKey(
        Schema.Literals([
          "auto-trailing-slash",
          "force-trailing-slash",
          "drop-trailing-slash",
          "none",
        ]),
      ),
      not_found_handling: Schema.optionalKey(
        Schema.Literals(["single-page-application", "404-page", "none"]),
      ),
    }),
  ),
  worker_loaders: Schema.optionalKey(
    Schema.Array(Schema.Struct({ binding: Schema.String })),
  ),
});
