import { Effect } from "effect";
import { encode, richValue } from "../shared/Codec.js";
import { operation, platform, rejection } from "./Platform.js";
export const storageOperation = (storage: DurableObjectStorage, path: string) =>
  Effect.gen(function* () {
    switch (path) {
      case "/sql/invalid-utf8":
        return yield* operation(() =>
          storage.sql
            .exec(
              "SELECT CAST(x'61ff62' AS TEXT) AS value, hex(CAST(x'61ff62' AS TEXT)) AS bytes",
            )
            .one(),
        );
      case "/sql/nested-sync":
        return yield* operation(() => {
          storage.sql.exec("CREATE TABLE nested_sync (n INTEGER)");
          const arities: number[] = [];
          const returned = storage.transactionSync((...args: unknown[]) => {
            arities.push(args.length);
            storage.sql.exec("INSERT INTO nested_sync VALUES (1)");
            const inner = storage.transactionSync((...innerArgs: unknown[]) => {
              arities.push(innerArgs.length);
              storage.sql.exec("INSERT INTO nested_sync VALUES (2)");
              return 42;
            });
            try {
              storage.transactionSync(() => {
                storage.sql.exec("INSERT INTO nested_sync VALUES (3)");
                // oxlint-disable-next-line effect/throw-in-effect-gen -- Synchronous rollback probe, caught by the enclosing transaction.
                throw new Error("rollback inner");
              });
            } catch (error) {
              if (
                !(error instanceof Error) ||
                error.message !== "rollback inner"
              )
                // oxlint-disable-next-line effect/throw-in-effect-gen -- Preserve unexpected synchronous transaction failures at the operation boundary.
                throw error;
            }
            storage.sql.exec("INSERT INTO nested_sync VALUES (4)");
            return inner;
          });
          try {
            storage.transactionSync(() => {
              storage.transactionSync(() =>
                storage.sql
                  .exec("INSERT INTO nested_sync VALUES (5)")
                  .toArray(),
              );
              // oxlint-disable-next-line effect/throw-in-effect-gen -- Outer rollback must undo a committed inner savepoint.
              throw new Error("rollback outer");
            });
          } catch (error) {
            if (!(error instanceof Error) || error.message !== "rollback outer")
              // oxlint-disable-next-line effect/throw-in-effect-gen -- Preserve unexpected synchronous transaction failures at the operation boundary.
              throw error;
          }
          return {
            arities,
            returned,
            rows: storage.sql
              .exec("SELECT n FROM nested_sync ORDER BY n")
              .toArray(),
          };
        });
      case "/storage/sync-committed": {
        yield* platform(() => storage.put("removed", "old"));
        yield* platform(() => storage.sync());
        yield* platform(() =>
          storage.put("retained", "durable λ", { allowUnconfirmed: true }),
        );
        yield* platform(() =>
          storage.delete("removed", { allowUnconfirmed: true }),
        );
        yield* operation(() => {
          storage.sql.exec(
            "CREATE TABLE synced (id INTEGER PRIMARY KEY, value TEXT)",
          );
          storage.sql
            .exec("INSERT INTO synced VALUES (1, 'committed')")
            .toArray();
        });
        yield* platform(() => storage.sync());
        // A second barrier with no intervening writes must also resolve.
        yield* platform(() => storage.sync());
        return {
          retained: yield* platform(() => storage.get("retained")),
          removed: (yield* platform(() => storage.get("removed"))) ?? null,
          rows: storage.sql.exec("SELECT * FROM synced ORDER BY id").toArray(),
        };
      }
      case "/sql/consumed-write-cursor": {
        return yield* Effect.gen(function* () {
          const returned = yield* operation(() => {
            storage.sql.exec("CREATE TABLE cursor_values (n INTEGER)");
            return storage.sql
              .exec(
                "INSERT INTO cursor_values VALUES (1), (2), (3) RETURNING n",
              )
              .toArray();
          });
          yield* platform(() => storage.sync());
          return {
            returned,
            rows: storage.sql
              .exec("SELECT n FROM cursor_values ORDER BY n")
              .toArray(),
          };
        });
      }
      case "/sql/open-read-cursor": {
        const cursor = yield* operation(() => {
          storage.sql.exec("CREATE TABLE read_values (n INTEGER)");
          storage.sql
            .exec("INSERT INTO read_values VALUES (1), (2), (3)")
            .toArray();
          return storage.sql.exec("SELECT n FROM read_values ORDER BY n");
        });
        const first = cursor.next().value;
        yield* platform(() => storage.sync());
        return { first, remaining: cursor.toArray() };
      }

      case "/storage/synchronous-kv": {
        return yield* operation(() => {
          const kv = storage.kv;
          kv.put("a", { value: 1 });
          kv.put("b", "λ");
          const a = kv.get("a");
          const list = [...kv.list({ reverse: true })];
          const removed = kv.delete("a");
          const missing = kv.get("a") === undefined;
          return { a, list, removed, missing };
        });
      }
      case "/sql/mixed-rollback": {
        yield* platform(() => storage.put("balance", 1));
        yield* operation(() =>
          storage.sql.exec("CREATE TABLE mixed (n INTEGER)"),
        );
        const error = yield* rejection(
          operation(() =>
            storage.transactionSync(() => {
              storage.kv.put("balance", 2);
              storage.sql.exec("INSERT INTO mixed VALUES (7)");
              // oxlint-disable-next-line effect/throw-in-effect-gen -- Throw synchronously to exercise transaction rollback; operation captures the error.
              throw new Error("rollback");
            }),
          ),
        );
        return {
          error,
          balance: yield* platform(() => storage.get("balance")),
          rows: storage.sql.exec("SELECT * FROM mixed").toArray(),
        };
      }
      case "/storage/rich": {
        yield* platform(() => storage.put("rich", richValue()));
        return encode(yield* platform(() => storage.get("rich")));
      }
      case "/storage/batch": {
        yield* platform(() =>
          storage.put({ "k:c": 3, "k:a": 1, "k:b": 2, other: 9 }),
        );
        const got = [
          ...(yield* platform(() => storage.get(["k:c", "missing", "k:a"]))),
        ];
        const forward = [
          ...(yield* platform(() =>
            storage.list({ prefix: "k:", start: "k:b", end: "k:d", limit: 1 }),
          )),
        ];
        const reverse = [
          ...(yield* platform(() =>
            storage.list({ prefix: "k:", reverse: true, limit: 2 }),
          )),
        ];
        const after = [
          ...(yield* platform(() =>
            storage.list({ startAfter: "k:a", end: "k:c" }),
          )),
        ];
        const deleted = yield* platform(() =>
          storage.delete(["k:a", "k:c", "missing"]),
        );
        return {
          got,
          forward,
          reverse,
          after,
          deleted,
          remaining: [...(yield* platform(() => storage.list()))],
        };
      }
      case "/flags/delete-all-alarm": {
        // delete_all_deletes_alarm (default from 2026-02-24) against its
        // delete_all_preserves_alarm counterpart: the stored values go either
        // way, only a pending alarm's survival differs between the variants.
        yield* platform(() => storage.put({ a: 1, b: "" }));
        yield* platform(() => storage.setAlarm(Date.now() + 3600000));
        yield* platform(() => storage.deleteAll());
        const alarm = yield* platform(() => storage.getAlarm());
        yield* platform(() => storage.deleteAlarm());
        return {
          values: [...(yield* platform(() => storage.list()))],
          alarmPresent: alarm !== null,
        };
      }
      case "/storage/delete-all": {
        yield* platform(() => storage.put({ a: 1, b: "" }));
        yield* platform(() => storage.setAlarm(Date.now() + 3600000));
        yield* platform(() => storage.deleteAll());
        return {
          values: [...(yield* platform(() => storage.list()))],
          alarm: yield* platform(() => storage.getAlarm()),
        };
      }
      case "/storage/commit": {
        const result = yield* platform(() =>
          storage.transaction((tx) =>
            Effect.runPromise(
              Effect.gen(function* () {
                yield* platform(() => tx.put("a", 7));
                const read = yield* platform(() => tx.get<number>("a"));
                yield* platform(() => tx.put("b", read! + 1));
                return read;
              }),
            ),
          ),
        );
        return {
          result,
          entries: [...(yield* platform(() => storage.list()))],
        };
      }
      case "/storage/explicit-rollback": {
        yield* platform(() => storage.put("a", 1));
        yield* platform(() =>
          storage.transaction((tx) =>
            Effect.runPromise(
              Effect.gen(function* () {
                yield* platform(() => tx.put("a", 2));
                tx.rollback();
              }),
            ),
          ),
        );
        return { value: yield* platform(() => storage.get("a")) };
      }
      case "/storage/invalid":
        return {
          limit: yield* rejection(platform(() => storage.list({ limit: -1 }))),
          unserializable: yield* rejection(
            platform(() => storage.put("fn", () => 1)),
          ),
        };
      case "/sql/types": {
        return yield* operation(() => {
          const sql = storage.sql;
          sql.exec(
            "CREATE TABLE typed (id INTEGER PRIMARY KEY, text TEXT, n REAL, empty TEXT, nil, bytes BLOB)",
          );
          const cursor = sql.exec(
            "INSERT INTO typed VALUES (?, ?, ?, ?, ?, ?) RETURNING id, text, n, empty, nil, bytes",
            1,
            "λ'",
            1.25,
            "",
            null,
            new Uint8Array([0, 128, 255]).buffer,
          );
          const columns = cursor.columnNames;
          const inserted = cursor
            .toArray()
            .map((row) => ({ ...row, bytes: encode(row.bytes) }));
          const raw = [...sql.exec("SELECT id, text, nil FROM typed").raw()];
          const count = sql.exec("SELECT COUNT(*) AS n FROM typed").one();
          return { columns, inserted, raw, count };
        });
      }
      case "/sql/constraints": {
        yield* operation(() =>
          storage.sql.exec(
            "CREATE TABLE unique_values (id INTEGER PRIMARY KEY, v TEXT UNIQUE NOT NULL)",
          ),
        );
        yield* operation(() =>
          storage.sql.exec("INSERT INTO unique_values VALUES (1, 'a')"),
        );
        const duplicate = yield* rejection(
          operation(() =>
            storage.sql.exec("INSERT INTO unique_values VALUES (2, 'a')"),
          ),
        );
        const nil = yield* rejection(
          operation(() =>
            storage.sql.exec("INSERT INTO unique_values VALUES (3, NULL)"),
          ),
        );
        const multiple = yield* rejection(
          operation(() =>
            storage.sql.exec("SELECT 1 UNION ALL SELECT 2").one(),
          ),
        );
        const empty = yield* rejection(
          operation(() => storage.sql.exec("SELECT 1 WHERE 0").one()),
        );
        return {
          duplicate: duplicate !== "accepted",
          nil: nil !== "accepted",
          multiple: multiple !== "accepted",
          empty: empty !== "accepted",
          rows: storage.sql.exec("SELECT * FROM unique_values").toArray(),
        };
      }
      case "/sql/commit":
        return yield* operation(() => {
          storage.sql.exec("CREATE TABLE counters (n INTEGER)");
          const returned = storage.transactionSync(() => {
            storage.sql.exec("INSERT INTO counters VALUES (1)");
            storage.sql.exec("UPDATE counters SET n = n + 2");
            return 42;
          });
          return {
            returned,
            row: storage.sql.exec("SELECT n FROM counters").one(),
          };
        });
      default:
        return undefined;
    }
  });
