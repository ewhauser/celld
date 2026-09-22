import { DatabaseSync } from "node:sqlite";
import type { Storage } from "../src/storage.ts";
export function memoryStorage(): Storage & { db: DatabaseSync } {
  const db = new DatabaseSync(":memory:");
  let depth = 0;
  return {
    db,
    sql: {
      exec(query, ...args) {
        return { toArray: () => db.prepare(query).all(...args) };
      },
    },
    transactionSync(callback) {
      const name = `sp${depth++}`;
      db.exec(`SAVEPOINT ${name}`);
      try {
        const result = callback();
        db.exec(`RELEASE ${name}`);
        return result;
      } catch (error) {
        db.exec(`ROLLBACK TO ${name}; RELEASE ${name}`);
        throw error;
      } finally {
        depth--;
      }
    },
    async sync() {},
  };
}
