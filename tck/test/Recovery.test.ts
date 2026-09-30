import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import { checkFacetsRecovered, checkRecovered } from "../src/Recovery.js";
const state = {
  activation: "new",
  retained: "durable λ",
  deleted: null,
  transaction: "committed",
  rows: [{ id: 1, value: "committed" }],
  fired: false,
};
it.effect(
  "recovery requires a new activation and exact acknowledged state",
  () =>
    Effect.gen(function* () {
      yield* checkRecovered("old", state, false);
      for (const corrupt of [
        { ...state, activation: "old" },
        { ...state, retained: null },
        { ...state, deleted: "remove" },
        { ...state, transaction: "rollback" },
        { ...state, rows: [...state.rows, { id: 2, value: "rollback" }] },
        { ...state, rows: [] },
        { ...state, fired: true },
      ])
        expect(
          (yield* Effect.exit(checkRecovered("old", corrupt, false)))._tag,
        ).toBe("Failure");
      expect(
        (yield* Effect.exit(checkRecovered("old", state, true)))._tag,
      ).toBe("Failure");
      yield* checkRecovered("old", { ...state, fired: true }, true);
    }),
);

const facets = {
  activation: "new",
  balance: 10,
  rows: [{ id: 1, value: "facet λ" }],
};
it.effect(
  "facet recovery requires a new activation and the exact replicated facet state",
  () =>
    Effect.gen(function* () {
      yield* checkFacetsRecovered("old", facets);
      for (const corrupt of [
        { ...facets, activation: "old" },
        { ...facets, balance: null },
        { ...facets, balance: 0 },
        { ...facets, rows: [] },
        { ...facets, rows: [{ id: 1, value: "stale" }] },
        { ...facets, rows: [...facets.rows, { id: 2, value: "facet λ" }] },
      ])
        expect(
          (yield* Effect.exit(checkFacetsRecovered("old", corrupt)))._tag,
        ).toBe("Failure");
    }),
);

import { ownedStateVolume } from "../src/DiskLoss.js";
it.effect("disk loss refuses running, foreign, or non-state volumes", () =>
  Effect.gen(function* () {
    const project = "tck-test-recovery";
    const container = {
      Id: "id",
      Config: {
        Labels: {
          "com.docker.compose.project": project,
          "com.docker.compose.service": "celld",
        },
      },
      State: { Running: false, StartedAt: "time" },
      Mounts: [
        {
          Type: "volume",
          Name: `${project}_celld-state`,
          Destination: "/state",
        },
      ],
    };
    const volume = {
      Name: `${project}_celld-state`,
      Labels: {
        "com.docker.compose.project": project,
        "com.docker.compose.volume": "celld-state",
      },
    };
    expect(yield* ownedStateVolume(project, container, volume)).toBe(
      volume.Name,
    );
    for (const bad of [
      { ...container, State: { ...container.State, Running: true } },
      {
        ...container,
        Config: {
          Labels: {
            ...container.Config.Labels,
            "com.docker.compose.project": "other",
          },
        },
      },
      { ...container, Mounts: [{ Type: "bind", Destination: "/state" }] },
      {
        ...container,
        Mounts: [{ ...container.Mounts[0]!, Name: `${project}_minio-data` }],
      },
      { ...container, Mounts: [...container.Mounts, ...container.Mounts] },
    ])
      expect(
        (yield* Effect.exit(ownedStateVolume(project, bad, volume)))._tag,
      ).toBe("Failure");
    expect(
      (yield* Effect.exit(
        ownedStateVolume(project, container, {
          ...volume,
          Labels: { ...volume.Labels, "com.docker.compose.project": "other" },
        }),
      ))._tag,
    ).toBe("Failure");
  }),
);

it.effect("fleet disk removal validates each node's volume independently", () =>
  Effect.gen(function* () {
    const project = "tck-fleet";
    for (const service of ["celld2", "celld3"] as const) {
      const name = `${project}_${service}-state`;
      const container = {
        Id: "owned",
        Config: {
          Labels: {
            "com.docker.compose.project": project,
            "com.docker.compose.service": service,
          },
        },
        State: { Running: false, StartedAt: "time" },
        Mounts: [{ Type: "volume", Name: name, Destination: "/state" }],
      };
      const volume = {
        Name: name,
        Labels: {
          "com.docker.compose.project": project,
          "com.docker.compose.volume": `${service}-state`,
        },
      };
      expect(yield* ownedStateVolume(project, container, volume, service)).toBe(
        name,
      );
      expect(
        (yield* Effect.exit(
          ownedStateVolume(project, container, volume, "celld"),
        ))._tag,
      ).toBe("Failure");
      expect(
        (yield* Effect.exit(
          ownedStateVolume(
            project,
            container,
            {
              ...volume,
              Labels: {
                ...volume.Labels,
                "com.docker.compose.volume": "minio-data",
              },
            },
            service,
          ),
        ))._tag,
      ).toBe("Failure");
    }
  }),
);
