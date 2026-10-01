# Fork documentation site

Astro + Starlight, matching celld-operator's documentation stack and visual theme.
The upstream runtime documentation stays at https://celld.dev/docs/.

```sh
cd site
pnpm install --frozen-lockfile
pnpm dev       # http://localhost:4321/celld/
pnpm build     # sync source docs, build search, validate internal links
pnpm preview
```

Use Node from `.node-version` and the pinned pnpm version in `package.json`.

Author the overview, installation, and bug-fix guide in `src/content/docs/`.
`scripts/sync-docs.mjs` explicitly allows only fork sources: `docs/fork-builds.md`,
`docs/previews.md`, `docs/export.md`, `docs/dynamodb-control.md`, and the Metrics
section of `docs/telemetry.md`. The generated `fork/` pages are ignored. Edit links
lead to their canonical source, and repository links are rewritten for the site.
Do not glob or mirror the upstream `docs/` tree. Add new fork sources explicitly.

The compatibility results page at `/compatibility/` is not built here. The TCK
workflow builds it from `tck/` on each push to `main`, and the Site workflow
downloads the latest one from a run that finished, then redeploys whenever such
a run completes. Locally, `pnpm sync` writes a placeholder; to see real results,
run `pnpm site:build --output ../site/public/compatibility --docs-url ../` in
`tck/` (see `tck/docs/DASHBOARD.md` for feeding it reports).

The performance results page at `fork/performance/` is rendered by
`scripts/perf-results.mjs` from celld-perf result files. The Site workflow downloads
the `perf-results` artifact from the latest Performance run on `main` that finished
into `perf-results/`, and redeploys whenever such a run completes. Without results the
page keeps its caveat and instructions. To render local results, run
`PERF_RESULTS=../target/perf pnpm dev`.

Maintain release status in the overview and install guide when publishing a new
fork release. Keep in-progress features clearly separate from released behavior.
Search includes the generated reference pages. The design document stays on GitHub.

`.github/workflows/site.yaml` builds pull requests and deploys pushes to `main`.
Enable Settings → Pages → Source → GitHub Actions once in the repository.
The `github-pages` environment must permit deployments from `main`.
PR artifacts do not deploy. The deployment URL is https://ewhauser.github.io/celld/.

For another host/base path, override both values:

```sh
SITE_URL=https://docs.example.com SITE_BASE=/ pnpm build
```

Before merging, run `pnpm build` and inspect desktop/mobile pages, search,
upstream navigation, and generated-page edit links. Internal link validation is
part of the build; external URLs are not network-checked by that validator.
