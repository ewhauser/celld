# Results dashboard

The TCK workflow (`.github/workflows/tck.yml` in celld) builds a static, searchable test matrix from each job’s JSON report and CI status. Pushes to `main` and manual runs build it in the `TCK dashboard` job and upload it as the `compatibility-site` artifact, including runs with failing tests. Pull requests skip it, since they run only the core suites.

The docs site publishes it at [ewhauser.github.io/celld/compatibility/](https://ewhauser.github.io/celld/compatibility/). The Site workflow downloads the dashboard from the latest TCK run on `main` that finished, and runs again whenever such a run completes. Cancelled or superseded runs do not publish. The dashboard's Documentation link leads back to the docs site.

If you rerun a workflow, rerun all jobs: evidence from an earlier attempt is deliberately rejected.

The page separates passes, accepted divergences, known bugs, failures, missing evidence, and unscheduled diagnostic cases. It includes suite diagnostics, individual observations, report downloads, and links to the exact commit and CI run. Missing, malformed, duplicate, or mismatched evidence cannot mark a run complete. Published evidence covers local runtime validation, not AWS qualification. Full logs stay in the CI artifacts; the website contains structured reports and observations.

To preview a run locally, download all `compatibility-summary-*` artifacts into `.cache/site-input/`, keeping their artifact-name directories, and provide the run context:

```json
{
  "repository": "ewhauser/celld",
  "sha": "FULL_COMMIT_SHA",
  "runId": "GITHUB_RUN_ID",
  "attempt": "1",
  "number": "GITHUB_RUN_NUMBER",
  "branch": "main",
  "conclusion": "success"
}
```

```sh
pnpm site:build --context .cache/site-context.json
python3 -m http.server 8765 --directory .cache/site
```

Open `http://localhost:8765`. Without input artifacts, the builder renders the complete matrix with missing-evidence states. CI supplies the context through GitHub’s environment variables and `CI_RESULT`. The site uses relative asset links so it works under a GitHub Pages project path.
