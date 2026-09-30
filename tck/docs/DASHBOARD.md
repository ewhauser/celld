# Results dashboard

The Compatibility workflow builds a static, searchable test matrix from each job’s JSON report and CI status. Main-branch runs publish it to GitHub Pages, including runs with failing tests. Pull requests and other branches produce a downloadable `compatibility-site` artifact without deploying. Cancelled or superseded runs do not publish. GitHub must support Pages for the repository’s visibility and account plan. Enable **Settings → Pages → Build and deployment → Source: GitHub Actions** for a fork.

If you rerun a workflow, rerun all jobs: evidence from an earlier attempt is deliberately rejected.

The page separates passes, accepted divergences, known bugs, failures, missing evidence, and unscheduled diagnostic cases. It includes suite diagnostics, individual observations, report downloads, and links to the exact commit and CI run. Missing, malformed, duplicate, or mismatched evidence cannot mark a run complete. Published evidence covers local runtime validation, not AWS qualification. Full logs stay in the CI artifacts; the website contains structured reports and observations.

To preview a run locally, download all `compatibility-summary-*` artifacts into `.cache/site-input/`, keeping their artifact-name directories, and provide the run context:

```json
{
  "repository": "ewhauser/celld-tck",
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
