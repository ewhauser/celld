import { escapeMarkup } from "../Escape.js";
import type { Cell, SiteModel, Status } from "./Model.js";
export const escapeHtml = (value: unknown) =>
  escapeMarkup(String(value), "&#39;");
const json = (value: unknown) =>
  JSON.stringify(value)
    .replaceAll("<", "\\u003c")
    .replaceAll(">", "\\u003e")
    .replaceAll("&", "\\u0026")
    .replaceAll("\u2028", "\\u2028")
    .replaceAll("\u2029", "\\u2029");
export const labels: Record<Status, string> = {
  pass: "Pass",
  fail: "Fail",
  divergence: "Divergence",
  "known-bug": "Known bug",
  "reference-error": "Reference error",
  "infrastructure-error": "Infrastructure error",
  missing: "No report",
  "not-scheduled": "Not scheduled",
  "not-applicable": "—",
};
const badge = (status: Status, content = labels[status]) =>
  `<span class="badge ${status}"><span class="dot" aria-hidden="true"></span>${escapeHtml(content)}</span>`;
const cellHtml = (cell: Cell, index: number, side: string) =>
  cell.result
    ? `<button class="result-button" data-result="${index}:${side}" aria-label="${escapeHtml(labels[cell.status])}: view case evidence">${badge(cell.status)}<span class="inspect" aria-hidden="true">↗</span></button>`
    : `<span title="${cell.status === "not-applicable" ? "This scenario checks celld invariants, without a workerd comparison." : cell.status === "not-scheduled" ? "Diagnostic repro cases are available separately, outside the default CI matrix." : "Expected result was not received."}">${badge(cell.status)}</span>`;
const groupLabels: Record<string, string> = {
  api: "API compatibility",
  recovery: "Recovery & resilience",
  qualification: "Qualification",
};

const head = () =>
  `<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="color-scheme" content="light"><meta name="description" content="celld test results and compatibility matrix."><title>Test matrix · celld-tck</title><link rel="stylesheet" href="./style.css"><script src="./client.js" defer></script></head>`;

const topbar = (repo: string) =>
  `<header class="topbar"><a href="./" class="brand" aria-label="celld-tck home"><span class="mark" aria-hidden="true"><i></i><i></i><i></i><i></i></span>celld<span class="brand-divider">/</span><span class="muted">tck</span></a><nav aria-label="Main"><a class="source-link" href="${repo}">GitHub <span aria-hidden="true">↗</span></a></nav></header>`;

const runHeader = (model: SiteModel, repo: string, runUrl: string) => {
  const { run } = model;
  return `<main><section class="run-header" aria-label="CI run"><div class="run-title"><h1>Test results</h1><span class="run-state ${model.complete ? "healthy" : "attention"}">${model.complete ? "Complete" : "Needs attention"}</span></div><div class="run-meta"><a href="${runUrl}">Run #${escapeHtml(run.number)} ↗</a><span>${escapeHtml(run.branch)}</span><a class="mono" href="${repo}/commit/${encodeURIComponent(run.sha)}">${escapeHtml(run.sha.slice(0, 8))}</a><span>Attempt ${escapeHtml(run.attempt)}</span><time datetime="${escapeHtml(model.generatedAt)}" title="Generated">${escapeHtml(model.generatedAt.replace("T", " ").slice(0, 16))} UTC</time></div></section>`;
};

const metrics = (
  model: SiteModel,
  totals: {
    total: number;
    missing: number;
    exceptions: number;
    failures: number;
  },
) => {
  const { counts } = model;
  const { total, missing, exceptions, failures } = totals;
  return `<section class="metrics" aria-label="Result totals"><div><span class="metric-label">Results</span><strong>${total}<small> / ${total + missing}</small></strong></div><div><span class="metric-label">Passed</span><strong class="green">${counts.pass ?? 0}</strong></div><div><span class="metric-label">Accepted exceptions</span><strong class="amber">${exceptions}</strong><p>${counts.divergence ?? 0} divergences · ${counts["known-bug"] ?? 0} known bugs</p></div><div><span class="metric-label">Failed / missing</span><strong class="${failures + missing ? "red" : "muted"}">${failures + missing}</strong><p>${failures} failed · ${missing} missing</p></div></section>`;
};

const suiteCard = (suite: SiteModel["suites"][number]) =>
  `<details class="suite-card ${suite.status}"><summary><span class="sr-only">${escapeHtml(suite.status)}: </span><span class="suite-indicator" aria-hidden="true"></span><span>${escapeHtml(suite.label)}<small>${suite.expected ? `${suite.report?.cases.filter((result) => result.status === "pass").length ?? 0} passed · ${suite.expected} scheduled` : "Formatting · lint · types · unit tests"}</small></span><span class="chevron" aria-hidden="true">⌄</span></summary><div class="suite-detail"><p>Job: <strong>${escapeHtml(suite.conclusion)}</strong></p>${suite.problems.map((problem) => `<p class="problem">${escapeHtml(problem)}</p>`).join("")}${suite.report ? `<p><a href="./reports/${suite.key}.json" download>Download report JSON ↓</a></p><p class="mono">${escapeHtml(suite.report.runId)}</p>` : ""}</div></details>`;

const suiteCards = (model: SiteModel) =>
  `<section class="suites-section" aria-label="CI suite status"><div class="section-heading"><h2>Suites</h2></div><div class="suite-grid">${model.suites.map(suiteCard).join("")}</div></section>`;

const matrixRow = (row: SiteModel["rows"][number], index: number) =>
  `<tr data-row="${index}"><th scope="row"><span class="family">${escapeHtml(row.family)}</span><span class="case-name">${escapeHtml(row.id.slice(row.family.length + 1))}</span>${row.contract && row.contract.startsWith("https://") ? `<a class="contract-link" href="${escapeHtml(row.contract)}" aria-label="Read contract for ${escapeHtml(row.id)}">↗</a>` : ""}</th><td><span class="suite-name">${escapeHtml(groupLabels[row.group] ?? row.group)}</span></td><td>${cellHtml(row.reference, index, "reference")}</td><td>${cellHtml(row.candidate, index, "candidate")}</td></tr>`;

const matrixSection = (model: SiteModel) =>
  [
    `<section id="matrix" class="matrix-section"><div class="section-heading"><div><h2>Test matrix <span class="count">${model.rows.length}</span></h2></div><a class="download" href="./results.json" download>Download results <span aria-hidden="true">↓</span></a></div>`,
    `<div class="filters"><div class="tabs" role="group" aria-label="Filter by suite group"><button data-group="all" aria-pressed="true">All suites</button><button data-group="api" aria-pressed="false">API</button><button data-group="recovery" aria-pressed="false">Recovery</button><button data-group="qualification" aria-pressed="false">Qualification</button></div><div class="filter-inputs"><label class="search"><span aria-hidden="true">⌕</span><span class="sr-only">Search cases</span><input type="search" id="search" placeholder="Find a case…" autocomplete="off"></label><label><span class="sr-only">Filter by result</span><select id="status"><option value="all">All outcomes</option><option value="attention">Needs attention</option><option value="exceptions">Accepted exceptions</option><option value="pass">Passed</option><option value="not-scheduled">Not scheduled</option></select></label></div></div>`,
    `<div class="matrix-meta"><span id="visible-count" role="status">${model.rows.length} cases</span><span>Click a result for details <span aria-hidden="true">↗</span></span></div><div class="table-scroll"><table><thead><tr><th scope="col">Case / contract</th><th scope="col">Suite</th><th scope="col">Reference <small>workerd × 2</small></th><th scope="col">Candidate <small>celld</small></th></tr></thead><tbody>${model.rows.map(matrixRow).join("")}</tbody></table></div><div id="empty" hidden class="empty"><strong>No matching cases</strong><button id="clear">Clear filters</button></div><noscript><p>All cases are shown. Enable JavaScript to filter results and inspect individual observations; report downloads work without it.</p></noscript>`,
    `<div class="legend">${["pass", "divergence", "known-bug", "fail", "missing"].map((status) => badge(status as Status)).join("")}<span>— Not applicable</span></div></section>`,
  ].join("\n");

const scopeDetails = (model: SiteModel, repo: string) =>
  `<details id="scope" class="scope-details"><summary>Scope and exclusions</summary><p>${escapeHtml(model.scope)}</p><p>Local validation only. AWS unqualified. <a href="${repo}/blob/main/docs/DESIGN.md">Test design ↗</a></p><div class="boundaries">${model.excluded.map((entry) => `<details><summary>${escapeHtml(entry.family)}</summary><p>${escapeHtml(entry.reason)}</p></details>`).join("")}<details><summary>Diagnostic repros</summary><p>Run separately with the repros suite; excluded from default CI.</p></details></div></details>`;

const evidenceDialog = () =>
  `<dialog id="evidence" aria-labelledby="evidence-title"><div class="dialog-head"><p class="eyebrow" id="evidence-suite"></p><button id="close" aria-label="Close evidence">×</button></div><h2 id="evidence-title"></h2><p id="evidence-meta"></p><p><a id="evidence-download" download>Download suite report ↓</a></p><div id="evidence-content"></div></dialog>`;

export const renderSite = (model: SiteModel): string => {
  const { run, counts } = model;
  const repo = `https://github.com/${run.repository.split("/").map(encodeURIComponent).join("/")}`;
  const runUrl = `${repo}/actions/runs/${encodeURIComponent(run.runId)}/attempts/${encodeURIComponent(run.attempt)}`;
  const total = model.rows.reduce(
    (n, row) =>
      n + Number(!!row.reference.result) + Number(!!row.candidate.result),
    0,
  );
  const exceptions = (counts.divergence ?? 0) + (counts["known-bug"] ?? 0);
  const failures =
    (counts.fail ?? 0) +
    (counts["reference-error"] ?? 0) +
    (counts["infrastructure-error"] ?? 0);
  const missing = counts.missing ?? 0;
  return [
    `<!doctype html>`,
    head(),
    `<body><a class="skip" href="#matrix">Skip to test matrix</a>`,
    topbar(repo),
    runHeader(model, repo, runUrl),
    metrics(model, { total, missing, exceptions, failures }),
    suiteCards(model),
    matrixSection(model),
    scopeDetails(model, repo),
    `</main>`,
    evidenceDialog(),
    `<script id="site-data" type="application/json">${json(model)}</script></body></html>`,
  ].join("\n");
};
