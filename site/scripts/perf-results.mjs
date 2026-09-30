// Render the performance results page from celld-perf result files.
// The Site workflow downloads the Performance workflow's `perf-results`
// artifact into site/perf-results/; PERF_RESULTS points elsewhere, for
// example at a local `target/perf`. Without results the page still explains
// the numbers and how to produce them.
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';

const groups = [
  { prefix: 'S', title: 'Single node', about: 'One node on the `dev` backend (its local SQLite store) unless the scenario needs a bucket.' },
  { prefix: 'F', title: 'Fleet and injected faults', about: 'Three nodes on MinIO: fleet proofs, forwarding, takeover, a slow follower, a frozen owner, a throttling bucket.' },
  { prefix: 'N', title: 'Network faults', about: 'Three nodes on MinIO, with the harness delaying, cutting, or resetting links between nodes and to the bucket.' },
];

function files(dir) {
  if (!existsSync(dir)) return [];
  return readdirSync(dir, { withFileTypes: true, recursive: true })
    .filter(entry => entry.isFile() && entry.name === 'result.json')
    .map(entry => path.join(entry.parentPath, entry.name));
}

function readJson(file) {
  return existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')) : null;
}

// Result text comes from a CI artifact; keep it from breaking the page.
function text(value) {
  return String(value ?? '').replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' })[c]).replace(/\|/g, '\\|').replace(/\s+/g, ' ').trim();
}

function median(values) {
  const sorted = values.filter(Number.isFinite).sort((a, b) => a - b);
  if (!sorted.length) return null;
  const mid = sorted.length >> 1;
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function perOk(total, phase) {
  return Number.isFinite(total) && phase.ok > 0 ? total / phase.ok : null;
}

// A phase with no successes has no latency to report.
function latency(phase, quantile) {
  return phase.ok > 0 ? phase.latency_us?.[quantile] : null;
}

const number = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 });
const fmt = {
  rate: v => (v == null ? '–' : number.format(v)),
  count: v => (v == null ? '–' : number.format(v)),
  ms: v => (v == null ? '–' : v < 100_000 ? (v / 1000).toFixed(v < 10_000 ? 2 : 1) : number.format(v / 1000)),
  per: digits => v => (v == null ? '–' : v.toFixed(digits)),
};

const columns = [
  { head: 'Offered/s', value: p => p.offered_rate, show: fmt.rate },
  { head: 'OK/s', value: p => p.achieved_rate, show: fmt.rate },
  { head: 'Errors', value: p => Object.values(p.errors ?? {}).reduce((a, b) => a + b, 0), show: fmt.count },
  { head: 'p50', value: p => latency(p, 'p50'), show: fmt.ms },
  { head: 'p99', value: p => latency(p, 'p99'), show: fmt.ms },
  { head: 'p99.9', value: p => latency(p, 'p999'), show: fmt.ms },
  { head: 'Bucket/OK', value: p => perOk(p.server?.bucket_requests_total, p), show: fmt.per(3) },
  { head: 'Core/OK', value: p => perOk(p.server?.counters?.['core.messages'], p), show: fmt.per(2) },
];

// Natural order: S2 before S10.
function order(a, b) {
  const key = name => name.match(/^([A-Z]+)(\d+)/)?.slice(1) ?? [name, '0'];
  const [pa, na] = key(a);
  const [pb, nb] = key(b);
  return pa.localeCompare(pb) || Number(na) - Number(nb) || a.localeCompare(b);
}

// One table for a scenario (or one of its variants), with the median of
// each metric over the repeats.
function table(runs) {
  const names = [];
  for (const run of runs) for (const phase of run.phases ?? []) if (!names.includes(phase.name)) names.push(phase.name);
  if (!names.length) return '';
  const rows = names.map(name => {
    const phases = runs.map(run => (run.phases ?? []).find(p => p.name === name)).filter(Boolean);
    return `| ${text(name)} | ${columns.map(c => c.show(median(phases.map(c.value)))).join(' | ')} |`;
  });
  return [`| Phase | ${columns.map(c => c.head).join(' | ')} |`, `| --- |${' ---: |'.repeat(columns.length)}`, ...rows].join('\n');
}

function verdict(runs) {
  const notes = [];
  const stopped = runs.find(run => run.error);
  // Drop the pointer to the node log, which names a path on the runner.
  if (stopped) notes.push(`Stopped early: ${text(stopped.error.split('; see ')[0]).slice(0, 240)}.`);
  const failed = new Set();
  const timing = new Set();
  for (const run of runs) for (const phase of run.phases ?? []) for (const check of phase.checks ?? []) {
    if (check.pass === false) (check.timing ? timing : failed).add(`${text(phase.name)}: ${text(check.metric)}`);
  }
  if (failed.size) notes.push(`**Count check failed** (${[...failed].join('; ')}).`);
  if (timing.size) notes.push(`Timing check missed, which does not fail a run (${[...timing].join('; ')}).`);
  const sweeps = runs.map(run => run.verify).filter(v => v && !v.skipped && v.cells > 0);
  if (sweeps.length) {
    const lost = Math.max(...sweeps.map(v => (v.violations?.length ?? 0) + (v.unreadable ?? 0)));
    notes.push(lost ? `**Verification found ${lost} lost or unreadable cell(s) in a repeat.**` : `Verification: every acknowledged write read back (${number.format(Math.max(...sweeps.map(v => v.cells)))} cells).`);
  }
  if (!stopped && !failed.size && runs.some(run => run.phases?.length)) notes.unshift('Count checks passed.');
  return notes.join(' ');
}

function caveat(run, runner, host) {
  const size = host ? ` (${runner?.cpus ?? host.cpus} CPUs, ${host.os} ${host.arch})` : '';
  const machine = runner?.environment === 'self-hosted' ? `a self-hosted runner${size}`
    : run || !host ? `a small, shared GitHub-hosted runner${size}`
    : `a local run${size}`;
  return `:::caution[\\* Not real-world numbers]
${host ? 'These numbers come' : 'The published numbers come'} from ${machine}. The load generator and every node share that one machine, and the bucket is a local store or MinIO that answers in well under a millisecond, where a real bucket takes tens. The runner's CPU, disk, and neighbours also change from run to run.

Read them for the shape of a result and for how it moves between commits, not for what celld does in production or on your hardware. To get numbers for your own machine, [run the scenarios yourself](#run-it-yourself).
:::`;
}

const howTo = (repo) => `## Run it yourself

The scenarios are in \`crates/perf\`. Build \`celld-perf\` and a node in the optimized \`lab\` profile; a debug build is too slow to measure.

\`\`\`sh
cargo build --profile lab -p celld -p celld-perf --features celld/perf
target/lab/celld-perf list
target/lab/celld-perf run S2 --repeat 3     # one scenario, one dev node
target/lab/celld-perf run all --repeat 3    # every single-node scenario
\`\`\`

The fleet and fault scenarios start several nodes on an S3-compatible bucket. Start MinIO, create a bucket named \`perf\`, then:

\`\`\`sh
AWS_ACCESS_KEY_ID=perf AWS_SECRET_ACCESS_KEY=perf-disposable-password \\
  target/lab/celld-perf run F1 F5 N2 --repeat 3 \\
  --backend s3 --bucket s3://perf --endpoint http://127.0.0.1:9000
\`\`\`

Each run prints a summary and writes \`target/perf/<run-id>/result.json\`. To compare two runs, or to see yours on this page:

\`\`\`sh
target/lab/celld-perf compare before/result.json after/result.json
cd site && PERF_RESULTS=../target/perf pnpm dev
\`\`\`

For steady numbers, use Linux on bare metal with a fixed CPU frequency and nothing else running. macOS fsyncs are full flushes of 10–15 ms, so its durability numbers are an order of magnitude slower. [Performance tests](${repo}/blob/main/docs/performance-tests.md) has the scenario catalog, the injected and network faults, and how to read each number.`;

export function renderPerformance({ site, repo }) {
  const dir = process.env.PERF_RESULTS ? path.resolve(site, process.env.PERF_RESULTS) : path.join(site, 'perf-results');
  const results = files(dir).map(readJson).filter(r => r?.schema === 'celld-perf.result.v1').sort((a, b) => b.started_unix_s - a.started_unix_s);
  const run = readJson(path.join(dir, 'run.json'));
  const runner = readJson(path.join(dir, 'runner.json'));

  // The newest result wins for each scenario and backend.
  const scenarios = new Map();
  for (const result of results) {
    const seen = new Set();
    for (const r of result.runs ?? []) {
      const key = `${r.name}\u0000${r.backend ?? result.backend}`;
      if (scenarios.has(key) && !seen.has(key)) continue;
      seen.add(key);
      if (!scenarios.has(key)) scenarios.set(key, { result, runs: [] });
      scenarios.get(key).runs.push(r);
    }
  }
  const entries = [...scenarios.values()].filter(({ runs }) => runs[0].name !== 'smoke');
  const newest = results[0];

  const out = [];
  if (!entries.length) {
    out.push(`No results are published yet. The [Performance workflow](${repo}/actions/workflows/perf.yml) runs the scenarios nightly on main, and this page shows the latest run.\\*`, '', caveat(run, runner, null));
  } else {
    const when = new Date(newest.started_unix_s * 1000).toISOString().slice(0, 10);
    const commit = newest.commit ? `[\`${newest.commit.slice(0, 7)}\`](${repo}/commit/${newest.commit})` : 'an unknown commit';
    const version = String(newest.celld?.version ?? '').match(/celld \S+( \(\w+\))?/)?.[0] ?? 'celld';
    const source = run?.url ? `the [${run.event === 'schedule' ? 'nightly' : 'manual'} Performance run](${run.url})` : 'local results';
    out.push(`Results from ${source} on ${when}, at ${commit} (${text(version)}). Each number is the median over the repeats of a scenario.\\*`, '', caveat(run, runner, newest.host));
    out.push('', '| Column | Meaning |', '| --- | --- |',
      '| Offered/s, OK/s | Requests the open-loop generator sent per second, and how many succeeded |',
      '| p50, p99, p99.9 | Latency in milliseconds from each request\'s scheduled start, so queueing counts |',
      '| Bucket/OK, Core/OK | Bucket requests and core messages per successful request; these do not depend on the machine |');
    for (const group of groups) {
      const members = entries.filter(({ runs }) => runs[0].name.startsWith(group.prefix) && /^\d/.test(runs[0].name.slice(group.prefix.length)));
      if (!members.length) continue;
      out.push('', `## ${group.title}`, '', group.about);
      const byBase = new Map();
      for (const entry of members) {
        const base = entry.runs[0].name.replace(/\[.*$/, '');
        if (!byBase.has(base)) byBase.set(base, []);
        byBase.get(base).push(entry);
      }
      for (const base of [...byBase.keys()].sort(order)) {
        const variants = byBase.get(base);
        const first = variants[0].runs[0];
        const backend = first.backend ?? variants[0].result.backend;
        out.push('', `### ${text(base)}`, '', text(first.description));
        out.push('', `${first.nodes} node${first.nodes === 1 ? '' : 's'}, \`${text(backend)}\` backend, ${variants[0].runs.length} repeat${variants[0].runs.length === 1 ? '' : 's'}.`);
        for (const { runs } of variants.sort((a, b) => a.runs[0].name.localeCompare(b.runs[0].name))) {
          const variant = runs[0].name.match(/\[(.*)\]$/)?.[1];
          if (variant) out.push('', `**${text(variant)}**`);
          const rows = table(runs);
          if (rows) out.push('', rows);
          const note = verdict(runs);
          if (note) out.push('', note);
        }
      }
    }
  }
  out.push('', howTo(repo));
  return out.join('\n');
}
