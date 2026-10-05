'use strict';

// Run the same HTTP harness repeatedly, alternating engine order to reduce
// order bias. Preserve every run and summarize variation instead of selecting
// the fastest result.
const { execFileSync } = require('node:child_process');
const { mkdirSync, readFileSync, writeFileSync, copyFileSync } = require('node:fs');
const { resolve, join } = require('node:path');
const ROOT = resolve(__dirname, '..');
const options = Object.fromEntries(process.argv.slice(2).map(arg => arg.replace(/^--/, '').split('=')));
for (const key of Object.keys(options)) {
  if (!['dimensions', 'records', 'batch-size', 'report-dir', 'repetitions'].includes(key) || !options[key]) throw new Error(`Invalid option: ${key}`);
}
const REPORT_ROOT = resolve(options['report-dir'] || process.env.NIODB_BENCHMARK_REPORT_DIR || join(ROOT, 'benchmarks'));
const OUTPUT = join(REPORT_ROOT, 'comparison');
const repetitions = Number(options.repetitions || process.env.NIODB_BENCHMARK_REPETITIONS || 3);
if (!Number.isInteger(repetitions) || repetitions < 2) throw new Error('Use at least two repetitions');
mkdirSync(OUTPUT, { recursive: true });
const runs = { niodb: [], sqlite: [] };
for (let iteration = 0; iteration < repetitions; iteration++) {
  for (const engine of iteration % 2 ? ['sqlite', 'niodb'] : ['niodb', 'sqlite']) {
    const output = join(OUTPUT, `${engine}-${iteration + 1}.json`);
    execFileSync(process.execPath, [join(__dirname, 'simulate-and-benchmark.cjs'), ...(engine === 'sqlite' ? ['--sqlite'] : [])], {
      cwd: ROOT, stdio: 'inherit',
      env: { ...process.env, NIODB_BENCHMARK_OUTPUT: output,
        ...(options.dimensions ? { NIODB_BENCHMARK_DIMENSIONS: options.dimensions } : {}),
        ...(options.records ? { NIODB_BENCHMARK_RECORDS: options.records } : {}),
        ...(options['batch-size'] ? { NIODB_BENCHMARK_BATCH_SIZE: options['batch-size'] } : {}),
      },
    });
    const report = JSON.parse(readFileSync(output, 'utf8'));
    if (report.verification.failures !== 0) throw new Error(`${engine}: verification failed`);
    runs[engine].push(report);
    copyFileSync(output, join(REPORT_ROOT, engine === 'niodb' ? 'latest.json' : 'sqlite.json'));
  }
}
function distribution(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return { median: sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2,
    min: sorted[0], max: sorted.at(-1) };
}
function summarize(reports) {
  return {
    records_per_second: distribution(reports.map(r => r.ingestion.records_per_second)),
    latency: Object.fromEntries(['point_lookup', 'collection_scan', 'vector_search', 'sql', 'event_publish'].map(metric => [metric,
      Object.fromEntries(['p50_ms', 'p95_ms', 'p99_ms'].map(percentile => [percentile,
        distribution(reports.map(r => r.latency[metric][percentile]))]))])),
    sql_by_query: reports[0].methodology.sql_queries.map((query, index) => ({ query,
      p50_ms: distribution(reports.map(r => r.latency.sql_by_query[index].p50_ms)) })),
    server_rss_bytes: reports.every(r => r.storage.server_rss_bytes !== null)
      ? distribution(reports.map(r => r.storage.server_rss_bytes)) : null,
  };
}
const summary = {
  measured_at: new Date().toISOString(), repetitions,
  methodology: 'Sequential HTTP workloads, alternating engine order; median/min/max across independent runs. Read tests follow ingestion and may overlap background projections.',
  run_files: Object.fromEntries(Object.keys(runs).map(engine => [engine, runs[engine].map((_, i) => `comparison/${engine}-${i + 1}.json`)])),
  niodb: summarize(runs.niodb), sqlite: summarize(runs.sqlite),
};
writeFileSync(join(REPORT_ROOT, 'comparison.json'), JSON.stringify(summary, null, 2) + '\n');
console.log('Median ingestion records/s:', summary.niodb.records_per_second.median, '(NioDB),', summary.sqlite.records_per_second.median, '(SQLite)');
console.table(Object.fromEntries(Object.keys(summary.niodb.latency).map(metric => [metric, {
  niodb_p50_ms: summary.niodb.latency[metric].p50_ms.median,
  sqlite_p50_ms: summary.sqlite.latency[metric].p50_ms.median,
  niodb_p99_ms: summary.niodb.latency[metric].p99_ms.median,
  sqlite_p99_ms: summary.sqlite.latency[metric].p99_ms.median,
}])));
