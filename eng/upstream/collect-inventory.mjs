#!/usr/bin/env node
/**
 * Collect the upstream issue and pull-request inventory as JSON.
 *
 * Read-only: calls GitHub's REST API through `gh` and writes one new file.
 * Counts are derived from the collected records. The repository's
 * open_issues_count is only a consistency hint: concurrent upstream activity
 * (or GitHub's cached counter) can make it disagree, so a mismatch is retried a
 * bounded number of times and then recorded as a warning instead of failing.
 */
import { execFile } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { promisify } from 'node:util';

const run = promisify(execFile);
const gh = process.env.GH_BIN ?? 'gh';
const attempts = Math.max(1, Number.parseInt(process.env.UPSTREAM_COLLECT_ATTEMPTS ?? '3', 10) || 3);
const retryDelay = Math.max(0, Number.parseInt(process.env.UPSTREAM_COLLECT_RETRY_DELAY_MS ?? '10000', 10) || 0);
const usage = 'Usage: node eng/upstream/collect-inventory.mjs --repository <owner/name> --upstream <40-hex-sha> --output <file>';

function parseArgs(argv) {
  const values = {};
  for (let index = 0; index < argv.length; index += 2) {
    const key = argv[index];
    const value = argv[index + 1];
    if (!key?.startsWith('--') || value === undefined) throw new Error(usage);
    values[key.slice(2)] = value;
  }
  if (!values.output || !/^[\w.-]+\/[\w.-]+$/.test(values.repository ?? '') || !/^[0-9a-f]{40}$/i.test(values.upstream ?? '')) {
    throw new Error(usage);
  }
  return values;
}

async function api(repository, endpoint, paginate = false) {
  const args = ['api', ...(paginate ? ['--paginate', '--slurp'] : []), `repos/${repository}${endpoint ? `/${endpoint}` : ''}`];
  const { stdout } = await run(gh, args, { maxBuffer: 64 * 1024 * 1024 });
  const parsed = JSON.parse(stdout);
  return paginate ? parsed.flat() : parsed;
}

function normalize(item, type) {
  return {
    number: item.number,
    type,
    title: item.title,
    url: item.html_url,
    state: item.merged_at ? 'merged' : item.state,
    draft: type === 'pull_request' ? Boolean(item.draft) : undefined,
    author: item.user?.login,
    createdAt: item.created_at,
    updatedAt: item.updated_at,
    closedAt: item.closed_at ?? undefined,
    mergedAt: item.merged_at ?? undefined,
    labels: (item.labels ?? []).map(label => (typeof label === 'string' ? label : label.name)),
    headCommit: item.head?.sha,
    baseBranch: item.base?.ref,
    mergeCommit: item.merge_commit_sha ?? undefined,
  };
}

async function collect(repository) {
  const [issueRecords, pulls, metadata] = await Promise.all([
    api(repository, 'issues?state=all&per_page=100&sort=created&direction=asc', true),
    api(repository, 'pulls?state=all&per_page=100&sort=created&direction=asc', true),
    api(repository, ''),
  ]);
  const issues = issueRecords.filter(item => !item.pull_request);
  if (metadata.has_issues !== false && issues.length === 0 && issueRecords.length > 0) {
    // Observed when the token lacks issue read access: only pull requests are returned.
    throw new Error('The issues endpoint returned only pull requests. Check that the token can read issues.');
  }
  const items = [
    ...issues.map(item => normalize(item, 'issue')),
    ...pulls.map(item => normalize(item, 'pull_request')),
  ].sort((left, right) => left.number - right.number);
  const counts = {
    issues: issues.length,
    pullRequests: pulls.length,
    openIssues: issues.filter(item => item.state === 'open').length,
    openPullRequests: pulls.filter(item => item.state === 'open').length,
    total: items.length,
  };
  counts.open = counts.openIssues + counts.openPullRequests;
  return { items, counts, reportedOpen: metadata.open_issues_count };
}

const args = parseArgs(process.argv.slice(2));
const warnings = [];
let result;
for (let attempt = 1; attempt <= attempts; attempt += 1) {
  result = await collect(args.repository);
  if (typeof result.reportedOpen !== 'number' || result.reportedOpen === result.counts.open) break;
  const message = `Collected ${result.counts.open} open items but the repository reports ${result.reportedOpen} (attempt ${attempt} of ${attempts}).`;
  console.error(message);
  if (attempt === attempts) {
    warnings.push(`${message} Counts are derived from the collected records; upstream activity during collection may be partially reflected.`);
  } else if (retryDelay > 0) {
    await new Promise(done => setTimeout(done, retryDelay));
  }
}

const snapshot = {
  schemaVersion: 2,
  collectedAt: new Date().toISOString(),
  repository: args.repository,
  upstreamCommit: args.upstream.toLowerCase(),
  coverage: 'Paginated REST issue and pull-request records. Discussions, review threads and CI runs are excluded.',
  counts: result.counts,
  warnings,
  items: result.items,
};
mkdirSync(dirname(resolve(args.output)), { recursive: true });
writeFileSync(args.output, `${JSON.stringify(snapshot, null, 2)}\n`, { flag: 'wx' });
console.log(JSON.stringify({ output: resolve(args.output), ...snapshot.counts, warnings: warnings.length }, null, 2));
