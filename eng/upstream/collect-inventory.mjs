#!/usr/bin/env node
/**
 * Collect a new, deliberately unassessed upstream inventory.
 *
 * This script only calls GitHub's REST API and writes the requested JSON file.
 * It does not use the dated docs/upstream/assessments.json input: carrying an
 * old recommendation into a new monthly review would falsely imply that the
 * item had been assessed against the current base and candidate.
 */
import { execFile } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { promisify } from 'node:util';

const run = promisify(execFile);
const repo = 'reactivemarbles/DynamicData';
const gh = process.env.GH_BIN ?? 'gh';

function usage() {
  return 'Usage: node eng/upstream/collect-inventory.mjs --output <file> --upstream <40-hex-sha>';
}

function parseArgs(argv) {
  const values = {};
  for (let index = 0; index < argv.length; index += 2) {
    const key = argv[index];
    const value = argv[index + 1];
    if (!key?.startsWith('--') || value === undefined) throw new Error(usage());
    values[key.slice(2)] = value;
  }
  if (!values.output || !/^[0-9a-f]{40}$/i.test(values.upstream ?? '')) throw new Error(usage());
  return values;
}

async function api(endpoint, paginate = false) {
  const args = ['api', ...(paginate ? ['--paginate', '--slurp'] : []), `repos/${repo}${endpoint ? `/${endpoint}` : ''}`];
  const { stdout } = await run(gh, args, { maxBuffer: 40 * 1024 * 1024 });
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
    createdAt: item.created_at,
    updatedAt: item.updated_at,
    closedAt: item.closed_at ?? undefined,
    mergedAt: item.merged_at ?? undefined,
    labels: item.labels.map(label => label.name),
    headCommit: item.head?.sha,
    baseBranch: item.base?.ref,
    mergeCommit: item.merge_commit_sha,
    assessment: {
      status: 'unassessed-for-this-review',
      note: 'Review this item against the pinned Runic base and upstream candidate. Dated assessments are evidence only and are not reused as a current disposition.',
    },
  };
}

const args = parseArgs(process.argv.slice(2));
const [issueRecords, pulls, metadata, upstream] = await Promise.all([
  api('issues?state=all&per_page=100&sort=created&direction=asc', true),
  api('pulls?state=all&per_page=100&sort=created&direction=asc', true),
  api(''),
  api('commits/main'),
]);
const issues = issueRecords.filter(item => !item.pull_request);
const items = [
  ...issues.map(item => normalize(item, 'issue')),
  ...pulls.map(item => normalize(item, 'pull_request')),
].sort((left, right) => left.number - right.number);
const open = items.filter(item => item.state === 'open').length;
if (open !== metadata.open_issues_count) {
  throw new Error(`Open item count changed while collecting: ${open} versus ${metadata.open_issues_count}. Retry the collection.`);
}
const snapshot = {
  schemaVersion: 1,
  collectedAt: new Date().toISOString(),
  repository: repo,
  requestedUpstreamCommit: args.upstream.toLowerCase(),
  observedUpstreamMainCommit: upstream.sha,
  coverage: 'Paginated REST issue and pull-request records. Discussions, deleted or inaccessible objects, review threads and CI runs are excluded.',
  assessmentPolicy: 'Every item is unassessed for this monthly review. Historical docs/upstream research remains dated evidence and is never copied as a current decision.',
  counts: {
    issues: issues.length,
    pullRequests: pulls.length,
    open,
    total: items.length,
  },
  items,
};
mkdirSync(dirname(resolve(args.output)), { recursive: true });
writeFileSync(args.output, `${JSON.stringify(snapshot, null, 2)}\n`, { flag: 'wx' });
console.log(JSON.stringify({ output: resolve(args.output), ...snapshot.counts, observedUpstreamMainCommit: upstream.sha }, null, 2));
