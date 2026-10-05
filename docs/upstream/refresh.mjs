#!/usr/bin/env node
// Read-only GitHub collection; writes only the generated research inventory.
import { execFileSync, execFile } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve, dirname } from 'node:path';
import { promisify } from 'node:util';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '../..');
const repo = 'reactivemarbles/DynamicData';
const run = promisify(execFile);
const assessments = JSON.parse(readFileSync(resolve(here, 'assessments.json')));
async function api(endpoint, paginate = false) {
  const args = ['api', ...(paginate ? ['--paginate', '--slurp'] : []), `repos/${repo}${endpoint ? '/' + endpoint : ''}`];
  const { stdout } = await run('gh', args, { maxBuffer: 40 * 1024 * 1024 });
  const result = JSON.parse(stdout);
  return paginate ? result.flat() : result;
}
const [issueRecords, pulls, metadata, upstream] = await Promise.all([
      api('issues?state=all&per_page=100&sort=created&direction=asc', true),
      api('pulls?state=all&per_page=100&sort=created&direction=asc', true),
      api(''), api('commits/main'),
    ]);
const forkCommit = execFileSync('git', ['-C', root, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim();
const history = new Set(execFileSync('git', ['-C', root, 'rev-list', 'HEAD'], { encoding: 'utf8', maxBuffer: 15e6 }).trim().split('\n'));
const issues = issueRecords.filter(x => !x.pull_request);
const issueApiNumbers = new Set(issueRecords.map(x => x.number));
const counts = { issues: { open: 0, closed: 0 }, pullRequests: { open: 0, closed: 0, merged: 0 } };

function category(title, labels) {
  const value = `${title} ${labels.join(' ')}`;
  if (/dependency|renovate|bump |update .*action|deps\)|source.?link|test.sdk/i.test(value)) return 'dependencies/build';
  if (/deadlock|concurren|thread|race|lock|subscription|oncompleted|onerror|terminal/i.test(value)) return 'concurrency/lifecycle';
  if (/sort|virtual|pag(e|ing)|index|mov(e|ing)|filter|removekey/i.test(value)) return 'ordering/viewport/filtering';
  if (/sum|average|\bavg\b|stddev|maximum|minimum|aggregation/i.test(value)) return 'aggregation';
  if (/transform|merge|group|join|refresh|changeset|sourcecache|sourcelist/i.test(value)) return 'collection operators';
  if (/aot|trim|reflection/i.test(value)) return 'AOT/trimming';
  if (/test|coverage|assert|benchmark/i.test(value)) return 'verification/performance';
  if (/readme|doc|website|release|housekeeping|using|profile/i.test(value)) return 'documentation/maintenance';
  return 'other/API';
}
function historicalSummary(x, type) {
  // A conservative scope synopsis: no inferred resolution from a closed state.
  const scope = x.title.replace(/^\[[^\]]+\]:?\s*/, '').replace(/^(fix|feat|chore|test|docs|perf|refactor)(\([^)]*\))?:\s*/i, '').trim();
  if (type === 'issue') {
    return `${/feature|enhancement/i.test(x.title) ? 'Requests' : /bug/i.test(x.title) ? 'Reports' : 'Tracks'} ${scope.replace(/[.!]$/, '')}.`;
  }
  if (/^update |^bump /i.test(scope)) return `${scope.replace(/[.!]$/, '')}.`;
  if (/^fix(ed|es)?\s+/i.test(scope)) return `Corrects ${scope.replace(/^fix(ed|es)?\s+/i, '').replace(/[.!]$/, '')}.`;
  if (/^add(ed|s)?\s+/i.test(scope)) return `Introduces ${scope.replace(/^add(ed|s)?\s+/i, '').replace(/[.!]$/, '')}.`;
  return `Proposes work on ${scope.replace(/[.!]$/, '')}.`;
}
function normalize(x, type) {
  const state = x.merged_at ? 'merged' : x.state;
  const labels = x.labels.map(l => l.name);
  const assessment = assessments[x.number];
  const present = type === 'pull_request' && x.merged_at ? history.has(x.merge_commit_sha) : null;
  return {
    number: x.number, type, title: x.title, url: x.html_url, state,
    draft: type === 'pull_request' ? x.draft : undefined,
    createdAt: x.created_at, updatedAt: x.updated_at, closedAt: x.closed_at,
    mergedAt: x.merged_at ?? undefined, labels, category: category(x.title, labels),
    headCommit: x.head?.sha, baseBranch: x.base?.ref, mergeCommit: x.merge_commit_sha,
    mergeCommitInForkAncestry: present,
    summary: assessment?.[0] ?? historicalSummary(x, type),
    summaryBasis: assessment ? 'Reviewed body/discussion and relevant source or PR diff' : 'Historical title-based scope synopsis; not an implementation audit',
    usefulness: assessment?.[1] ?? (present ? 'Already in fork ancestry' : state === 'open' ? 'Needs refreshed assessment' : 'Historical reference; inspect if this scenario is needed'),
    forkStatus: assessment?.[2] ?? (present ? 'Exact upstream merge commit is an ancestor' : type === 'pull_request' && state === 'merged' ? 'Merge commit absent from ancestry; equivalent code not ruled out' : 'Not individually compared with fork code'),
    implementation: assessment?.[3] ?? null,
  };
}
const items = [...issues.map(x => normalize(x, 'issue')), ...pulls.map(x => normalize(x, 'pull_request'))].sort((a, b) => a.number - b.number);
const ids = new Set();
for (const x of items) {
  if (ids.has(x.number)) throw new Error(`Duplicate GitHub number ${x.number}`);
  ids.add(x.number);
  counts[x.type === 'issue' ? 'issues' : 'pullRequests'][x.state]++;
}
const openCount = counts.issues.open + counts.pullRequests.open;
if (openCount !== metadata.open_issues_count) throw new Error(`Open count changed while collecting: ${openCount} vs repository ${metadata.open_issues_count}. Retry.`);
const snapshot = {
  schemaVersion: 1, collectedAt: new Date().toISOString(), repository: repo,
  forkRepository: 'Runic-Artifex/DynamicData', forkCommit, upstreamMainCommit: upstream.sha,
  counts, total: items.length,
  coverage: 'All issue and pull-request objects returned by paginated GitHub REST state=all. Discussions, deleted/inaccessible objects, review threads and CI runs are not an exhaustive archive.',
  reconciliation: { pullRequestsAbsentFromIssuesEndpoint: pulls.filter(x => !issueApiNumbers.has(x.number)).map(x => x.number) },
  items,
};
writeFileSync(resolve(here, 'inventory.json'), JSON.stringify(snapshot, null, 2) + '\n');
const esc = text => String(text ?? '').replace(/\|/g, '\\|').replace(/[\r\n]+/g, ' ').replace(/\[/g, '\\[').replace(/\]/g, '\\]');
const link = x => `[#${x.number} ${esc(x.title)}](${x.url})`;
for (const [type, file, label] of [['issue', 'issues.md', 'issues'], ['pull_request', 'pull-requests.md', 'pull requests']]) {
  const rows = items.filter(x => x.type === type);
  let md = `# Upstream ${label}\n\nSnapshot: ${snapshot.collectedAt}. Repository: [${repo}](https://github.com/${repo}). ${rows.length} records.\n\n[Analysis](analysis.md) · [Index](README.md) · [Machine-readable inventory](inventory.json)\n\nOpen items have authored summaries and fork assessments. Historical summaries conservatively describe title-level scope; closed does not mean fixed, and absent ancestry does not mean absent code.\n\n`;
  for (const [name, subset] of [['Open', rows.filter(x => x.state === 'open')], ['Historical', rows.filter(x => x.state !== 'open')]]) {
    md += `## ${name} (${subset.length})\n\n| Item | State | Summary | Usefulness / fork evidence |\n| --- | --- | --- | --- |\n`;
    for (const x of subset.slice().reverse()) md += `| ${link(x)} | ${x.state}${x.draft ? ' (draft)' : ''} | ${esc(x.summary)} | ${esc(x.usefulness)}; ${esc(x.forkStatus)} |\n`;
    md += '\n';
  }
  writeFileSync(resolve(here, file), md);
}
let backlog = `# Assessment of every open upstream item\n\nSnapshot: ${snapshot.collectedAt}. [Main analysis](analysis.md). These are research recommendations, not canonical planning status or implementation claims.\n\n`;
for (const type of ['issue', 'pull_request']) {
  backlog += `## ${type === 'issue' ? 'Issues' : 'Pull requests'}\n\n| Item | Summary | Usefulness | Fork status | Implementation / completion approach |\n| --- | --- | --- | --- | --- |\n`;
  for (const x of items.filter(x => x.type === type && x.state === 'open').reverse()) backlog += `| ${link(x)} | ${esc(x.summary)} | ${esc(x.usefulness)} | ${esc(x.forkStatus)} | ${esc(x.implementation ?? 'New item: review its body, discussion and diff before deciding.')} |\n`;
  backlog += '\n';
}
writeFileSync(resolve(here, 'open-assessment.md'), backlog);
console.log(JSON.stringify({ collectedAt: snapshot.collectedAt, counts, total: snapshot.total, authoredAssessments: items.filter(x => assessments[x.number]).length, unassessedOpen: items.filter(x => x.state === 'open' && !assessments[x.number]).map(x => x.number), reconciliation: snapshot.reconciliation }, null, 2));
