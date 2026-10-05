#!/usr/bin/env node
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '../..');
const collector = resolve(root, 'eng/upstream/collect-inventory.mjs');
const temporary = mkdtempSync(join(tmpdir(), 'dynamicdata-upstream-collector-'));
const upstream = '0123456789abcdef0123456789abcdef01234567';

function run(args, environment = {}) {
  return execFileSync('node', [collector, ...args], {
    cwd: root,
    env: { ...process.env, ...environment },
    encoding: 'utf8',
    stdio: 'pipe',
  }).trim();
}

function item(number, type, state = 'open') {
  return {
    number,
    title: `${type} ${number}`,
    html_url: `https://example.invalid/${type}/${number}`,
    state,
    created_at: '2026-10-01T00:00:00Z',
    updated_at: '2026-10-01T00:00:00Z',
    closed_at: state === 'open' ? null : '2026-10-01T00:00:00Z',
    labels: [],
    ...(type === 'pull_request'
      ? { merged_at: null, draft: false, head: { sha: upstream }, base: { ref: 'main' }, merge_commit_sha: null }
      : {}),
  };
}

function fakeGh(scenario) {
  const executable = join(temporary, `gh-${scenario}.mjs`);
  writeFileSync(executable, `#!/usr/bin/env node
const endpoint = process.argv.find(value => value.startsWith('repos/reactivemarbles/DynamicData'));
const upstream = '${upstream}';
const issues = Array.from({ length: 35 }, (_, index) => ({ number: index + 1, title: 'issue', html_url: 'https://example.invalid/issues/' + (index + 1), state: 'open', created_at: '2026-10-01T00:00:00Z', updated_at: '2026-10-01T00:00:00Z', closed_at: null, labels: [] }));
const pulls = Array.from({ length: 23 }, (_, index) => ({ number: index + 101, title: 'pull', html_url: 'https://example.invalid/pulls/' + (index + 101), state: 'open', created_at: '2026-10-01T00:00:00Z', updated_at: '2026-10-01T00:00:00Z', closed_at: null, labels: [], merged_at: null, draft: false, head: { sha: upstream }, base: { ref: 'main' }, merge_commit_sha: null }));
const issueEndpointPulls = pulls.map(pull => ({ ...pull, pull_request: {} }));
if (endpoint.includes('/issues?')) console.log(JSON.stringify([${scenario === 'missing-issues' ? 'issueEndpointPulls' : 'issues'}]));
else if (endpoint.includes('/pulls?')) console.log(JSON.stringify([pulls]));
else if (endpoint.endsWith('/commits/main')) console.log(JSON.stringify({ sha: upstream }));
else if (endpoint === 'repos/reactivemarbles/DynamicData') console.log(JSON.stringify({ open_issues_count: 58 }));
else process.exit(2);
`, { mode: 0o755 });
  return executable;
}

try {
  const completeOutput = join(temporary, 'complete.json');
  const completeGh = fakeGh('complete');
  const complete = JSON.parse(run(['--output', completeOutput, '--upstream', upstream], { PATH: `${temporary}:${process.env.PATH}`, GH_BIN: completeGh }));
  assert.equal(complete.open, 58);
  const snapshot = JSON.parse(readFileSync(completeOutput, 'utf8'));
  assert.equal(snapshot.items.length, 58);
  assert.ok(snapshot.items.every(item => item.assessment.status === 'unassessed-for-this-review'));

  const incompleteOutput = join(temporary, 'incomplete.json');
  const missingIssuesGh = fakeGh('missing-issues');
  let failure;
  try {
    run(['--output', incompleteOutput, '--upstream', upstream], { PATH: `${temporary}:${process.env.PATH}`, GH_BIN: missingIssuesGh });
  } catch (error) {
    failure = error;
  }
  assert.ok(failure, 'A pull-request-only issues response must be rejected.');
  assert.match(String(failure.stderr), /23 versus 58/);
  assert.equal(existsSync(incompleteOutput), false);
  console.log('collect-inventory fixtures passed: complete inventory and pull-request-only issue response rejection');
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
