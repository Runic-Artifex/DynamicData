#!/usr/bin/env node
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '../..');
const script = resolve(root, 'eng/upstream/review-issue.mjs');
const temporary = mkdtempSync(join(tmpdir(), 'upstream-review-issue-'));
const repo = 'example-fork/Library';
const issuesFile = join(temporary, 'issues.json');
const logFile = join(temporary, 'gh.log');

// Fake gh backed by a JSON issue list; it logs every invocation and rejects
// anything other than issue list/create/edit.
const fakeGh = join(temporary, 'gh.cjs');
writeFileSync(fakeGh, `#!/usr/bin/env node
const { appendFileSync, readFileSync, writeFileSync } = require('node:fs');
const args = process.argv.slice(2);
appendFileSync(process.env.FAKE_GH_LOG, JSON.stringify(args) + '\\n');
const option = name => args[args.indexOf(name) + 1];
const issues = JSON.parse(readFileSync(process.env.FAKE_GH_ISSUES, 'utf8'));
const save = () => writeFileSync(process.env.FAKE_GH_ISSUES, JSON.stringify(issues));
if (option('--repo') !== '${repo}') process.exit(4);
if (args[0] === 'issue' && args[1] === 'list') {
  console.log(JSON.stringify(issues.filter(issue => issue.labels.includes(option('--label')))));
} else if (args[0] === 'issue' && args[1] === 'create') {
  const number = Math.max(0, ...issues.map(issue => issue.number)) + 1;
  const url = 'https://github.com/${repo}/issues/' + number;
  issues.push({ number, url, title: option('--title'), state: 'OPEN', createdAt: '2026-11-01T05:00:00Z', labels: [option('--label')], body: readFileSync(option('--body-file'), 'utf8') });
  save();
  console.log(url);
} else if (args[0] === 'issue' && args[1] === 'edit') {
  const issue = issues.find(candidate => candidate.number === Number(args[2]));
  issue.body = readFileSync(option('--body-file'), 'utf8');
  save();
} else {
  process.exit(3);
}
`, { mode: 0o755 });

function invoke(args) {
  return execFileSync('node', [script, ...args], {
    cwd: root,
    env: { ...process.env, GH_BIN: fakeGh, FAKE_GH_ISSUES: issuesFile, FAKE_GH_LOG: logFile },
    encoding: 'utf8',
    stdio: 'pipe',
  }).trim();
}
function previous(month) {
  const output = join(temporary, `previous-${month}.json`);
  invoke(['previous', '--repo', repo, '--month', month, '--output', output]);
  return JSON.parse(readFileSync(output, 'utf8'));
}
const marker = data => `Summary\n\n<!-- upstream-review-data ${JSON.stringify(data)} -->\n`;
const issue = (number, title, state, createdAt, body, labels = ['upstream-review']) => ({ number, url: `https://github.com/${repo}/issues/${number}`, title, state, createdAt, body, labels });

try {
  // No review issue yet: the summary falls back to the recorded baseline.
  writeFileSync(issuesFile, JSON.stringify([issue(1, 'Unrelated', 'OPEN', '2026-10-20T00:00:00Z', '', ['bug'])]));
  assert.deepEqual(previous('2026-11'), {});

  // The most recent labelled issue wins, closed or open; this month's issue is excluded.
  writeFileSync(issuesFile, JSON.stringify([
    issue(2, 'Upstream review 2026-09', 'CLOSED', '2026-09-01T05:00:00Z', marker({ month: '2026-09', collectedAt: '2026-09-01T04:30:00Z', upstreamCommit: 'a'.repeat(40) })),
    issue(5, 'Upstream review 2026-10', 'CLOSED', '2026-10-01T05:00:00Z', marker({ month: '2026-10', collectedAt: '2026-10-01T04:30:00Z', upstreamCommit: 'b'.repeat(40) })),
    issue(9, 'Upstream review 2026-11', 'OPEN', '2026-11-01T05:00:00Z', marker({ month: '2026-11', collectedAt: '2026-11-01T04:30:00Z', upstreamCommit: 'c'.repeat(40) })),
    issue(10, 'Unrelated', 'OPEN', '2026-11-02T00:00:00Z', '', ['bug']),
  ]));
  assert.deepEqual(previous('2026-11'), {
    number: 5, url: `https://github.com/${repo}/issues/5`, title: 'Upstream review 2026-10', state: 'CLOSED', createdAt: '2026-10-01T05:00:00Z',
    data: { month: '2026-10', collectedAt: '2026-10-01T04:30:00Z', upstreamCommit: 'b'.repeat(40) },
  });
  assert.equal(previous('2026-12').number, 9);

  // An issue without a parsable marker is still detected; data is null.
  writeFileSync(issuesFile, JSON.stringify([issue(3, 'Upstream review 2026-10', 'OPEN', '2026-10-01T05:00:00Z', 'Edited by hand')]));
  assert.equal(previous('2026-11').data, null);

  // Publish creates this month's issue once, then updates it in place.
  const body = join(temporary, 'summary.md');
  writeFileSync(body, marker({ month: '2026-11', collectedAt: '2026-11-01T04:30:00Z' }));
  assert.deepEqual(invoke(['publish', '--repo', repo, '--month', '2026-11', '--body-file', body]).split('\n'),
    ['status=created', 'number=4', `url=https://github.com/${repo}/issues/4`]);
  writeFileSync(body, `${'x'.repeat(70000)}\n${marker({ month: '2026-11', collectedAt: '2026-11-02T00:00:00Z' })}`);
  assert.deepEqual(invoke(['publish', '--repo', repo, '--month', '2026-11', '--body-file', body]).split('\n'),
    ['status=updated', 'number=4', `url=https://github.com/${repo}/issues/4`]);
  const stored = JSON.parse(readFileSync(issuesFile, 'utf8'));
  assert.equal(stored.length, 2);
  const current = stored.find(candidate => candidate.number === 4);
  assert.equal(current.title, 'Upstream review 2026-11');
  assert.deepEqual(current.labels, ['upstream-review']);
  assert.ok(current.body.length <= 65000, 'Issue bodies are truncated below the GitHub limit.');
  assert.match(current.body, /Truncated; see the workflow artifact/);
  assert.match(current.body, /"collectedAt":"2026-11-02T00:00:00Z"/);

  // Only issue commands were used.
  const calls = readFileSync(logFile, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.ok(calls.every(args => args[0] === 'issue' && ['list', 'create', 'edit'].includes(args[1])));

  // No-commit design: read-only workflow contents, no publication scripts.
  const workflow = readFileSync(resolve(root, '.github/workflows/upstream-review.yml'), 'utf8');
  assert.match(workflow, /permissions:\n {2}contents: read\n {2}issues: write\n/);
  assert.doesNotMatch(workflow, /contents: write|pull-requests:|git push|git commit|gh pr /);
  for (const name of ['publish-review-branch.sh', 'open-review-pr.sh']) assert.equal(existsSync(resolve(root, 'eng/upstream', name)), false);
  for (const name of readdirSync(resolve(root, 'eng/upstream')).filter(file => /\.(mjs|sh)$/.test(file) && !file.startsWith('test-'))) {
    const source = readFileSync(resolve(root, 'eng/upstream', name), 'utf8');
    assert.doesNotMatch(source, /['"](push|commit|switch|checkout|merge|pr)['"]/, `${name} must not change repositories`);
  }
  console.log('review-issue fixtures passed: previous-review detection, create then update, body limit, issue-only gh usage, read-only workflow');
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
