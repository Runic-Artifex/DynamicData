#!/usr/bin/env node
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '../..');
const prepare = resolve(root, 'eng/upstream/prepare-review.mjs');
const fixture = resolve(root, 'eng/upstream/test-fixtures/inventory.json');
const temporary = mkdtempSync(join(tmpdir(), 'upstream-review-'));
const isolated = { GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_NOSYSTEM: '1' };

function run(command, args, cwd, env = {}) {
  return execFileSync(command, args, { cwd, env: { ...process.env, ...isolated, ...env }, encoding: 'utf8', stdio: 'pipe' }).trim();
}
function git(args, repo, env) { return run('git', args, repo, env); }
function commit(repo, message, date = '2026-09-15T00:00:00Z') {
  git(['add', '.'], repo);
  git(['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', '-c', 'commit.gpgsign=false', 'commit', '-m', message], repo,
    { GIT_AUTHOR_DATE: date, GIT_COMMITTER_DATE: date });
  return git(['rev-parse', 'HEAD'], repo);
}
function merge(repo, branch, message) {
  git(['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', '-c', 'commit.gpgsign=false', 'merge', '--no-ff', branch, '-m', message], repo,
    { GIT_AUTHOR_DATE: '2026-09-20T00:00:00Z', GIT_COMMITTER_DATE: '2026-09-20T00:00:00Z' });
  return git(['rev-parse', 'HEAD'], repo);
}
function init(name) {
  const repo = join(temporary, name);
  run('git', ['init', '--initial-branch=main', repo], temporary);
  return repo;
}
function state(repo) {
  return [git(['rev-parse', 'HEAD'], repo), git(['for-each-ref'], repo), git(['status', '--porcelain', '--untracked-files=all'], repo)].join('\n---\n');
}
function invoke(repo, base, upstream, name, previous) {
  const output = join(temporary, `review-${name}`);
  const inventory = join(temporary, `${name}-inventory.json`);
  const data = JSON.parse(readFileSync(fixture, 'utf8'));
  data.upstreamCommit = upstream;
  writeFileSync(inventory, JSON.stringify(data));
  const args = [prepare, '--repo', repo, '--base', base, '--upstream', upstream, '--month', '2026-11', '--inventory', inventory, '--output', output,
    '--run-url', 'https://example.invalid/runs/1', '--artifact', 'upstream-review-2026-11-1'];
  if (previous !== undefined) {
    const previousFile = join(temporary, `${name}-previous.json`);
    writeFileSync(previousFile, JSON.stringify(previous));
    args.push('--previous', previousFile);
  }
  const before = state(repo);
  const result = JSON.parse(run('node', args, root));
  assert.equal(state(repo), before, 'The review must not change refs, HEAD or the working tree.');
  return {
    result,
    manifest: JSON.parse(readFileSync(join(output, 'manifest.json'), 'utf8')),
    summary: readFileSync(join(output, 'summary.md'), 'utf8'),
  };
}

try {
  // Fork with a real upstream merge, followed by further upstream work.
  const merged = init('merged');
  writeFileSync(join(merged, 'shared.txt'), 'initial\n');
  const initial = commit(merged, 'initial');
  git(['switch', '-c', 'upstream-main'], merged);
  writeFileSync(join(merged, 'upstream.txt'), 'one\n');
  const imported = commit(merged, 'upstream one', '2026-09-10T00:00:00Z');
  git(['switch', '-c', 'fork-main', initial], merged);
  writeFileSync(join(merged, 'fork.txt'), 'fork\n');
  commit(merged, 'fork change');
  git(['switch', '-c', 'integration'], merged);
  const importMerge = merge(merged, 'upstream-main', 'real upstream merge');
  git(['switch', 'fork-main'], merged);
  const base = merge(merged, 'integration', 'integrate review');
  git(['switch', 'upstream-main'], merged);
  writeFileSync(join(merged, 'upstream.txt'), 'two\n');
  const second = commit(merged, 'upstream two | piped', '2026-10-12T00:00:00Z');
  writeFileSync(join(merged, 'upstream.txt'), 'three\n');
  const candidate = commit(merged, 'upstream three', '2026-10-28T00:00:00Z');
  git(['switch', 'fork-main'], merged);

  // No previous review: compare with the baseline (the merged upstream parent).
  const first = invoke(merged, base, candidate, 'first', {});
  assert.deepEqual(first.manifest.baseline, { kind: 'upstream-merge', commit: imported, merge: importMerge, date: '2026-09-10T00:00:00Z' });
  assert.equal(first.manifest.since, '2026-09-10T00:00:00.000Z');
  assert.equal(first.manifest.previousReview, null);
  assert.equal(first.manifest.commitsSinceBaseline.count, 2);
  assert.deepEqual(first.manifest.commitsSinceBaseline.items.map(item => item.sha), [candidate, second]);
  assert.equal(first.manifest.commitsSincePrevious, null);
  assert.equal(first.manifest.merge.status, 'clean');
  assert.deepEqual(first.manifest.changes.items.map(item => [item.number, item.change]), [[5, 'new'], [2, 'updated'], [4, 'new']]);
  assert.match(first.summary, /^# Upstream review 2026-11\n/);
  assert.match(first.summary, /\| Previous review \| none; compared with the baseline \|/);
  assert.match(first.summary, /\| Issues \| 2 \| 3 \|/);
  assert.match(first.summary, /\| Pull requests \| 1 \| 2 \|/);
  assert.match(first.summary, /\| Upstream commits since baseline \| \| 2 \|/);
  assert.match(first.summary, /Old issue with \\\| pipe, updated/);
  assert.match(first.summary, /upstream two \\\| piped/);
  assert.match(first.summary, /\[#4\]\(https:\/\/redirect\.github\.com\/example-upstream\/Library\/issues\/4\)/);
  assert.match(first.summary, new RegExp(`\\(https://redirect\\.github\\.com/example-upstream/Library/commit/${candidate}\\)`));
  assert.doesNotMatch(first.summary, /https:\/\/github\.com\/example-upstream/, 'Upstream links must not create cross-references.');
  assert.match(first.summary, /\[run\]\(https:\/\/example\.invalid\/runs\/1\) \/ `upstream-review-2026-11-1`/);
  const data = JSON.parse(/<!-- upstream-review-data (\{.*\}) -->/.exec(first.summary)[1]);
  assert.deepEqual(data, { month: '2026-11', collectedAt: '2026-11-01T04:30:00.000Z', upstreamCommit: candidate, baselineCommit: imported });

  // Previous review issue: compare items since its collection and commits since its upstream pin.
  const previous = { number: 7, url: 'https://example.invalid/issues/7', title: 'Upstream review 2026-10', createdAt: '2026-10-01T05:00:00Z',
    data: { month: '2026-10', collectedAt: '2026-10-18T00:00:00Z', upstreamCommit: second } };
  const next = invoke(merged, base, candidate, 'next', previous);
  assert.equal(next.manifest.since, '2026-10-18T00:00:00.000Z');
  assert.deepEqual(next.manifest.previousReview, { number: 7, url: previous.url, title: previous.title, month: '2026-10' });
  assert.deepEqual(next.manifest.changes.items.map(item => [item.number, item.change]), [[5, 'new'], [2, 'updated']]);
  assert.deepEqual(next.manifest.commitsSincePrevious, { from: second, count: 1 });
  assert.equal(next.manifest.commitsSinceBaseline.count, 2);
  assert.match(next.summary, /\| Previous review \| \[#7\]\(https:\/\/example\.invalid\/issues\/7\) \|/);
  assert.match(next.summary, /\| Upstream commits since previous review \| \| 1 \|/);

  // A previous issue without the data marker falls back to its creation time.
  const unmarked = invoke(merged, base, candidate, 'unmarked', { number: 6, url: 'https://example.invalid/issues/6', title: 'Upstream review 2026-09', createdAt: '2026-10-19T00:00:00Z', data: null });
  assert.equal(unmarked.manifest.since, '2026-10-19T00:00:00.000Z');
  assert.equal(unmarked.manifest.commitsSincePrevious, null);
  assert.throws(() => invoke(merged, base, candidate, 'first', {}), /Refusing to overwrite/);

  // Linear fork that never merged upstream: the inherited merge base is the baseline.
  const linear = init('linear');
  writeFileSync(join(linear, 'shared.txt'), 'root\n');
  const shared = commit(linear, 'shared root', '2026-08-01T00:00:00Z');
  git(['switch', '-c', 'upstream-main'], linear);
  writeFileSync(join(linear, 'shared.txt'), 'upstream\n');
  const conflicting = commit(linear, 'upstream edit', '2026-10-02T00:00:00Z');
  git(['switch', '-c', 'fork-main', shared], linear);
  writeFileSync(join(linear, 'shared.txt'), 'fork\n');
  const linearBase = commit(linear, 'fork edit');
  const inherited = invoke(linear, linearBase, conflicting, 'linear');
  assert.deepEqual(inherited.manifest.baseline, { kind: 'merge-base', commit: shared, date: '2026-08-01T00:00:00Z' });
  assert.equal(inherited.manifest.commitsSinceBaseline.count, 1);
  assert.deepEqual(inherited.manifest.merge, { status: 'conflicts', conflicts: ['shared.txt'] });
  assert.match(inherited.summary, /conflicts in 1 path\(s\)/);
  assert.match(inherited.summary, /## Conflicting paths\n\n- `shared.txt`/);
  assert.match(inherited.summary, /\(inherited merge base\)/);

  // Unchanged upstream: nothing ahead of the baseline.
  const unchanged = invoke(linear, linearBase, shared, 'unchanged');
  assert.equal(unchanged.manifest.commitsSinceBaseline.count, 0);
  assert.match(unchanged.summary, /None\. The fork contains upstream main\./);
  console.log('prepare-review fixtures passed: merge and inherited baselines, previous-review comparison and fallbacks, conflicts, summary content, no repository changes');
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
