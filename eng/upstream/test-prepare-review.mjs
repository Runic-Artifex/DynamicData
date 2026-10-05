#!/usr/bin/env node
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync, cpSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '../..');
const prepare = resolve(root, 'eng/upstream/prepare-review.mjs');
const publish = resolve(root, 'eng/upstream/publish-review-branch.sh');
const fixture = resolve(root, 'eng/upstream/test-fixtures/inventory.json');
const temporary = mkdtempSync(join(tmpdir(), 'dynamicdata-upstream-review-'));

function run(command, args, cwd) {
  return execFileSync(command, args, { cwd, encoding: 'utf8', stdio: 'pipe' }).trim();
}
function git(args, cwd) { return run('git', args, cwd); }
function commit(repo, message) {
  git(['add', '.'], repo);
  git(['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', message], repo);
  return git(['rev-parse', 'HEAD'], repo);
}
function inventory(output, upstream) {
  const data = JSON.parse(readFileSync(fixture, 'utf8'));
  data.requestedUpstreamCommit = upstream;
  data.observedUpstreamMainCommit = upstream;
  writeFileSync(output, `${JSON.stringify(data, null, 2)}\n`);
}
function invoke(repo, base, upstream, output, dryRun = false) {
  const inventoryFile = join(temporary, `inventory-${Math.random()}.json`);
  inventory(inventoryFile, upstream);
  return run('node', [prepare, '--repo', repo, '--base', base, '--upstream', upstream, '--month', '2026-11', '--inventory', inventoryFile, '--output', output, ...(dryRun ? ['--dry-run'] : [])], root);
}

try {
  const repo = join(temporary, 'repo');
  run('git', ['init', '--initial-branch=main', repo]);
  writeFileSync(join(repo, 'shared.txt'), 'base\n');
  const base = commit(repo, 'base');

  const unchanged = join(temporary, 'unchanged');
  const unchangedOutput = JSON.parse(invoke(repo, base, base, unchanged));
  assert.equal(unchangedOutput.merge, 'clean');
  assert.equal(unchangedOutput.commits, 0);
  assert.match(readFileSync(join(unchanged, 'report.md'), 'utf8'), /no commits beyond it/);

  git(['branch', 'upstream', base], repo);
  git(['switch', 'upstream'], repo);
  writeFileSync(join(repo, 'upstream.txt'), 'candidate\n');
  const upstream = commit(repo, 'upstream change');
  git(['switch', 'main'], repo);
  writeFileSync(join(repo, 'runic.txt'), 'local\n');
  const divergentBase = commit(repo, 'runic change');
  const divergent = join(temporary, 'divergent');
  const divergentOutput = JSON.parse(invoke(repo, divergentBase, upstream, divergent));
  assert.equal(divergentOutput.merge, 'clean');
  assert.equal(divergentOutput.commits, 1);
  assert.equal(JSON.parse(readFileSync(join(divergent, 'manifest.json'))).merge.status, 'clean');

  git(['branch', 'conflict-upstream', divergentBase], repo);
  git(['switch', 'conflict-upstream'], repo);
  writeFileSync(join(repo, 'shared.txt'), 'upstream conflict\n');
  const conflictUpstream = commit(repo, 'upstream conflict');
  git(['switch', 'main'], repo);
  writeFileSync(join(repo, 'shared.txt'), 'runic conflict\n');
  const conflictBase = commit(repo, 'runic conflict');
  const conflicted = join(temporary, 'conflicted');
  const conflictOutput = JSON.parse(invoke(repo, conflictBase, conflictUpstream, conflicted));
  assert.equal(conflictOutput.merge, 'conflicts');
  assert.match(readFileSync(join(conflicted, 'report.md'), 'utf8'), /virtual three-way merge reports conflicts/);

  assert.throws(() => invoke(repo, conflictBase, conflictUpstream, conflicted), /Refusing to overwrite/);
  const dryRun = JSON.parse(invoke(repo, conflictBase, conflictUpstream, join(temporary, 'dry-run'), true));
  assert.equal(dryRun.merge, 'conflicts');

  const remote = join(temporary, 'origin.git');
  run('git', ['init', '--bare', remote]);
  git(['remote', 'add', 'origin', remote], repo);
  git(['push', '--set-upstream', 'origin', 'main'], repo);
  const reviewBranch = `review/upstream/2026-11-${conflictBase.slice(0, 12)}-${conflictUpstream.slice(0, 12)}`;
  const published = run('bash', [publish, '--repo', repo, '--base', conflictBase, '--branch', reviewBranch, '--snapshot', conflicted], root);
  assert.match(published, /status=created/);
  assert.equal(git(['ls-remote', '--heads', 'origin', `refs/heads/${reviewBranch}`], repo).split('\t')[1], `refs/heads/${reviewBranch}`);
  const repeated = run('bash', [publish, '--repo', repo, '--base', conflictBase, '--branch', reviewBranch, '--snapshot', conflicted], root);
  assert.match(repeated, /status=existing/);
  console.log('prepare-review fixtures passed: unchanged, divergent, conflicts, repeat protection, dry-run, and immutable review branch publication');
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
