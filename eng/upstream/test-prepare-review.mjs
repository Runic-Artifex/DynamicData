#!/usr/bin/env node
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '../..');
const prepare = resolve(root, 'eng/upstream/prepare-review.mjs');
const publish = resolve(root, 'eng/upstream/publish-review-branch.sh');
const openPullRequest = resolve(root, 'eng/upstream/open-review-pr.sh');
const fixture = resolve(root, 'eng/upstream/test-fixtures/inventory.json');
const temporary = mkdtempSync(join(tmpdir(), 'dynamicdata-upstream-review-'));

function run(command, args, cwd, env = {}) {
  return execFileSync(command, args, { cwd, env: { ...process.env, ...env }, encoding: 'utf8', stdio: 'pipe' }).trim();
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
  const provenanceRepo = join(temporary, 'provenance');
  run('git', ['init', '--initial-branch=main', provenanceRepo]);
  writeFileSync(join(provenanceRepo, 'initial.txt'), 'initial\n');
  const initial = commit(provenanceRepo, 'initial');
  git(['switch', '-c', 'upstream-main', initial], provenanceRepo);
  writeFileSync(join(provenanceRepo, 'upstream.txt'), 'one\n');
  const importedUpstream = commit(provenanceRepo, 'upstream one');
  git(['switch', '-c', 'runic-main', initial], provenanceRepo);
  writeFileSync(join(provenanceRepo, 'runic.txt'), 'base\n');
  commit(provenanceRepo, 'runic base');
  git(['switch', '-c', 'integration'], provenanceRepo);
  git(['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'merge', '--no-ff', 'upstream-main', '-m', 'real upstream merge'], provenanceRepo);
  const importedMerge = git(['rev-parse', 'HEAD'], provenanceRepo);
  writeFileSync(join(provenanceRepo, 'integration.txt'), 'review\n');
  commit(provenanceRepo, 'integration work');
  git(['switch', 'runic-main'], provenanceRepo);
  git(['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'merge', '--no-ff', 'integration', '-m', 'integrate review'], provenanceRepo);
  const nestedBase = git(['rev-parse', 'HEAD'], provenanceRepo);
  git(['switch', 'upstream-main'], provenanceRepo);
  writeFileSync(join(provenanceRepo, 'upstream.txt'), 'two\n');
  const nestedCandidate = commit(provenanceRepo, 'upstream two');
  git(['update-ref', 'refs/remotes/upstream/main', nestedCandidate], provenanceRepo);
  const nested = join(temporary, 'nested');
  invoke(provenanceRepo, nestedBase, nestedCandidate, nested);
  const nestedManifest = JSON.parse(readFileSync(join(nested, 'manifest.json')));
  assert.equal(nestedManifest.previousUpstreamPin.status, 'found');
  assert.equal(nestedManifest.previousUpstreamPin.merge, importedMerge);
  assert.equal(nestedManifest.previousUpstreamPin.upstreamParent, importedUpstream);

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
const fakeGit = join(temporary, 'fake-git.sh');
writeFileSync(fakeGit, `#!/usr/bin/env bash
if [[ " $* " == *" remote get-url --push --all origin "* ]]; then
  if [[ "\${FAKE_GIT_REAL_PUSH:-}" == '1' ]]; then exec git "$@"; fi
  printf '%s\\n' 'https://github.com/Runic-Artifex/DynamicData.git'
  exit 0
fi
if [[ " $* " == *" remote get-url origin "* ]]; then
  printf '%s\\n' 'https://github.com/Runic-Artifex/DynamicData.git'
  exit 0
fi
exec git "$@"
`);
  run('chmod', ['+x', fakeGit]);
  const isolatedGit = { GIT_BIN: fakeGit, GIT_CONFIG_GLOBAL: '/dev/null', GIT_CONFIG_NOSYSTEM: '1' };
  const publishArgs = [publish, '--repo', repo, '--base', conflictBase, '--upstream', conflictUpstream, '--branch', reviewBranch, '--snapshot', conflicted];
  const published = run('bash', publishArgs, root, isolatedGit);
  assert.deepEqual(published.split('\n'), ['status=created', `branch=${reviewBranch}`, `target=eng/upstream/reviews/${reviewBranch.slice('review/upstream/'.length)}`]);
  assert.equal(git(['ls-remote', '--heads', 'origin', `refs/heads/${reviewBranch}`], repo).split('\t')[1], `refs/heads/${reviewBranch}`);
  const repeated = run('bash', publishArgs, root, isolatedGit);
  assert.deepEqual(repeated.split('\n'), ['status=existing', `branch=${reviewBranch}`]);
  git(['config', 'remote.origin.pushurl', 'https://example.invalid/not-our-fork.git'], repo);
  assert.throws(() => run('bash', publishArgs, root, { ...isolatedGit, FAKE_GIT_REAL_PUSH: '1' }), /Origin push URL is not Runic-Artifex/);
  git(['config', '--unset-all', 'remote.origin.pushurl'], repo);
  const dirty = join(repo, 'untracked.txt');
  writeFileSync(dirty, 'must not be published\n');
  assert.throws(() => run('bash', publishArgs, root, isolatedGit), /dirty worktree/);
  unlinkSync(dirty);

const failingGit = join(temporary, 'failing-git.sh');
writeFileSync(failingGit, `#!/usr/bin/env bash
if [[ " $* " == *" remote get-url --push --all origin "* ]]; then
  printf '%s\\n' 'https://github.com/Runic-Artifex/DynamicData.git'
  exit 0
fi
if [[ " $* " == *" remote get-url origin "* ]]; then
  printf '%s\\n' 'https://github.com/Runic-Artifex/DynamicData.git'
  exit 0
fi
if [[ " $* " == *" ls-remote "* ]]; then
  exit 128
fi
exec git "$@"
`);
  run('chmod', ['+x', failingGit]);
  assert.throws(() => run('bash', publishArgs, root, { ...isolatedGit, GIT_BIN: failingGit }), /Unable to determine whether review branch exists/);

  const fakeGh = join(temporary, 'fake-gh.sh');
  writeFileSync(fakeGh, `#!/usr/bin/env bash
if [[ "$1 $2" == 'pr list' ]]; then
  [[ "\${GH_MODE:-}" == 'existing' ]] && printf '%s\\n' 'https://github.com/Runic-Artifex/DynamicData/pull/99'
  exit 0
fi
if [[ "$1 $2" == 'pr create' ]]; then
  if [[ "\${GH_MODE:-}" == 'deferred' ]]; then
    printf '%s\\n' 'Resource not accessible by integration' >&2
    exit 1
  fi
  printf '%s\\n' 'https://github.com/Runic-Artifex/DynamicData/pull/100'
  exit 0
fi
exit 2
`);
  run('chmod', ['+x', fakeGh]);
  const evidence = join(temporary, 'pr-evidence');
  const prArgs = [openPullRequest, '--repo', 'Runic-Artifex/DynamicData', '--branch', reviewBranch, '--base', conflictBase, '--upstream', conflictUpstream, '--month', '2026-11', '--evidence-dir', evidence];
  const deferred = run('bash', prArgs, root, { GH_BIN: fakeGh, GH_MODE: 'deferred' });
  assert.match(deferred, /^status=deferred\nmanual_url=https:\/\/github\.com\/Runic-Artifex\/DynamicData\/compare\/main/);
  assert.match(readFileSync(join(evidence, 'pr-status.md'), 'utf8'), /Open it manually/);
  const retried = run('bash', prArgs, root, { GH_BIN: fakeGh, GH_MODE: 'created' });
  assert.deepEqual(retried.split('\n'), ['status=created', 'url=https://github.com/Runic-Artifex/DynamicData/pull/100']);
  assert.equal(existsSync(join(evidence, 'pr-status.md')), false);
  const existing = run('bash', prArgs, root, { GH_BIN: fakeGh, GH_MODE: 'existing' });
  assert.deepEqual(existing.split('\n'), ['status=existing', 'url=https://github.com/Runic-Artifex/DynamicData/pull/99']);
  console.log('prepare-review fixtures passed: unchanged, divergent, conflicts, repeat protection, dry-run, isolated identity, immutable publication, remote failure, and deferred PR retry');
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
