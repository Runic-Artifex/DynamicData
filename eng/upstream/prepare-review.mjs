#!/usr/bin/env node
/**
 * Produce an immutable review snapshot from already-fetched Git objects.
 *
 * It never switches branches, merges a working tree, fetches, pushes, or runs
 * code from upstream. git merge-tree is used only to calculate whether a real
 * merge would need human conflict resolution.
 */
import { execFileSync } from 'node:child_process';
import { cpSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { relative, resolve } from 'node:path';

function usage() {
  return 'Usage: node eng/upstream/prepare-review.mjs --repo <repo> --base <40-hex-sha> --upstream <40-hex-sha> --month YYYY-MM --inventory <file> --output <new-directory> [--dry-run]';
}

function parseArgs(argv) {
  const values = { dryRun: false };
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === '--dry-run') {
      values.dryRun = true;
      continue;
    }
    if (!argument.startsWith('--') || argv[index + 1] === undefined) throw new Error(usage());
    values[argument.slice(2)] = argv[index + 1];
    index += 1;
  }
  for (const key of ['repo', 'base', 'upstream', 'month', 'inventory', 'output']) {
    if (!values[key]) throw new Error(usage());
  }
  if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(values.month) ||
      !/^[0-9a-f]{40}$/i.test(values.base) || !/^[0-9a-f]{40}$/i.test(values.upstream)) {
    throw new Error(usage());
  }
  return values;
}

function git(repo, args, options = {}) {
  return execFileSync('git', ['-C', repo, ...args], {
    encoding: 'utf8',
    maxBuffer: 20 * 1024 * 1024,
    stdio: options.allowFailure ? ['ignore', 'pipe', 'pipe'] : undefined,
  }).trim();
}

function gitResult(repo, args) {
  try {
    return { ok: true, status: 0, output: git(repo, args) };
  } catch (error) {
    return {
      ok: false,
      status: Number.isInteger(error.status) ? error.status : null,
      output: String(error.stdout ?? error.stderr ?? '').trim(),
    };
  }
}

function assertCommit(repo, sha, label) {
  if (git(repo, ['rev-parse', `${sha}^{commit}`]).toLowerCase() !== sha.toLowerCase()) {
    throw new Error(`${label} is not an exact commit object: ${sha}`);
  }
}

function previousUpstreamPin(repo, base) {
  const upstreamMain = gitResult(repo, ['rev-parse', '--verify', 'refs/remotes/upstream/main^{commit}']);
  if (!upstreamMain.ok) return { status: 'not-found', reason: 'No fetched refs/remotes/upstream/main ref.' };
  const merges = git(repo, ['rev-list', '--merges', '--parents', base]).split('\n').filter(Boolean);
  const candidates = [];
  for (const line of merges) {
    const [merge, firstParent, ...additionalParents] = line.split(' ');
    if (gitResult(repo, ['merge-base', '--is-ancestor', firstParent, upstreamMain.output]).ok) continue;
    const upstreamParents = additionalParents.filter(parent =>
      gitResult(repo, ['merge-base', '--is-ancestor', parent, upstreamMain.output]).ok);
    if (upstreamParents.length === 1) candidates.push({ merge, runicParent: firstParent, upstreamParent: upstreamParents[0] });
    if (upstreamParents.length > 1) candidates.push({ merge, runicParent: firstParent, upstreamParents, ambiguous: true });
  }
  if (candidates.length === 0) return { status: 'not-found', reason: 'No imported upstream merge was found in reachable history.' };
  if (candidates.some(candidate => candidate.ambiguous)) return { status: 'ambiguous', candidates };
  const maximal = candidates.filter(candidate => !candidates.some(other =>
    other !== candidate && gitResult(repo, ['merge-base', '--is-ancestor', candidate.upstreamParent, other.upstreamParent]).ok));
  return maximal.length === 1 ? { status: 'found', ...maximal[0] } : { status: 'ambiguous', candidates: maximal };
}

function markdown(manifest) {
  const conflicts = manifest.merge.status === 'conflicts';
  const noChange = manifest.commits.count === 0;
  return `# ${manifest.month} upstream review preparation

This is a preparation record, not an implementation review or a merge approval.
It was generated from immutable Git object IDs and a fresh unassessed GitHub
inventory. Historical research under \`docs/upstream/\` remains dated evidence.

## Pins

| Input | SHA |
| --- | --- |
| Runic base | \`${manifest.base}\` |
| Upstream candidate | \`${manifest.upstream}\` |
| Previous recorded upstream merge | ${manifest.previousUpstreamPin.status === 'found' ? `\`${manifest.previousUpstreamPin.upstreamParent}\` via \`${manifest.previousUpstreamPin.merge}\`` : `${manifest.previousUpstreamPin.status}: ${manifest.previousUpstreamPin.reason ?? 'inspect candidates in manifest.json'}`} |

## Result

${noChange ? 'The candidate is already the base or has no commits beyond it. Record the no-change review after checking the fresh inventory.' : `${manifest.commits.count} upstream commits are ahead of the merge base.`}

${conflicts
    ? 'A virtual three-way merge reports conflicts. No merge was attempted. Resolve them only on a temporary sync branch after reviewing the affected fork differences.'
    : 'A virtual three-way merge produced a tree without textual conflicts. This is not approval to merge: inspect cleanly merged workflow, dependency, test and source changes.'}

## Fresh inventory

The inventory contains ${manifest.inventory.counts.total} records (${manifest.inventory.counts.open} open). Every item is marked \`unassessed-for-this-review\`; no October assessment was copied forward. Review relevant issues and PRs against these pins before selecting a sync scope.

## Required human follow-up

1. Inspect the commit list and changed paths in \`manifest.json\`.
2. Decide whether this candidate is suitable, then create \`sync/upstream/${manifest.month}\` from the recorded base.
3. Perform a real, reviewed merge only on that temporary branch. Preserve the upstream merge parent and record conflict decisions, validations, adopted and deferred work in \`docs/upstream/reviews/${manifest.month}.md\`.
4. Do not merge this preparation branch into Runic \`main\`, publish packages, tags, or send anything upstream.
`;
}

const args = parseArgs(process.argv.slice(2));
const repo = resolve(args.repo);
const output = resolve(args.output);
const inventoryPath = resolve(args.inventory);
if (!existsSync(repo) || !existsSync(inventoryPath)) throw new Error('Repository or inventory file does not exist.');
if (existsSync(output)) throw new Error(`Refusing to overwrite existing review snapshot: ${output}`);
assertCommit(repo, args.base, 'Base');
assertCommit(repo, args.upstream, 'Upstream candidate');
const inventory = JSON.parse(readFileSync(inventoryPath, 'utf8'));
if (inventory.requestedUpstreamCommit?.toLowerCase() !== args.upstream.toLowerCase()) {
  throw new Error('Inventory candidate pin does not match --upstream. Collect a new inventory for this exact candidate.');
}
const mergeBase = git(repo, ['merge-base', args.base, args.upstream]);
const mergeTree = gitResult(repo, ['merge-tree', '--write-tree', args.base, args.upstream]);
if (!mergeTree.ok && mergeTree.status !== 1) {
  throw new Error(`git merge-tree failed with exit ${mergeTree.status ?? 'unknown'}: ${mergeTree.output}`);
}
const commits = git(repo, ['log', '--format=%H%x09%s', `${mergeBase}..${args.upstream}`])
  .split('\n').filter(Boolean).map(line => {
    const [sha, subject] = line.split('\t');
    return { sha, subject };
  });
const changedPaths = git(repo, ['diff', '--name-only', `${mergeBase}..${args.upstream}`]).split('\n').filter(Boolean);
const manifest = {
  schemaVersion: 1,
  generatedAt: new Date().toISOString(),
  month: args.month,
  base: args.base.toLowerCase(),
  upstream: args.upstream.toLowerCase(),
  mergeBase,
  previousUpstreamPin: previousUpstreamPin(repo, args.base),
  merge: {
    status: mergeTree.ok ? 'clean' : 'conflicts',
    virtualTree: mergeTree.ok ? mergeTree.output : null,
    diagnostic: mergeTree.ok ? null : mergeTree.output,
    method: 'git merge-tree --write-tree; no checkout or working-tree merge was performed',
  },
  commits: { count: commits.length, items: commits },
  changedPaths,
  inventory: {
    file: 'inventory.json',
    collectedAt: inventory.collectedAt,
    observedUpstreamMainCommit: inventory.observedUpstreamMainCommit,
    counts: inventory.counts,
    assessmentPolicy: inventory.assessmentPolicy,
  },
};
if (!args.dryRun) {
  mkdirSync(output, { recursive: false });
  cpSync(inventoryPath, resolve(output, 'inventory.json'));
  writeFileSync(resolve(output, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`, { flag: 'wx' });
  writeFileSync(resolve(output, 'report.md'), markdown(manifest), { flag: 'wx' });
}
console.log(JSON.stringify({ output: args.dryRun ? relative(process.cwd(), output) : output, base: manifest.base, upstream: manifest.upstream, merge: manifest.merge.status, commits: manifest.commits.count, inventory: manifest.inventory.counts }, null, 2));
