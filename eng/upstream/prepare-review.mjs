#!/usr/bin/env node
/**
 * Summarize one monthly upstream review from already-fetched Git objects and a
 * collected inventory.
 *
 * Writes manifest.json and summary.md into the output directory. It never
 * switches branches, merges a working tree, commits, fetches, pushes or runs
 * upstream code; git merge-tree only reports whether a merge would conflict.
 */
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

export const marker = 'upstream-review-data';
const listLimit = 50;
const usage = 'Usage: node eng/upstream/prepare-review.mjs --repo <repo> --base <sha> --upstream <sha> --month YYYY-MM --inventory <file> --output <directory> [--previous <file>] [--run-url <url>] [--artifact <name>]';

function parseArgs(argv) {
  const values = {};
  for (let index = 0; index < argv.length; index += 2) {
    if (!argv[index]?.startsWith('--') || argv[index + 1] === undefined) throw new Error(usage);
    values[argv[index].slice(2)] = argv[index + 1];
  }
  if (!values.repo || !values.inventory || !values.output || !/^\d{4}-(0[1-9]|1[0-2])$/.test(values.month ?? '') ||
      !/^[0-9a-f]{40}$/i.test(values.base ?? '') || !/^[0-9a-f]{40}$/i.test(values.upstream ?? '')) {
    throw new Error(usage);
  }
  return values;
}

function git(repo, args) {
  return execFileSync('git', ['-C', repo, ...args], { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'] }).trim();
}

function gitResult(repo, args) {
  try {
    return { ok: true, status: 0, output: git(repo, args) };
  } catch (error) {
    return { ok: false, status: Number.isInteger(error.status) ? error.status : null, output: String(error.stdout ?? error.stderr ?? '').trim() };
  }
}

function assertCommit(repo, sha, label) {
  const resolved = gitResult(repo, ['rev-parse', '--verify', `${sha}^{commit}`]);
  if (!resolved.ok || resolved.output.toLowerCase() !== sha.toLowerCase()) throw new Error(`${label} is not a commit in ${repo}: ${sha}`);
}

/**
 * The fork's recorded upstream baseline: the upstream parent of the newest real
 * upstream merge in the fork's history, or, for a fork that never merged
 * upstream, the shared merge base it inherited.
 */
export function upstreamBaseline(repo, base, upstream) {
  const merges = git(repo, ['rev-list', '--merges', '--parents', base]).split('\n').filter(Boolean);
  const candidates = [];
  for (const line of merges) {
    const [merge, firstParent, ...others] = line.split(' ');
    if (gitResult(repo, ['merge-base', '--is-ancestor', firstParent, upstream]).ok) continue;
    const imported = others.filter(parent => gitResult(repo, ['merge-base', '--is-ancestor', parent, upstream]).ok);
    if (imported.length === 1) candidates.push({ merge, upstreamParent: imported[0] });
  }
  const newest = candidates.filter(candidate => !candidates.some(other =>
    other !== candidate && other.upstreamParent !== candidate.upstreamParent &&
    gitResult(repo, ['merge-base', '--is-ancestor', candidate.upstreamParent, other.upstreamParent]).ok));
  if (newest.length >= 1) {
    return { kind: 'upstream-merge', commit: newest[0].upstreamParent, merge: newest[0].merge };
  }
  return { kind: 'merge-base', commit: git(repo, ['merge-base', base, upstream]) };
}

function commitsBetween(repo, from, to) {
  return git(repo, ['log', '--format=%H%x09%cI%x09%s', `${from}..${to}`]).split('\n').filter(Boolean).map(line => {
    const [sha, date, ...subject] = line.split('\t');
    return { sha, date, subject: subject.join('\t') };
  });
}

function readJson(file, fallback) {
  if (!file) return fallback;
  const text = readFileSync(file, 'utf8').trim();
  return text ? JSON.parse(text) : fallback;
}

/** Items created or updated after `since`, newest first. */
export function changedItems(items, since) {
  const threshold = Date.parse(since);
  return items
    .filter(item => Date.parse(item.updatedAt) > threshold)
    .map(item => ({ ...item, change: Date.parse(item.createdAt) > threshold ? 'new' : 'updated' }))
    .sort((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt) || right.number - left.number);
}

function escapeCell(text) {
  return String(text ?? '').replace(/\|/g, '\\|').replace(/[\r\n]+/g, ' ');
}

/**
 * Upstream links go through redirect.github.com (as Renovate does) so the
 * public tracking issue does not add cross-references to upstream timelines.
 */
export function quietLink(url) {
  return String(url).replace(/^https:\/\/github\.com\//, 'https://redirect.github.com/');
}

function short(sha) {
  return sha.slice(0, 12);
}

export function summaryMarkdown(manifest) {
  const { counts } = manifest.inventory;
  const upstreamUrl = quietLink(`https://github.com/${manifest.upstreamRepository}`);
  const commitLink = sha => `[\`${short(sha)}\`](${upstreamUrl}/commit/${sha})`;
  const previous = manifest.previousReview;
  const lines = [
    `# Upstream review ${manifest.month}`,
    '',
    `Automated monthly summary of [${manifest.upstreamRepository}](${upstreamUrl}). Review it, then file or update fork issues for anything worth adopting. Nothing was merged or committed.`,
    '',
    '| | |',
    '| --- | --- |',
    `| Upstream main | ${commitLink(manifest.upstream)} |`,
    `| Fork baseline | ${commitLink(manifest.baseline.commit)} (${manifest.baseline.kind === 'upstream-merge' ? `last upstream merge \`${short(manifest.baseline.merge)}\`` : 'inherited merge base'}) |`,
    `| Previous review | ${previous ? `[#${previous.number}](${previous.url})` : 'none; compared with the baseline'} |`,
    `| Compared since | ${manifest.since} |`,
    `| Virtual merge into fork | ${manifest.merge.status === 'clean' ? 'clean' : `conflicts in ${manifest.merge.conflicts.length} path(s)`} |`,
    ...(manifest.runUrl ? [`| Workflow run / artifact | [run](${manifest.runUrl}) / \`${manifest.artifact}\` |`] : []),
    '',
    '## Counts',
    '',
    '| | Open | Total |',
    '| --- | ---: | ---: |',
    `| Issues | ${counts.openIssues} | ${counts.issues} |`,
    `| Pull requests | ${counts.openPullRequests} | ${counts.pullRequests} |`,
    `| New or updated since ${manifest.since.slice(0, 10)} | ${manifest.changes.items.filter(item => item.state === 'open').length} | ${manifest.changes.count} |`,
    `| Upstream commits since baseline | | ${manifest.commitsSinceBaseline.count} |`,
    ...(manifest.commitsSincePrevious ? [`| Upstream commits since previous review | | ${manifest.commitsSincePrevious.count} |`] : []),
    '',
  ];
  if (manifest.inventory.warnings.length > 0) {
    lines.push('> [!WARNING]', ...manifest.inventory.warnings.map(warning => `> ${warning}`), '');
  }
  lines.push(`## New or updated upstream items (${manifest.changes.count})`, '');
  if (manifest.changes.count === 0) {
    lines.push('None.', '');
  } else {
    lines.push('| Item | Change | State | Updated | Title |', '| --- | --- | --- | --- | --- |');
    for (const item of manifest.changes.items.slice(0, listLimit)) {
      lines.push(`| [#${item.number}](${quietLink(item.url)}) ${item.type === 'pull_request' ? 'PR' : 'issue'} | ${item.change} | ${item.state}${item.draft ? ' (draft)' : ''} | ${item.updatedAt.slice(0, 10)} | ${escapeCell(item.title)} |`);
    }
    if (manifest.changes.count > listLimit) lines.push('', `${manifest.changes.count - listLimit} more in the artifact's \`manifest.json\`.`);
    lines.push('');
  }
  lines.push(`## Upstream commits since baseline (${manifest.commitsSinceBaseline.count})`, '');
  if (manifest.commitsSinceBaseline.count === 0) {
    lines.push('None. The fork contains upstream main.', '');
  } else {
    for (const commit of manifest.commitsSinceBaseline.items.slice(0, listLimit)) {
      lines.push(`- ${commitLink(commit.sha)} ${commit.date.slice(0, 10)} ${escapeCell(commit.subject)}`);
    }
    if (manifest.commitsSinceBaseline.count > listLimit) lines.push('', `${manifest.commitsSinceBaseline.count - listLimit} more in the artifact's \`manifest.json\`.`);
    lines.push('');
  }
  if (manifest.merge.status !== 'clean') {
    lines.push('## Conflicting paths', '', ...manifest.merge.conflicts.slice(0, listLimit).map(path => `- \`${path}\``), '');
  }
  const data = { month: manifest.month, collectedAt: manifest.inventory.collectedAt, upstreamCommit: manifest.upstream, baselineCommit: manifest.baseline.commit };
  lines.push(`<!-- ${marker} ${JSON.stringify(data)} -->`, '');
  return lines.join('\n');
}

export function prepare(options) {
  const repo = resolve(options.repo);
  const output = resolve(options.output);
  assertCommit(repo, options.base, 'Base');
  assertCommit(repo, options.upstream, 'Upstream candidate');
  const inventory = readJson(options.inventory);
  if (inventory.upstreamCommit?.toLowerCase() !== options.upstream.toLowerCase()) {
    throw new Error('Inventory upstream commit does not match --upstream. Collect the inventory for this candidate.');
  }
  const previous = readJson(options.previous, {});
  const baseline = upstreamBaseline(repo, options.base, options.upstream);
  const baselineDate = git(repo, ['show', '-s', '--format=%cI', baseline.commit]);
  const since = new Date(previous.data?.collectedAt ?? previous.createdAt ?? baselineDate).toISOString();
  const commits = commitsBetween(repo, baseline.commit, options.upstream);
  const previousUpstream = previous.data?.upstreamCommit;
  const commitsSincePrevious = previousUpstream &&
    gitResult(repo, ['merge-base', '--is-ancestor', previousUpstream, options.upstream]).ok
    ? commitsBetween(repo, previousUpstream, options.upstream)
    : null;
  const virtual = gitResult(repo, ['merge-tree', '--write-tree', '--name-only', '--no-messages', options.base, options.upstream]);
  if (!virtual.ok && virtual.status !== 1) throw new Error(`git merge-tree failed with exit ${virtual.status ?? 'unknown'}: ${virtual.output}`);
  const changes = changedItems(inventory.items, since);
  const manifest = {
    schemaVersion: 2,
    generatedAt: new Date().toISOString(),
    month: options.month,
    upstreamRepository: inventory.repository,
    base: options.base.toLowerCase(),
    upstream: options.upstream.toLowerCase(),
    baseline: { ...baseline, date: baselineDate },
    previousReview: previous.number ? { number: previous.number, url: previous.url, title: previous.title, month: previous.data?.month } : null,
    since,
    runUrl: options['run-url'],
    artifact: options.artifact,
    merge: { status: virtual.ok ? 'clean' : 'conflicts', conflicts: virtual.ok ? [] : [...new Set(virtual.output.split('\n').slice(1).filter(Boolean))] },
    inventory: { file: 'inventory.json', collectedAt: inventory.collectedAt, counts: inventory.counts, warnings: inventory.warnings ?? [] },
    commitsSinceBaseline: { count: commits.length, items: commits },
    commitsSincePrevious: commitsSincePrevious ? { from: previousUpstream, count: commitsSincePrevious.length } : null,
    changes: { count: changes.length, items: changes.map(({ number, type, title, url, state, draft, createdAt, updatedAt, change }) => ({ number, type, title, url, state, draft, createdAt, updatedAt, change })) },
  };
  for (const name of ['manifest.json', 'summary.md']) {
    if (existsSync(resolve(output, name))) throw new Error(`Refusing to overwrite ${resolve(output, name)}`);
  }
  mkdirSync(output, { recursive: true });
  writeFileSync(resolve(output, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`, { flag: 'wx' });
  writeFileSync(resolve(output, 'summary.md'), summaryMarkdown(manifest), { flag: 'wx' });
  return manifest;
}

if (process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  const manifest = prepare(parseArgs(process.argv.slice(2)));
  console.log(JSON.stringify({
    month: manifest.month,
    upstream: manifest.upstream,
    baseline: manifest.baseline.commit,
    previousReview: manifest.previousReview?.number ?? null,
    merge: manifest.merge.status,
    commitsSinceBaseline: manifest.commitsSinceBaseline.count,
    changes: manifest.changes.count,
  }, null, 2));
}
