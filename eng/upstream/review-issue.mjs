#!/usr/bin/env node
/**
 * Find the previous monthly upstream review issue, or open/update this month's.
 *
 *   previous --repo <owner/name> --month YYYY-MM --output <file>
 *   publish  --repo <owner/name> --month YYYY-MM --body-file <file>
 *
 * Only `gh issue list|create|edit` are used: the review publishes no branches,
 * commits or pull requests.
 */
import { execFile } from 'node:child_process';
import { readFileSync, realpathSync, writeFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { promisify } from 'node:util';

const run = promisify(execFile);
const gh = process.env.GH_BIN ?? 'gh';
export const label = 'upstream-review';
const marker = /<!--\s*upstream-review-data\s+(\{.*?\})\s*-->/s;
const bodyLimit = 65000;
const usage = 'Usage: node eng/upstream/review-issue.mjs previous --repo <owner/name> --month YYYY-MM --output <file>\n' +
  '       node eng/upstream/review-issue.mjs publish --repo <owner/name> --month YYYY-MM --body-file <file>';

export function title(month) {
  return `Upstream review ${month}`;
}

function parseArgs(argv) {
  const [command, ...rest] = argv;
  const values = { command };
  for (let index = 0; index < rest.length; index += 2) {
    if (!rest[index]?.startsWith('--') || rest[index + 1] === undefined) throw new Error(usage);
    values[rest[index].slice(2)] = rest[index + 1];
  }
  const valid = /^[\w.-]+\/[\w.-]+$/.test(values.repo ?? '') && /^\d{4}-(0[1-9]|1[0-2])$/.test(values.month ?? '') &&
    ((command === 'previous' && values.output) || (command === 'publish' && values['body-file']));
  if (!valid) throw new Error(usage);
  return values;
}

async function ghJson(args) {
  const { stdout } = await run(gh, args, { maxBuffer: 64 * 1024 * 1024 });
  return JSON.parse(stdout || '[]');
}

async function reviewIssues(repo) {
  return ghJson(['issue', 'list', '--repo', repo, '--label', label, '--state', 'all', '--limit', '200', '--json', 'number,title,url,state,createdAt,body']);
}

export function parseData(body) {
  const match = marker.exec(body ?? '');
  if (!match) return null;
  try {
    return JSON.parse(match[1]);
  } catch {
    return null;
  }
}

/** The most recent review issue other than this month's, open or closed. */
export function selectPrevious(issues, month) {
  const current = title(month);
  const [latest] = issues
    .filter(issue => issue.title !== current)
    .sort((left, right) => Date.parse(right.createdAt) - Date.parse(left.createdAt) || right.number - left.number);
  if (!latest) return {};
  return { number: latest.number, url: latest.url, title: latest.title, state: latest.state, createdAt: latest.createdAt, data: parseData(latest.body) };
}

function limitBody(body) {
  if (body.length <= bodyLimit) return body;
  const data = marker.exec(body)?.[0] ?? '';
  return `${body.slice(0, bodyLimit - data.length - 200)}\n\n_Truncated; see the workflow artifact for the full summary._\n\n${data}\n`;
}

async function publish(repo, month, bodyFile) {
  const body = limitBody(readFileSync(bodyFile, 'utf8'));
  writeFileSync(bodyFile, body);
  const existing = (await reviewIssues(repo)).find(issue => issue.title === title(month));
  if (existing) {
    await run(gh, ['issue', 'edit', String(existing.number), '--repo', repo, '--body-file', bodyFile]);
    return { status: 'updated', number: existing.number, url: existing.url };
  }
  const { stdout } = await run(gh, ['issue', 'create', '--repo', repo, '--title', title(month), '--label', label, '--body-file', bodyFile]);
  const url = stdout.trim().split('\n').pop();
  return { status: 'created', number: Number(url.split('/').pop()), url };
}

if (process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  const options = parseArgs(process.argv.slice(2));
  if (options.command === 'previous') {
    const previous = selectPrevious(await reviewIssues(options.repo), options.month);
    writeFileSync(options.output, `${JSON.stringify(previous, null, 2)}\n`);
    console.log(JSON.stringify(previous.number ? { previous: previous.number, url: previous.url, data: previous.data } : { previous: null }));
  } else {
    const result = await publish(options.repo, options.month, options['body-file']);
    for (const [key, value] of Object.entries(result)) console.log(`${key}=${value}`);
  }
}
