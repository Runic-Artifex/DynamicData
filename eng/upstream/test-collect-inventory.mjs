#!/usr/bin/env node
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '../..');
const collector = resolve(root, 'eng/upstream/collect-inventory.mjs');
const temporary = mkdtempSync(join(tmpdir(), 'upstream-collector-'));
const repository = 'example-upstream/Library';
const upstream = '0123456789abcdef0123456789abcdef01234567';

// Fake gh: 35 open issues and 23 open pull requests (58 open items). The
// repository metadata count depends on FAKE_GH_SCENARIO and the call number.
const fakeGh = join(temporary, 'gh.cjs');
writeFileSync(fakeGh, `#!/usr/bin/env node
const { existsSync, readFileSync, writeFileSync } = require('node:fs');
const endpoint = process.argv.find(value => value.startsWith('repos/'));
const scenario = process.env.FAKE_GH_SCENARIO;
const record = (number, extra) => ({ number, title: 'item ' + number, html_url: 'https://example.invalid/' + number, state: 'open',
  user: { login: 'someone' }, created_at: '2026-10-01T00:00:00Z', updated_at: '2026-10-01T00:00:00Z', closed_at: null, labels: [{ name: 'bug' }], ...extra });
const issues = Array.from({ length: 35 }, (_, index) => record(index + 1, {}));
const pulls = Array.from({ length: 23 }, (_, index) => record(index + 101, { merged_at: null, draft: false, head: { sha: '${upstream}' }, base: { ref: 'main' }, merge_commit_sha: null }));
const issueEndpoint = [...(scenario === 'pr-only' ? [] : issues), ...pulls.map(pull => ({ ...pull, pull_request: {} }))];
if (endpoint === 'repos/${repository}/issues?state=all&per_page=100&sort=created&direction=asc') console.log(JSON.stringify([issueEndpoint]));
else if (endpoint === 'repos/${repository}/pulls?state=all&per_page=100&sort=created&direction=asc') console.log(JSON.stringify([pulls]));
else if (endpoint === 'repos/${repository}') {
  const counter = process.env.FAKE_GH_COUNTER;
  const call = (existsSync(counter) ? Number(readFileSync(counter, 'utf8')) : 0) + 1;
  writeFileSync(counter, String(call));
  const reported = scenario === 'persistent' ? 60 : scenario === 'transient' && call === 1 ? 59 : 58;
  console.log(JSON.stringify({ has_issues: true, open_issues_count: reported }));
}
else process.exit(2);
`, { mode: 0o755 });

function collect(name, scenario) {
  const output = join(temporary, `${name}.json`);
  const counter = join(temporary, `${name}.count`);
  const stdout = execFileSync('node', [collector, '--repository', repository, '--upstream', upstream, '--output', output], {
    cwd: root,
    env: { ...process.env, GH_BIN: fakeGh, FAKE_GH_SCENARIO: scenario, FAKE_GH_COUNTER: counter, UPSTREAM_COLLECT_RETRY_DELAY_MS: '0' },
    encoding: 'utf8',
    stdio: 'pipe',
  });
  return { result: JSON.parse(stdout), snapshot: JSON.parse(readFileSync(output, 'utf8')), calls: Number(readFileSync(counter, 'utf8')) };
}

try {
  const complete = collect('complete', 'complete');
  assert.equal(complete.calls, 1);
  assert.deepEqual(complete.snapshot.counts, { issues: 35, pullRequests: 23, openIssues: 35, openPullRequests: 23, total: 58, open: 58 });
  assert.equal(complete.snapshot.repository, repository);
  assert.equal(complete.snapshot.upstreamCommit, upstream);
  assert.deepEqual(complete.snapshot.warnings, []);
  assert.equal(complete.snapshot.items.length, 58);
  assert.deepEqual(complete.snapshot.items[0].labels, ['bug']);
  assert.equal(complete.snapshot.items.at(-1).type, 'pull_request');

  // Concurrent upstream activity: the second attempt is consistent.
  const transient = collect('transient', 'transient');
  assert.equal(transient.calls, 2);
  assert.deepEqual(transient.snapshot.warnings, []);
  assert.equal(transient.snapshot.counts.open, 58);

  // A persistent disagreement is recorded, not fatal; counts come from the records.
  const persistent = collect('persistent', 'persistent');
  assert.equal(persistent.calls, 3);
  assert.equal(persistent.snapshot.counts.open, 58);
  assert.equal(persistent.snapshot.warnings.length, 1);
  assert.match(persistent.snapshot.warnings[0], /58 open items but the repository reports 60/);

  // An issues response containing only pull requests means missing issue access.
  const prOnly = join(temporary, 'pr-only.json');
  assert.throws(
    () => execFileSync('node', [collector, '--repository', repository, '--upstream', upstream, '--output', prOnly], {
      cwd: root,
      env: { ...process.env, GH_BIN: fakeGh, FAKE_GH_SCENARIO: 'pr-only', FAKE_GH_COUNTER: join(temporary, 'pr-only.count'), UPSTREAM_COLLECT_RETRY_DELAY_MS: '0' },
      stdio: 'pipe',
    }),
    error => /returned only pull requests/.test(String(error.stderr)),
  );
  assert.equal(existsSync(prOnly), false);
  console.log('collect-inventory fixtures passed: complete, transient mismatch retry, persistent mismatch warning, pull-request-only rejection');
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
