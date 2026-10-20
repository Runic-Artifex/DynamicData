#!/usr/bin/env python3
"""Keyless, read-only nuget.org checks for the release guard (eng/release/README.md).

plan          decides which packages still need a push, refusing a changed published
              version and an ID the trusted-publishing key could not push.
wait-indexed  waits until the flat container lists each package version.

Only public endpoints are read; this script never sends a key or pushes.
"""

import argparse
import io
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile

SERVICE_INDEX = 'https://api.nuget.org/v3/index.json'
POLICY = Path(__file__).with_name('nuget-policy.json')
# nuget.org adds its repository signature to every accepted package.
SIGNATURE = '.signature.p7s'
ATTEMPTS = 6


class Refusal(Exception):
    pass


def service_index_url():
    override = os.environ.get('NUGET_TEST_SERVICE_INDEX')
    if not override:
        return SERVICE_INDEX
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        raise Refusal('NUGET_TEST_SERVICE_INDEX is only for the offline tests')
    return override


def fetch(url):
    """GET url; returns the body, or None for 404. Retries 429 and 5xx responses."""
    for attempt in range(ATTEMPTS):
        delay = min(30, 2 ** attempt)
        try:
            request = urllib.request.Request(url, headers={'User-Agent': 'runic-dynamicdata-release'})
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            if error.code != 429 and error.code < 500:
                raise Refusal(f'nuget.org lookup failed: HTTP {error.code} {url}') from error
            retry_after = error.headers.get('Retry-After', '')
            if retry_after.isdigit():
                delay = int(retry_after)
            last = f'HTTP {error.code}'
        except (urllib.error.URLError, TimeoutError) as error:
            last = str(getattr(error, 'reason', error))
        if attempt < ATTEMPTS - 1:
            if delay > 120:
                break
            time.sleep(delay)
    raise Refusal(f'nuget.org lookup failed after retries ({last}): {url}')


def fetch_json(url):
    body = fetch(url)
    return None if body is None else json.loads(body)


class NuGet:
    def __init__(self):
        index = fetch_json(service_index_url())
        if not isinstance(index, dict):
            raise Refusal('the nuget.org service index is unavailable')
        resources = index.get('resources', [])

        def resource(prefix):
            for item in resources:
                if str(item.get('@type', '')).startswith(prefix) and isinstance(item.get('@id'), str):
                    return item['@id']
            raise Refusal(f'the nuget.org service index has no {prefix}')

        self.search = resource('SearchQueryService')
        self.flat = resource('PackageBaseAddress/3.0.0').rstrip('/') + '/'

    def versions(self, package_id):
        listing = fetch_json(f'{self.flat}{package_id.lower()}/index.json')
        return None if listing is None else [str(v).lower() for v in listing.get('versions', [])]

    def download(self, package_id, version):
        lower_id, lower_version = package_id.lower(), version.lower()
        return fetch(f'{self.flat}{lower_id}/{lower_version}/{lower_id}.{lower_version}.nupkg')

    def ownership(self, package_id):
        """('new', None), ('exists', [owners]) or ('exists', None) when search lists no version."""
        query = urllib.parse.urlencode({'q': f'packageid:{package_id}', 'prerelease': 'true',
                                        'semVerLevel': '2.0.0', 'take': '1'})
        result = fetch_json(f'{self.search}?{query}') or {}
        for entry in result.get('data', []):
            if str(entry.get('id', '')).lower() == package_id.lower():
                owners = entry.get('owners') or []
                return 'exists', [owners] if isinstance(owners, str) else [str(o) for o in owners]
        # Search omits an ID whose versions are all unlisted (or not indexed yet).
        return ('new', None) if self.versions(package_id) is None else ('exists', None)


def glob_matches(pattern, package_id):
    """nuget.org package globs: '*' matches any run of characters, case-insensitively."""
    expression = '.*'.join(re.escape(part) for part in pattern.split('*'))
    return re.fullmatch(expression, package_id, re.IGNORECASE) is not None


def entries(source, label):
    """Every archive entry except the nuget.org repository signature, by name."""
    try:
        with zipfile.ZipFile(io.BytesIO(source) if isinstance(source, bytes) else source) as archive:
            names = [info.filename for info in archive.infolist()]
            if len(names) != len(set(names)):
                raise Refusal(f'{label} repeats an archive entry')
            return {name: archive.read(name) for name in names if name != SIGNATURE}
    except zipfile.BadZipFile as error:
        raise Refusal(f'{label} is not a valid package: {error}') from error


def identity(path):
    with zipfile.ZipFile(path) as archive:
        nuspecs = [name for name in archive.namelist() if '/' not in name and name.endswith('.nuspec')]
        if len(nuspecs) != 1:
            raise Refusal(f'{path.name} must contain exactly one root .nuspec')
        root = ET.fromstring(archive.read(nuspecs[0]))
    metadata = next((child for child in root if child.tag.rsplit('}', 1)[-1] == 'metadata'), None)
    children = list(metadata) if metadata is not None else []
    values = {child.tag.rsplit('}', 1)[-1]: (child.text or '').strip() for child in children}
    if not values.get('id') or not values.get('version'):
        raise Refusal(f'{path.name} has no package id or version')
    return values['id'], values['version']


def plan(arguments):
    policy_path = Path(os.environ.get('NUGET_POLICY') or POLICY)
    if policy_path != POLICY and os.environ.get('GITHUB_ACTIONS') == 'true':
        raise Refusal('NUGET_POLICY is only for the offline tests')
    policy = json.loads(policy_path.read_text(encoding='utf-8'))
    owner, globs = policy['owner'], policy['packageGlobs']
    nuget = NuGet()
    packages = [(Path(p), *identity(Path(p))) for p in arguments.packages]
    problems, pending = [], []

    for path, package_id, version in packages:
        if not any(glob_matches(glob, package_id) for glob in globs):
            problems.append(f'{package_id} matches none of the trusted-publishing package globs '
                            f'({", ".join(globs)}). Add a matching glob to the policy of {owner} on nuget.org '
                            'and to eng/release/nuget-policy.json, or choose an ID under the glob.')

    for path, package_id, version in packages:
        published = nuget.download(package_id, version)
        if published is None:
            pending.append((path, package_id, version))
            continue
        local, remote = entries(path, path.name), entries(published, f'nuget.org {package_id} {version}')
        if local != remote:
            changed = sorted(name for name in set(local) | set(remote) if local.get(name) != remote.get(name))
            problems.append(f'nuget.org already has {package_id} {version} with different contents '
                            f'({", ".join(changed[:5])}). Published versions are immutable; release a new version.')
        else:
            print(f'{package_id} {version} is already on nuget.org with identical contents; skipping it.')

    for path, package_id, version in pending:
        state, owners = nuget.ownership(package_id)
        if state == 'new':
            if policy.get('pushNewPackages'):
                print(f'::notice::{package_id} is a new nuget.org package ID; the trusted-publishing policy of {owner} must cover it.')
            else:
                problems.append(f'{package_id} is a new nuget.org package ID, but the policy may only push versions of existing packages. '
                                f'Allow the policy of {owner} to push new packages.')
        elif owners is None:
            problems.append(f'nuget.org knows {package_id}, but its search lists no version, so its owners cannot be checked. '
                            'If a version was pushed moments ago, wait until nuget.org search lists it and rerun; '
                            f'otherwise relist a version and make sure {owner} owns the ID.')
        elif not any(o.lower() == owner.lower() for o in owners):
            problems.append(f'{package_id} is owned on nuget.org by {", ".join(owners) or "nobody listed"}, not by {owner}, '
                            f'so the trusted-publishing key cannot push it. Ask the owners to add {owner} as an owner, '
                            'or choose another package ID.')

    if problems:
        raise Refusal('\n'.join(['nuget.org cannot accept this release as built; nothing was pushed.',
                                 *(f'- {problem}' for problem in problems),
                                 'See eng/release/README.md, "nuget.org publishing".']))

    Path(arguments.plan_file).write_text(''.join(f'{path}\n' for path, _, _ in pending), encoding='utf-8')
    names = ', '.join(f'{package_id} {version}' for _, package_id, version in pending)
    print(f'{len(pending)} of {len(packages)} packages need a push: {names or "none"}.')


def wait_indexed(arguments):
    pairs = arguments.packages
    if len(pairs) % 2:
        raise Refusal('wait-indexed takes <id> <version> pairs')
    missing = list(zip(pairs[::2], pairs[1::2]))
    nuget = NuGet()
    deadline = time.monotonic() + arguments.timeout_seconds
    while True:
        missing = [(i, v) for i, v in missing if v.lower() not in (nuget.versions(i) or [])]
        if not missing:
            print('nuget.org lists every package of this release.')
            return
        names = ', '.join(f'{i} {v}' for i, v in missing)
        if time.monotonic() + arguments.interval_seconds > deadline:
            raise Refusal(f'nuget.org still does not list {names} after {arguments.timeout_seconds} seconds; rerun this job later.')
        print(f'Waiting for nuget.org to list {names}', flush=True)
        time.sleep(arguments.interval_seconds)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest='command', required=True)
    plan_parser = commands.add_parser('plan')
    plan_parser.add_argument('--plan-file', required=True, help='receives the packages that still need a push, one per line')
    plan_parser.add_argument('packages', nargs='+')
    wait_parser = commands.add_parser('wait-indexed')
    wait_parser.add_argument('--timeout-seconds', type=float, default=3600)
    wait_parser.add_argument('--interval-seconds', type=float, default=30)
    wait_parser.add_argument('packages', nargs='+', help='<id> <version> pairs')
    arguments = parser.parse_args()
    try:
        plan(arguments) if arguments.command == 'plan' else wait_indexed(arguments)
    except Refusal as refusal:
        print(f'nuget: {refusal}', file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
