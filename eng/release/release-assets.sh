#!/usr/bin/env bash
# Validates the two branded packages and, only when requested by the manual
# GitHub workflow, creates an immutable prerelease from those exact bytes.
set -euo pipefail

readonly PACKAGE_ID='Runic.DynamicData'
readonly REACTIVE_PACKAGE_ID='Runic.DynamicData.Reactive'
readonly DEFAULT_PRIMITIVES_VERSION='9.0.0'
readonly BUILD_WORKFLOW_PATH='.github/workflows/ci-build.yml'
readonly GH_BIN="${GH_BIN:-gh}"
TEMP_FILES=()

cleanup() {
  local path
  for path in "${TEMP_FILES[@]}"; do
    rm -f -- "$path"
  done
}
trap cleanup EXIT

die() {
  printf 'release-assets: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat >&2 <<'EOF'
Usage: eng/release/release-assets.sh <validate|publish> <package-directory>

validate verifies the exact branded pair and its package metadata.
publish additionally creates a draft GitHub prerelease, uploads and downloads
the assets byte-for-byte, then publishes that draft.
EOF
  exit 2
}

require_environment() {
  [[ "${GITHUB_REPOSITORY:-}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] ||
    die 'GITHUB_REPOSITORY must identify the current workflow repository.'
  [[ "${GITHUB_SHA:-}" =~ ^[0-9a-f]{40}$ ]] ||
    die 'GITHUB_SHA must be the 40-character commit being released.'
}

require_tools() {
  command -v python3 >/dev/null || die 'python3 is required to inspect package metadata.'
  command -v cmp >/dev/null || die 'cmp is required to verify uploaded assets.'
}

validate_nuspec() {
  local package="$1"
  local package_id="$2"
  local dependency_id="$3"
  local expected_version="$4"
  local expected_repository="$5"
  local expected_sha="$6"
  local assembly_name="$7"

  python3 - "$package" "$package_id" "$expected_version" "$expected_repository" "$expected_sha" "$dependency_id" "$DEFAULT_PRIMITIVES_VERSION" "$assembly_name" 'LICENSE' <<'PY'
import sys
import xml.etree.ElementTree as ET
import zipfile

path, package_id, version, repository, sha, dependency_id, dependency_version, assembly_name, license_path = sys.argv[1:]

try:
    package = zipfile.ZipFile(path)
except zipfile.BadZipFile as error:
    raise SystemExit(f"invalid nupkg archive: {error}")

nuspec_name = f'{package_id}.nuspec'
required_entries = {
    nuspec_name,
    'README.md',
    'logo.png',
    'LICENSE/LICENSE',
    f'lib/net10.0/{assembly_name}.dll',
    f'lib/net10.0/{assembly_name}.xml',
}
missing_entries = sorted(required_entries - set(package.namelist()))
if missing_entries:
    raise SystemExit(f"package is missing required content: {', '.join(missing_entries)}")
duplicate_entries = sorted(entry for entry in required_entries if package.namelist().count(entry) != 1)
if duplicate_entries:
    raise SystemExit(f"package repeats required content: {', '.join(duplicate_entries)}")
library_frameworks = {
    entry.split('/')[1]
    for entry in package.namelist()
    if entry.startswith('lib/') and len(entry.split('/')) >= 3 and not entry.endswith('/')
}
if library_frameworks != {'net10.0'}:
    raise SystemExit(f"package library target frameworks are not exactly net10.0: {', '.join(sorted(library_frameworks))}")
try:
    with open(license_path, 'rb') as expected_license:
        if package.read('LICENSE/LICENSE') != expected_license.read():
            raise SystemExit('package license payload does not match the repository LICENSE')
except OSError as error:
    raise SystemExit(f'could not read repository LICENSE: {error}')

try:
    root = ET.fromstring(package.read(nuspec_name))
except (KeyError, ET.ParseError) as error:
    raise SystemExit(f"invalid nuspec XML: {error}")

def local_name(element):
    return element.tag.rsplit('}', 1)[-1]

def one(parent, name):
    matches = [child for child in parent if local_name(child) == name]
    if len(matches) != 1:
        raise SystemExit(f"expected exactly one <{name}> element, found {len(matches)}")
    return matches[0]

metadata = one(root, 'metadata')

def text(name):
    value = (one(metadata, name).text or '').strip()
    if not value:
        raise SystemExit(f"<{name}> must not be empty")
    return value

if text('id') != package_id:
    raise SystemExit(f"package id is not {package_id}")
if text('version') != version:
    raise SystemExit(f"package version is not {version}")
if text('authors') != 'Roland Pheasant':
    raise SystemExit('package authorship must retain Roland Pheasant')

license_element = one(metadata, 'license')
if license_element.get('type') != 'expression' or (license_element.text or '').strip() != 'MIT':
    raise SystemExit('package license must be the MIT expression')

repository_element = one(metadata, 'repository')
if repository_element.get('type') != 'git':
    raise SystemExit('package repository type must be git')
if repository_element.get('url') != f'https://github.com/{repository}':
    raise SystemExit('package repository URL does not identify the current workflow repository')
if repository_element.get('commit') != sha:
    raise SystemExit('package repository commit does not match GITHUB_SHA')

dependencies = one(metadata, 'dependencies')
groups = [child for child in dependencies if local_name(child) == 'group']
if len(groups) != 1 or groups[0].get('targetFramework') != 'net10.0':
    raise SystemExit('package must contain exactly one net10.0 dependency group')

entries = [child for child in groups[0] if local_name(child) == 'dependency']
if len(entries) != 1:
    raise SystemExit('package must contain exactly one runtime dependency')
entry = entries[0]
if entry.get('id') != dependency_id:
    raise SystemExit(f'package dependency is not {dependency_id}')
if entry.get('version') != dependency_version:
    raise SystemExit(f'package dependency version is not {dependency_version}')
PY
}

PACKAGE_VERSION=''
PACKAGE_FILES=()

validate_packages() {
  local package_directory="$1"
  local names=()
  local file

  require_environment
  require_tools
  [[ -d "$package_directory" ]] || die "package directory does not exist: $package_directory"

  while IFS= read -r -d '' file; do
    names+=("$(basename "$file")")
  done < <(find "$package_directory" -maxdepth 1 -type f -name '*.nupkg' -print0 | sort -z)

  (( ${#names[@]} == 2 )) ||
    die "expected exactly two .nupkg files in $package_directory, found ${#names[@]}."

  local base_package=''
  for file in "${names[@]}"; do
    if [[ "$file" == "$PACKAGE_ID".*.nupkg ]] && [[ "$file" != "$REACTIVE_PACKAGE_ID".*.nupkg ]]; then
      [[ -z "$base_package" ]] || die 'found more than one Runic.DynamicData package.'
      base_package="$file"
    fi
  done
  [[ -n "$base_package" ]] || die 'missing the Runic.DynamicData package.'

  PACKAGE_VERSION="${base_package#"$PACKAGE_ID".}"
  PACKAGE_VERSION="${PACKAGE_VERSION%.nupkg}"
  [[ "$PACKAGE_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$ ]] ||
    die "invalid package version in $base_package."

  local expected_base="$PACKAGE_ID.$PACKAGE_VERSION.nupkg"
  local expected_reactive="$REACTIVE_PACKAGE_ID.$PACKAGE_VERSION.nupkg"
  [[ -f "$package_directory/$expected_base" ]] || die "missing $expected_base."
  [[ -f "$package_directory/$expected_reactive" ]] || die "missing $expected_reactive."

  local expected
  for expected in "$expected_base" "$expected_reactive"; do
    local found=false
    for file in "${names[@]}"; do
      if [[ "$file" == "$expected" ]]; then
        found=true
        break
      fi
    done
    "$found" || die "unexpected package set; missing exact filename $expected."
  done

  PACKAGE_FILES=("$package_directory/$expected_base" "$package_directory/$expected_reactive")
  validate_nuspec "${PACKAGE_FILES[0]}" "$PACKAGE_ID" 'ReactiveUI.Primitives' "$PACKAGE_VERSION" "$GITHUB_REPOSITORY" "$GITHUB_SHA" 'DynamicData'
  validate_nuspec "${PACKAGE_FILES[1]}" "$REACTIVE_PACKAGE_ID" 'ReactiveUI.Primitives.Reactive' "$PACKAGE_VERSION" "$GITHUB_REPOSITORY" "$GITHUB_SHA" 'DynamicData.Reactive'
}

assert_remote_absent() {
  local kind="$1"
  local endpoint="$2"
  local response
  response="$(mktemp)"
  TEMP_FILES+=("$response")

  if "$GH_BIN" api --silent --include "$endpoint" >"$response" 2>&1; then
    die "$kind already exists; published versions, tags and release assets are immutable."
  fi

  if grep -Eq '^HTTP/[0-9.]+ 404( |$)' "$response"; then
    return
  fi

  cat "$response" >&2
  die "could not determine whether $kind exists; refusing to publish."
}

assert_release_absent() {
  local tag="$1"
  local response
  response="$(mktemp)"
  TEMP_FILES+=("$response")

  if ! "$GH_BIN" api --paginate --slurp "repos/$GITHUB_REPOSITORY/releases?per_page=100" >"$response" 2>&1; then
    cat "$response" >&2
    die 'could not list releases; refusing to publish.'
  fi

  python3 - "$response" "$tag" <<'PY'
import json
import sys

path, tag = sys.argv[1:]
try:
    pages = json.load(open(path, encoding='utf-8'))
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f'could not parse release list: {error}')

if not isinstance(pages, list):
    raise SystemExit('release list response is not a page list')
for page in pages:
    if not isinstance(page, list):
        raise SystemExit('release list page is not an array')
    for release in page:
        if isinstance(release, dict) and release.get('tag_name') == tag:
            raise SystemExit(f'release {tag} already exists; release assets are immutable')
PY
}

assert_trusted_workflow_source() {
  [[ "${RELEASE_ALLOW_LOCAL_PUBLISH:-}" == '1' ]] && return

  local default_branch
  default_branch="$("$GH_BIN" api "repos/$GITHUB_REPOSITORY" --jq '.default_branch')" ||
    die 'could not resolve the current repository default branch.'
  [[ "$default_branch" == 'main' && "${GITHUB_REF:-}" == 'refs/heads/main' ]] ||
    die 'manual package release must run from maintained main.'
  [[ "$(git rev-parse HEAD)" == "$GITHUB_SHA" ]] ||
    die 'checked-out source does not match GITHUB_SHA.'
  git diff --quiet --ignore-submodules -- ||
    die 'checked-out source has uncommitted changes.'
  git diff --cached --quiet --ignore-submodules -- ||
    die 'checked-out source has staged changes.'
  [[ -z "$(git ls-files --others --exclude-standard)" ]] ||
    die 'checked-out source has unexpected untracked files.'
}

assert_hosted_build() {
  local runs_json jobs_json run_id
  runs_json="$(mktemp)"
  TEMP_FILES+=("$runs_json")
  if ! "$GH_BIN" api --paginate --slurp \
    "repos/$GITHUB_REPOSITORY/actions/workflows/ci-build.yml/runs?head_sha=$GITHUB_SHA&per_page=100" \
    >"$runs_json" 2>&1; then
    cat "$runs_json" >&2
    die 'could not list completed Build workflow runs; refusing to publish.'
  fi

  run_id="$(python3 - "$runs_json" "$GITHUB_SHA" "$GITHUB_REPOSITORY" "$BUILD_WORKFLOW_PATH" <<'PY'
import json
import sys

path, expected_sha, repository, workflow_path = sys.argv[1:]
try:
    pages = json.load(open(path, encoding='utf-8'))
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f'could not parse Build workflow runs: {error}')

runs = []
for page in pages:
    if not isinstance(page, dict) or not isinstance(page.get('workflow_runs'), list):
        raise SystemExit('Build workflow run response has an unexpected page shape')
    for run in page['workflow_runs']:
        if not isinstance(run, dict):
            continue
        path_value = run.get('path')
        head_repository = run.get('head_repository')
        if (run.get('head_sha') == expected_sha and isinstance(head_repository, dict) and
                head_repository.get('full_name') == repository and run.get('head_branch') == 'main' and
                run.get('event') in {'push', 'workflow_dispatch'} and isinstance(path_value, str) and
                (path_value == workflow_path or path_value == f'{workflow_path}@refs/heads/main') and
                isinstance(run.get('id'), int)):
            runs.append(run)

if not runs:
    raise SystemExit('no current Build workflow run exists for GITHUB_SHA')

def ordering(run):
    return (run.get('created_at') or '', run['id'])

latest = max(runs, key=ordering)
if latest.get('status') != 'completed' or latest.get('conclusion') != 'success':
    raise SystemExit('latest current Build workflow run is not completed successfully')
print(latest['id'])
PY
)"

  jobs_json="$(mktemp)"
  TEMP_FILES+=("$jobs_json")
  if ! "$GH_BIN" api --paginate --slurp \
    "repos/$GITHUB_REPOSITORY/actions/runs/$run_id/jobs?filter=all&per_page=100" \
    >"$jobs_json" 2>&1; then
    cat "$jobs_json" >&2
    die 'could not list Build jobs; refusing to publish.'
  fi

  python3 - "$jobs_json" <<'PY'
import json
import sys

path = sys.argv[1]
expected = {
    'build (ubuntu-latest, 8.4.0)',
    'build (ubuntu-latest, 9.0.0)',
    'build (windows-latest, 8.4.0)',
    'build (windows-latest, 9.0.0)',
}
try:
    pages = json.load(open(path, encoding='utf-8'))
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f'could not parse Build jobs: {error}')

latest = {}
for page in pages:
    if not isinstance(page, dict) or not isinstance(page.get('jobs'), list):
        raise SystemExit('Build jobs response has an unexpected page shape')
    for job in page['jobs']:
        if not isinstance(job, dict) or job.get('name') not in expected:
            continue
        key = job['name']
        ordering = (job.get('run_attempt') or 0, job.get('completed_at') or '', job.get('id') or 0)
        previous = latest.get(key)
        if previous is None or ordering > previous[0]:
            latest[key] = (ordering, job)

missing = sorted(expected - set(latest))
if missing:
    raise SystemExit(f'Build matrix is missing: {", ".join(missing)}')
failed = sorted(name for name, (_, job) in latest.items() if job.get('conclusion') != 'success')
if failed:
    raise SystemExit(f'latest Build matrix jobs did not succeed: {", ".join(failed)}')
PY
}

verify_tag_target() {
  local tag="$1"
  local tag_json
  tag_json="$(mktemp)"
  TEMP_FILES+=("$tag_json")
  "$GH_BIN" api "repos/$GITHUB_REPOSITORY/git/ref/tags/$tag" >"$tag_json"

  python3 - "$tag_json" "$GITHUB_SHA" <<'PY'
import json
import sys

path, expected_sha = sys.argv[1:]
with open(path, encoding='utf-8') as source:
    reference = json.load(source)
object_ = reference.get('object')
if not isinstance(object_, dict) or object_.get('type') != 'commit' or object_.get('sha') != expected_sha:
    raise SystemExit('created tag does not resolve directly to GITHUB_SHA')
PY
}

create_tag_at_source() {
  local tag="$1"
  "$GH_BIN" api --method POST "repos/$GITHUB_REPOSITORY/git/refs" \
    -f "ref=refs/tags/$tag" \
    -f "sha=$GITHUB_SHA" >/dev/null
  verify_tag_target "$tag"
}

read_release_assets() {
  local release_json="$1"
  local package_one="$2"
  local package_two="$3"
  local expected_sha="$4"

  python3 - "$release_json" "$package_one" "$package_two" "$expected_sha" <<'PY'
import json
import os
import sys

path, package_one, package_two, expected_sha = sys.argv[1:]
with open(path, encoding='utf-8') as source:
    release = json.load(source)

if release.get('draft') is not True:
    raise SystemExit('release is not still a draft during asset verification')
if release.get('prerelease') is not True:
    raise SystemExit('release is not marked prerelease')
if release.get('target_commitish') != expected_sha:
    raise SystemExit('draft release target does not match GITHUB_SHA')

expected = {os.path.basename(package_one): package_one, os.path.basename(package_two): package_two}
assets = release.get('assets')
if not isinstance(assets, list) or len(assets) != len(expected):
    raise SystemExit('draft release does not contain exactly the expected package assets')

seen = set()
for asset in assets:
    name = asset.get('name')
    asset_id = asset.get('id')
    if name not in expected or name in seen or not isinstance(asset_id, int):
        raise SystemExit('draft release asset set is not the expected exact package pair')
    if asset.get('size') != os.path.getsize(expected[name]):
        raise SystemExit(f'draft asset size does not match {name}')
    seen.add(name)
    print(f'{asset_id}\t{name}')

if seen != set(expected):
    raise SystemExit('draft release omitted an expected package asset')
PY
}

create_draft() {
  local tag="$1"
  local draft_json
  draft_json="$(mktemp)"
  TEMP_FILES+=("$draft_json")
  "$GH_BIN" api --method POST "repos/$GITHUB_REPOSITORY/releases" \
    -f "tag_name=$tag" \
    -f "target_commitish=$GITHUB_SHA" \
    -f "name=$PACKAGE_VERSION" \
    -F draft=true \
    -F prerelease=true \
    -F generate_release_notes=true >"$draft_json"

  python3 - "$draft_json" "$GITHUB_SHA" <<'PY'
import json
import sys

path, expected_sha = sys.argv[1:]
with open(path, encoding='utf-8') as source:
    draft = json.load(source)
if not isinstance(draft.get('id'), int):
    raise SystemExit('GitHub did not return a draft release id')
if draft.get('draft') is not True or draft.get('prerelease') is not True:
    raise SystemExit('GitHub did not create the expected prerelease draft')
if draft.get('target_commitish') != expected_sha:
    raise SystemExit('draft release target does not match GITHUB_SHA')
print(draft['id'])
PY
}

publish_draft() {
  local release_id="$1"
  "$GH_BIN" api --method PATCH "repos/$GITHUB_REPOSITORY/releases/$release_id" \
    -F draft=false >/dev/null
}

publish_packages() {
  local package_directory="$1"
  validate_packages "$package_directory"

  [[ "${GITHUB_ACTIONS:-}" == 'true' || "${RELEASE_ALLOW_LOCAL_PUBLISH:-}" == '1' ]] ||
    die 'publish may run only in GitHub Actions.'
  command -v "$GH_BIN" >/dev/null || die "GitHub CLI not found: $GH_BIN"
  [[ -n "${GH_TOKEN:-${GITHUB_TOKEN:-}}" ]] || die 'GitHub token is required to publish a release.'
  assert_trusted_workflow_source
  assert_hosted_build

  local tag="v$PACKAGE_VERSION"
  assert_remote_absent "tag $tag" "repos/$GITHUB_REPOSITORY/git/ref/tags/$tag"
  assert_release_absent "$tag"

  local release_id
  release_id="$(create_draft "$tag")"
  "$GH_BIN" release upload "$tag" \
    --repo "$GITHUB_REPOSITORY" \
    "${PACKAGE_FILES[0]}" \
    "${PACKAGE_FILES[1]}"

  local release_json
  release_json="$(mktemp)"
  TEMP_FILES+=("$release_json")
  "$GH_BIN" api "repos/$GITHUB_REPOSITORY/releases/$release_id" >"$release_json"

  local asset_rows asset_id asset_name downloaded
  asset_rows="$(mktemp)"
  TEMP_FILES+=("$asset_rows")
  read_release_assets "$release_json" "${PACKAGE_FILES[0]}" "${PACKAGE_FILES[1]}" "$GITHUB_SHA" >"$asset_rows"
  while IFS=$'\t' read -r asset_id asset_name; do
    downloaded="$(mktemp)"
    TEMP_FILES+=("$downloaded")
    "$GH_BIN" api -H 'Accept: application/octet-stream' \
      "repos/$GITHUB_REPOSITORY/releases/assets/$asset_id" >"$downloaded"
    cmp -s "$package_directory/$asset_name" "$downloaded" ||
      die "uploaded asset bytes do not match $asset_name."
  done <"$asset_rows"

  create_tag_at_source "$tag"
  publish_draft "$release_id"
}

[[ $# -eq 2 ]] || usage
case "$1" in
  validate)
    validate_packages "$2"
    ;;
  publish)
    publish_packages "$2"
    ;;
  *)
    usage
    ;;
esac
