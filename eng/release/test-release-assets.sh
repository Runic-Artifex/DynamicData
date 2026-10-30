#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly root
readonly helper="$root/eng/release/release-assets.sh"
readonly fixtures="$root/eng/release/fixtures"
readonly mock_gh="$root/eng/release/tests/mock-gh.sh"
readonly mock_dotnet="$root/eng/release/tests/mock-dotnet.sh"
readonly mock_nuget="$root/eng/release/tests/mock-nuget.py"
readonly repository='Runic-Artifex/DynamicData'
readonly sha='0123456789abcdef0123456789abcdef01234567'
readonly version='10.0.0-runic.test'
scratch="$(mktemp -d)"
mock_nuget_pid=''
cleanup() {
  [[ -z "$mock_nuget_pid" ]] || kill "$mock_nuget_pid" 2>/dev/null || true
  rm -rf "$scratch"
}
trap cleanup EXIT

fail() {
  printf 'test-release-assets: %s\n' "$*" >&2
  exit 1
}

write_nuspec() {
  local fixture="$1"
  local output="$2"
  local nuspec_version="${3:-$version}"
  sed \
    -e "s/@VERSION@/$nuspec_version/g" \
    -e "s|@REPOSITORY@|$repository|g" \
    -e "s/@SHA@/$sha/g" \
    "$fixture" >"$output"
}

make_package() {
  local destination="$1"
  local package_id="$2"
  local package_version="$3"
  local fixture="$4"
  local stage="$scratch/stage-$RANDOM-$RANDOM"
  mkdir -p "$stage"
  write_nuspec "$fixture" "$stage/$package_id.nuspec" "${NUSPEC_VERSION:-$version}"
  PACKAGE_ID="$package_id" PACKAGE_VERSION="$package_version" STAGE="$stage" DESTINATION="$destination" REPOSITORY_ROOT="$root" \
    python3 - <<'PY'
import os
import pathlib
import zipfile

package_id = os.environ['PACKAGE_ID']
package_version = os.environ['PACKAGE_VERSION']
stage = os.environ['STAGE']
destination = os.environ['DESTINATION']
assembly = 'DynamicData.Reactive' if package_id.endswith('.Reactive') else 'DynamicData'
for entry in ('README.md', 'logo.png', f'lib/net10.0/{assembly}.dll', f'lib/net10.0/{assembly}.xml'):
    path = os.path.join(stage, entry)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as output:
        output.write(b'fixture')
license_path = pathlib.Path(os.environ['REPOSITORY_ROOT']) / 'LICENSE'
target_license = os.path.join(stage, 'LICENSE', 'LICENSE')
os.makedirs(os.path.dirname(target_license), exist_ok=True)
with open(license_path, 'rb') as source, open(target_license, 'wb') as output:
    output.write(source.read())
package = os.path.join(destination, f'{package_id}.{package_version}.nupkg')
with zipfile.ZipFile(package, 'w') as output:
    for directory, _, names in os.walk(stage):
        for name in names:
            path = os.path.join(directory, name)
            output.write(path, os.path.relpath(path, stage))
PY
}

make_pair() {
  local destination="$1"
  local base_fixture="${2:-$fixtures/RunicArtifex.DynamicData.nuspec}"
  local reactive_fixture="${3:-$fixtures/RunicArtifex.DynamicData.Reactive.nuspec}"
  local reactive_version="${4:-$version}"
  mkdir -p "$destination"
  make_package "$destination" 'RunicArtifex.DynamicData' "$version" "$base_fixture"
  make_package "$destination" 'RunicArtifex.DynamicData.Reactive' "$reactive_version" "$reactive_fixture"
}

validate() {
  GITHUB_REPOSITORY="$repository" GITHUB_SHA="$sha" bash "$helper" validate "$1"
}

append_package_entry() {
  local package="$1"
  local entry="$2"
  PACKAGE_PATH="$package" PACKAGE_ENTRY="$entry" python3 - <<'PY'
import os
import tempfile
import zipfile

path = os.environ['PACKAGE_PATH']
entry = os.environ['PACKAGE_ENTRY']
with zipfile.ZipFile(path) as source, tempfile.NamedTemporaryFile(delete=False) as temporary:
    temporary_path = temporary.name
    with zipfile.ZipFile(temporary, 'w') as output:
        for item in source.infolist():
            if item.filename != entry:
                output.writestr(item, source.read(item))
        output.writestr(entry, b'wrong')
os.replace(temporary_path, path)
PY
}

expect_failure() {
  local description="$1"
  shift
  if "$@" >"$scratch/output" 2>&1; then
    fail "$description unexpectedly succeeded"
  fi
}

# Like expect_failure, and the output must contain the expected reason.
expect_failure_with() {
  local description="$1"
  local reason="$2"
  shift 2
  expect_failure "$description" "$@"
  grep -qF -- "$reason" "$scratch/output" || {
    cat "$scratch/output" >&2
    fail "$description failed without the expected reason: $reason"
  }
}

valid="$scratch/valid"
make_pair "$valid"
validate "$valid"

malformed="$scratch/malformed"
make_pair "$malformed" "$fixtures/malformed.nuspec"
expect_failure 'malformed nuspec' validate "$malformed"

mismatched_pair="$scratch/mismatched-pair"
make_pair "$mismatched_pair" '' '' '10.0.1-runic.test'
expect_failure 'mismatched package versions' validate "$mismatched_pair"

wrong_source="$scratch/wrong-source"
make_pair "$wrong_source" "$fixtures/wrong-repository-sha.nuspec"
expect_failure 'repository SHA mismatch' validate "$wrong_source"

wrong_license="$scratch/wrong-license"
make_pair "$wrong_license" "$fixtures/wrong-license.nuspec"
expect_failure 'license mismatch' validate "$wrong_license"

upstream_metadata="$scratch/upstream-metadata"
make_pair "$upstream_metadata" "$fixtures/upstream-metadata.nuspec"
expect_failure_with 'upstream authors' 'package authors must be' validate "$upstream_metadata"

upstream_title="$scratch/upstream-title"
make_pair "$upstream_title" "$fixtures/upstream-title.nuspec"
expect_failure_with 'upstream title' 'must start by naming the Runic Artifex fork' validate "$upstream_title"

declared_owners="$scratch/declared-owners"
make_pair "$declared_owners" "$fixtures/owners.nuspec"
expect_failure_with 'declared owners' 'must not declare <owners>' validate "$declared_owners"

invalid_version="$scratch/invalid-version"
mkdir -p "$invalid_version"
make_package "$invalid_version" 'RunicArtifex.DynamicData' 'not-a-version' "$fixtures/RunicArtifex.DynamicData.nuspec"
make_package "$invalid_version" 'RunicArtifex.DynamicData.Reactive' 'not-a-version' "$fixtures/RunicArtifex.DynamicData.Reactive.nuspec"
expect_failure 'malformed package version' validate "$invalid_version"

wrong_target="$scratch/wrong-target"
make_pair "$wrong_target"
append_package_entry "$wrong_target/RunicArtifex.DynamicData.$version.nupkg" 'lib/net9.0/DynamicData.dll'
expect_failure 'additional library target' validate "$wrong_target"

wrong_license_payload="$scratch/wrong-license-payload"
make_pair "$wrong_license_payload"
append_package_entry "$wrong_license_payload/RunicArtifex.DynamicData.$version.nupkg" 'LICENSE/LICENSE'
expect_failure 'license payload mismatch' validate "$wrong_license_payload"

run_publish() {
  local state="$1"
  shift
  MOCK_GH_STATE="$state" \
    GH_BIN="$mock_gh" \
    GITHUB_REPOSITORY="$repository" \
    GITHUB_SHA="$sha" \
    GITHUB_TOKEN='test-token' \
    RELEASE_ALLOW_LOCAL_PUBLISH=1 \
    "$@"
}

tag_collision="$scratch/tag-collision"
mkdir -p "$tag_collision"
expect_failure 'existing tag at another SHA' run_publish "$tag_collision" env MOCK_GH_TAG_STATE=existing bash "$helper" publish "$valid"

release_collision="$scratch/release-collision"
mkdir -p "$release_collision"
expect_failure 'existing release' run_publish "$release_collision" env MOCK_GH_RELEASE_STATE=existing bash "$helper" publish "$valid"

operational_failure="$scratch/operational-failure"
mkdir -p "$operational_failure"
expect_failure 'non-404 API failure' run_publish "$operational_failure" env MOCK_GH_TAG_STATE=forbidden bash "$helper" publish "$valid"

published="$scratch/published"
mkdir -p "$published"
run_publish "$published" bash "$helper" publish "$valid"
[[ -f "$published/published" ]] || fail 'valid draft was not published after byte verification'
[[ "$(cat "$published/tag")" == "$sha" ]] || fail 'publish did not create the tag at GITHUB_SHA'
[[ "$(find "$published/assets" -maxdepth 1 -type f -name '*.nupkg' | wc -l)" == '2' ]] ||
  fail 'publish did not upload the exact package pair'
[[ "$(cat "$published/prerelease")" == 'true' ]] || fail 'prerelease version was not published as a GitHub prerelease'

# A stable version (no prerelease label) becomes a regular GitHub release.
stable_valid="$scratch/stable-valid"
mkdir -p "$stable_valid"
NUSPEC_VERSION='10.0.0' make_package "$stable_valid" 'RunicArtifex.DynamicData' '10.0.0' "$fixtures/RunicArtifex.DynamicData.nuspec"
NUSPEC_VERSION='10.0.0' make_package "$stable_valid" 'RunicArtifex.DynamicData.Reactive' '10.0.0' "$fixtures/RunicArtifex.DynamicData.Reactive.nuspec"
stable_published="$scratch/stable-published"
mkdir -p "$stable_published"
run_publish "$stable_published" bash "$helper" publish "$stable_valid"
[[ -f "$stable_published/published" ]] || fail 'stable draft was not published'
[[ "$(cat "$stable_published/prerelease")" == 'false' ]] || fail 'stable version was published as a GitHub prerelease'
run_publish "$scratch/stable-check" bash "$helper" check "$stable_valid" >"$scratch/output" 2>&1 ||
  { cat "$scratch/output" >&2; fail 'stable GitHub dry-run check failed'; }
grep -qF 'v10.0.0 (prerelease false)' "$scratch/output" || fail 'stable dry-run check did not report a regular release'

for malformed_draft in wrong-target missing-assets flipped-prerelease; do
  draft_failure="$scratch/draft-$malformed_draft"
  mkdir -p "$draft_failure"
  expect_failure "draft $malformed_draft" run_publish "$draft_failure" env MOCK_GH_DRAFT_STATE="$malformed_draft" bash "$helper" publish "$valid"
  [[ ! -e "$draft_failure/published" ]] || fail "invalid draft $malformed_draft was published"
  [[ ! -e "$draft_failure/tag" ]] || fail "invalid draft $malformed_draft created a tag"
done

for ci_case in missing-cell wrong-sha wrong-workflow failed-job old-green-new-failed old-green-new-running wrong-head-repository non-main pull-request; do
  ci_failure="$scratch/ci-$ci_case"
  mkdir -p "$ci_failure"
  expect_failure "CI gate $ci_case" run_publish "$ci_failure" env MOCK_GH_CI_CASE="$ci_case" bash "$helper" publish "$valid"
  [[ ! -e "$ci_failure/release" ]] || fail "CI gate $ci_case created a draft"
done

# Dry run, script level: check and nuget-check need no NuGet key and write nothing.
dry_run_github="$scratch/dry-run-github"
mkdir -p "$dry_run_github"
run_publish "$dry_run_github" bash "$helper" check "$valid" >"$scratch/output" 2>&1 ||
  { cat "$scratch/output" >&2; fail 'GitHub dry-run check failed'; }
[[ ! -e "$dry_run_github/release" && ! -e "$dry_run_github/tag" && ! -e "$dry_run_github/published" ]] ||
  fail 'GitHub dry-run check created a draft, tag or release'
expect_failure 'GitHub dry-run check with an existing tag' run_publish "$dry_run_github" env MOCK_GH_TAG_STATE=existing bash "$helper" check "$valid"

# nuget.org, mocked: a local HTTP server serves one directory per case.
nuget_root="$scratch/nuget"
mkdir -p "$nuget_root"
python3 "$mock_nuget" "$nuget_root" "$scratch/nuget-port" &
mock_nuget_pid=$!
for _ in $(seq 100); do
  [[ -s "$scratch/nuget-port" ]] && break
  sleep 0.1
done
[[ -s "$scratch/nuget-port" ]] || fail 'mock nuget.org did not start'
nuget_port="$(cat "$scratch/nuget-port")"

nuget_case() {
  local name="$1"
  mkdir -p "$nuget_root/$name/packages" "$nuget_root/$name/owners"
  printf '%s\n' "$nuget_root/$name"
}

# Publishes a local package into a mock case as nuget.org would serve it,
# optionally replacing one entry to simulate different contents.
publish_to_mock() {
  local case_dir="$1" package="$2" owner="$3" changed="${4:-}"
  CASE_DIR="$case_dir" PACKAGE_PATH="$package" OWNER="$owner" CHANGED="$changed" python3 - <<'PY'
import os
import xml.etree.ElementTree as ET
import zipfile

case, path, owner, changed = (os.environ[name] for name in ('CASE_DIR', 'PACKAGE_PATH', 'OWNER', 'CHANGED'))
with zipfile.ZipFile(path) as source:
    nuspec = next(name for name in source.namelist() if '/' not in name and name.endswith('.nuspec'))
    metadata = {child.tag.rsplit('}', 1)[-1]: child.text for child in ET.fromstring(source.read(nuspec))[0]}
    directory = os.path.join(case, 'packages', metadata['id'].lower())
    os.makedirs(directory, exist_ok=True)
    with zipfile.ZipFile(os.path.join(directory, metadata['version'].lower() + '.nupkg'), 'w') as published:
        for item in source.infolist():
            published.writestr(item, b'changed' if item.filename == changed else source.read(item))
        published.writestr('.signature.p7s', b'repository signature')
with open(os.path.join(case, 'owners', metadata['id'].lower()), 'w', encoding='utf-8') as owners:
    owners.write(owner + '\n')
PY
}

run_nuget() {
  local case_dir="$1"
  shift
  NUGET_TEST_SERVICE_INDEX="http://127.0.0.1:$nuget_port/$(basename "$case_dir")/v3/index.json" \
    MOCK_NUGET_CASE="$case_dir" \
    DOTNET_BIN="$mock_dotnet" \
    GITHUB_REPOSITORY="$repository" \
    GITHUB_SHA="$sha" \
    RELEASE_ALLOW_LOCAL_PUBLISH=1 \
    NUGET_WAIT_TIMEOUT_SECONDS=2 \
    NUGET_WAIT_INTERVAL_SECONDS=0.2 \
    "$@"
}

pushes() {
  if [[ -f "$1/pushes" ]]; then tr '\n' ' ' <"$1/pushes"; fi
}

base_package="$valid/RunicArtifex.DynamicData.$version.nupkg"
reactive_package="$valid/RunicArtifex.DynamicData.Reactive.$version.nupkg"
both="RunicArtifex.DynamicData $version RunicArtifex.DynamicData.Reactive $version "

# New IDs: the dry run reports both and pushes nothing; the push sends both to nuget.org.
new_ids="$(nuget_case new-ids)"
run_nuget "$new_ids" bash "$helper" nuget-check "$valid" >"$scratch/output" 2>&1 ||
  { cat "$scratch/output" >&2; fail 'nuget-check failed for new IDs'; }
grep -qF '2 of 2 packages need a push' "$scratch/output" || fail 'nuget-check did not plan both new packages'
grep -qF 'RunicArtifex.DynamicData is a new nuget.org package ID' "$scratch/output" || fail 'nuget-check did not note a new ID'
[[ -z "$(pushes "$new_ids")" ]] || fail 'nuget-check pushed a package'
expect_failure_with 'push without NuGet key' 'NUGET_API_KEY from NuGet/login is required' \
  run_nuget "$new_ids" bash "$helper" nuget-push "$valid"
expect_failure_with 'wait before indexing' 'does not list both packages yet' \
  run_nuget "$new_ids" bash "$helper" wait-indexed "$version"
run_nuget "$new_ids" env NUGET_API_KEY=test-key bash "$helper" nuget-push "$valid" >"$scratch/output" 2>&1 ||
  { cat "$scratch/output" >&2; fail 'nuget-push failed for new IDs'; }
[[ "$(pushes "$new_ids")" == "$both" ]] || fail "nuget-push did not push both new packages: $(pushes "$new_ids")"
run_nuget "$new_ids" bash "$helper" wait-indexed "$version" >/dev/null || fail 'wait-indexed did not see both packages'
expect_failure_with 'wait-indexed with an invalid version' 'invalid package version' \
  run_nuget "$new_ids" bash "$helper" wait-indexed 'not a version'

# Rerun after success: both identical versions are skipped, nothing is pushed again.
run_nuget "$new_ids" env NUGET_API_KEY=test-key bash "$helper" nuget-push "$valid" >"$scratch/output" 2>&1 ||
  { cat "$scratch/output" >&2; fail 'nuget-push rerun failed'; }
[[ "$(pushes "$new_ids")" == "$both" ]] || fail 'nuget-push rerun pushed again'
grep -qF '0 of 2 packages need a push' "$scratch/output" || fail 'nuget-push rerun did not skip both packages'

# Existing identical version (nuget.org adds only its signature): skipped, the other is pushed.
identical="$(nuget_case identical)"
publish_to_mock "$identical" "$base_package" runic-artifex
run_nuget "$identical" env NUGET_API_KEY=test-key bash "$helper" nuget-push "$valid" >"$scratch/output" 2>&1 ||
  { cat "$scratch/output" >&2; fail 'nuget-push failed with an identical published version'; }
grep -qF "RunicArtifex.DynamicData $version is already on nuget.org with identical contents" "$scratch/output" ||
  fail 'identical published version was not reported as skipped'
[[ "$(pushes "$identical")" == "RunicArtifex.DynamicData.Reactive $version " ]] ||
  fail "identical published version was pushed again: $(pushes "$identical")"

# Existing version with different contents: refused before any push.
different="$(nuget_case different)"
publish_to_mock "$different" "$reactive_package" runic-artifex 'lib/net10.0/DynamicData.Reactive.dll'
expect_failure_with 'different published version' 'already has RunicArtifex.DynamicData.Reactive' \
  run_nuget "$different" env NUGET_API_KEY=test-key bash "$helper" nuget-push "$valid"
grep -qF 'lib/net10.0/DynamicData.Reactive.dll' "$scratch/output" || fail 'different contents did not name the changed entry'
[[ -z "$(pushes "$different")" ]] || fail 'a package was pushed despite different published contents'

# ID owned by another account: refused before any push, with the remedy.
foreign="$(nuget_case foreign)"
mkdir -p "$foreign/packages/runicartifex.dynamicdata.reactive"
cp "$reactive_package" "$foreign/packages/runicartifex.dynamicdata.reactive/1.0.0.nupkg"
printf 'someone-else\n' >"$foreign/owners/runicartifex.dynamicdata.reactive"
expect_failure_with 'ID owned by someone else' 'owned on nuget.org by someone-else, not by runic-artifex' \
  run_nuget "$foreign" env NUGET_API_KEY=test-key bash "$helper" nuget-push "$valid"
grep -qF 'add runic-artifex as an owner' "$scratch/output" || fail 'foreign owner failure did not name the remedy'
[[ -z "$(pushes "$foreign")" ]] || fail 'a package was pushed despite a foreign owner'

# ID that search does not list (all versions unlisted, or not indexed yet): owners unknown, refused.
unlisted="$(nuget_case unlisted)"
mkdir -p "$unlisted/packages/runicartifex.dynamicdata"
cp "$base_package" "$unlisted/packages/runicartifex.dynamicdata/1.0.0.nupkg"
expect_failure_with 'ID with unknown owners' 'its owners cannot be checked' \
  run_nuget "$unlisted" env NUGET_API_KEY=test-key bash "$helper" nuget-push "$valid"
[[ -z "$(pushes "$unlisted")" ]] || fail 'a package was pushed despite unknown owners'

# ID outside the trusted-publishing globs: refused before any push, also in the dry run.
outside_glob="$(nuget_case outside-glob)"
printf '{"owner": "runic-artifex", "packageGlobs": ["Other.*"], "pushNewPackages": true}\n' >"$scratch/other-policy.json"
expect_failure_with 'dry run outside the package glob' 'matches none of the trusted-publishing package globs (Other.*)' \
  run_nuget "$outside_glob" env NUGET_POLICY="$scratch/other-policy.json" bash "$helper" nuget-check "$valid"
expect_failure_with 'push outside the package glob' 'matches none of the trusted-publishing package globs (Other.*)' \
  run_nuget "$outside_glob" env NUGET_POLICY="$scratch/other-policy.json" NUGET_API_KEY=test-key bash "$helper" nuget-push "$valid"
[[ -z "$(pushes "$outside_glob")" ]] || fail 'a package was pushed outside the package glob'

# Retry after a partial push: the second push is refused (403), the rerun skips the first.
partial="$(nuget_case partial)"
expect_failure_with 'refused second push' 'dotnet nuget push failed for RunicArtifex.DynamicData.Reactive' \
  run_nuget "$partial" env NUGET_API_KEY=test-key MOCK_DOTNET_FAIL=RunicArtifex.DynamicData.Reactive bash "$helper" nuget-push "$valid"
[[ "$(pushes "$partial")" == "RunicArtifex.DynamicData $version " ]] || fail 'partial push did not stop after the first package'
run_nuget "$partial" env NUGET_API_KEY=test-key bash "$helper" nuget-push "$valid" >"$scratch/output" 2>&1 ||
  { cat "$scratch/output" >&2; fail 'nuget-push retry failed'; }
[[ "$(pushes "$partial")" == "$both" ]] || fail "nuget-push retry did not push only the missing package: $(pushes "$partial")"

# Test-only overrides are refused in GitHub Actions.
expect_failure_with 'mock service index in Actions' 'only for the offline tests' \
  run_nuget "$new_ids" env GITHUB_ACTIONS=true bash "$helper" nuget-check "$valid"

# Every nuget.org lookup was keyless.
[[ -s "$nuget_root/requests.log" ]] || fail 'mock nuget.org saw no requests'
if grep -v 'credentials=none$' "$nuget_root/requests.log"; then
  fail 'a nuget.org lookup carried a credential'
fi

printf 'release asset checks passed\n'
