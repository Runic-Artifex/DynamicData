#!/usr/bin/env bash
set -euo pipefail

readonly root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly helper="$root/eng/release/release-assets.sh"
readonly fixtures="$root/eng/release/fixtures"
readonly mock_gh="$root/eng/release/tests/mock-gh.sh"
readonly repository='Runic-Artifex/DynamicData'
readonly sha='0123456789abcdef0123456789abcdef01234567'
readonly version='10.0.0-runic.test'
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT

fail() {
  printf 'test-release-assets: %s\n' "$*" >&2
  exit 1
}

write_nuspec() {
  local fixture="$1"
  local output="$2"
  sed \
    -e "s/@VERSION@/$version/g" \
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
  write_nuspec "$fixture" "$stage/$package_id.nuspec"
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
  local base_fixture="${2:-$fixtures/Runic.DynamicData.nuspec}"
  local reactive_fixture="${3:-$fixtures/Runic.DynamicData.Reactive.nuspec}"
  local reactive_version="${4:-$version}"
  mkdir -p "$destination"
  make_package "$destination" 'Runic.DynamicData' "$version" "$base_fixture"
  make_package "$destination" 'Runic.DynamicData.Reactive' "$reactive_version" "$reactive_fixture"
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

invalid_version="$scratch/invalid-version"
mkdir -p "$invalid_version"
make_package "$invalid_version" 'Runic.DynamicData' 'not-a-version' "$fixtures/Runic.DynamicData.nuspec"
make_package "$invalid_version" 'Runic.DynamicData.Reactive' 'not-a-version' "$fixtures/Runic.DynamicData.Reactive.nuspec"
expect_failure 'malformed package version' validate "$invalid_version"

wrong_target="$scratch/wrong-target"
make_pair "$wrong_target"
append_package_entry "$wrong_target/Runic.DynamicData.$version.nupkg" 'lib/net9.0/DynamicData.dll'
expect_failure 'additional library target' validate "$wrong_target"

wrong_license_payload="$scratch/wrong-license-payload"
make_pair "$wrong_license_payload"
append_package_entry "$wrong_license_payload/Runic.DynamicData.$version.nupkg" 'LICENSE/LICENSE'
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

for malformed_draft in wrong-target missing-assets; do
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

printf 'release asset checks passed\n'
