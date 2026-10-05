#!/usr/bin/env bash
set -euo pipefail

state_dir="${MOCK_GH_STATE:?MOCK_GH_STATE is required}"
fixture_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../fixtures/ci" && pwd)"
mkdir -p "$state_dir/assets"

http_missing() {
  printf 'HTTP/2 404 Not Found\n\n{"message":"Not Found"}\n' >&2
  exit 1
}

ci_fixture() {
  local section="$1"
  local case_name="${MOCK_GH_CI_CASE:-successful-retry}"
  local fixture="$fixture_dir/$case_name.json"
  [[ -f "$fixture" ]] || exit 1
  python3 - "$fixture" "$section" "${GITHUB_SHA:?GITHUB_SHA is required}" <<'PY'
import json
import sys

path, section, sha = sys.argv[1:]
with open(path, encoding='utf-8') as source:
    payload = json.load(source)
text = json.dumps(payload[section]).replace('@SHA@', sha)
key = 'workflow_runs' if section == 'runs' else 'jobs'
print(json.dumps([{key: json.loads(text)}]))
PY
}

if [[ "$1" == 'api' ]]; then
  endpoint=''
  for argument in "$@"; do
    if [[ "$argument" == repos/* ]]; then
      endpoint="$argument"
      break
    fi
  done
  [[ -n "$endpoint" ]] || exit 1
  if [[ "$endpoint" == *'/actions/workflows/ci-build.yml/runs?'* ]]; then
    ci_fixture runs
    exit 0
  fi
  if [[ "$endpoint" == *'/actions/runs/'*'/jobs?'* ]]; then
    ci_fixture jobs
    exit 0
  fi
  if [[ "$endpoint" == repos/*'/releases?per_page=100' ]]; then
    if [[ "${MOCK_GH_RELEASE_STATE:-missing}" == 'existing' ]]; then
      printf '[[{"tag_name":"v10.0.0-runic.test"}]]\n'
    else
      printf '[[]]\n'
    fi
    exit 0
  fi
  if [[ "$endpoint" =~ ^repos/[^/]+/[^/]+$ ]]; then
    if [[ " $* " == *' --jq '* ]]; then
      printf 'main\n'
    else
      printf '{"default_branch":"main"}\n'
    fi
    exit 0
  fi
  if [[ "$endpoint" == *'/git/ref/tags/'* ]]; then
    if [[ "${MOCK_GH_TAG_STATE:-missing}" == 'existing' ]]; then
      printf 'HTTP/2 200 OK\n\n{"object":{"sha":"1111111111111111111111111111111111111111"}}\n'
      exit 0
    fi
    if [[ -f "$state_dir/tag" ]]; then
      target="$(cat "$state_dir/tag")"
      printf '{"object":{"type":"commit","sha":"%s"}}\n' "$target"
      exit 0
    fi
    if [[ "${MOCK_GH_TAG_STATE:-missing}" == 'forbidden' ]]; then
      printf 'HTTP/2 403 Forbidden\n\n{"message":"forbidden"}\n' >&2
      exit 1
    fi
    http_missing
  fi
  if [[ "$endpoint" == *'/releases/1' && " $* " != *' --method '* ]]; then
    if [[ ! -f "$state_dir/release" ]]; then
      http_missing
    fi
    python3 - "$state_dir" <<'PY'
import json
import os
import sys

state = sys.argv[1]
with open(os.path.join(state, 'release'), encoding='utf-8') as source:
    target = source.read().strip()
if os.environ.get('MOCK_GH_DRAFT_STATE') == 'wrong-target':
    target = '1111111111111111111111111111111111111111'
assets = []
for index, name in enumerate(sorted(os.listdir(os.path.join(state, 'assets'))), start=1):
    if os.environ.get('MOCK_GH_DRAFT_STATE') == 'missing-assets' and index > 1:
        continue
    path = os.path.join(state, 'assets', name)
    assets.append({'id': index, 'name': name, 'size': os.path.getsize(path)})
print(json.dumps({'draft': True, 'prerelease': True, 'target_commitish': target, 'assets': assets}))
PY
    exit 0
  fi
  if [[ "$endpoint" == *'/git/refs' && " $* " == *' --method POST '* ]]; then
    if [[ -f "$state_dir/tag" || "${MOCK_GH_TAG_STATE:-missing}" == 'existing' ]]; then
      printf 'HTTP/2 422 Unprocessable Entity\n\n{"message":"Reference already exists"}\n' >&2
      exit 1
    fi
    for argument in "$@"; do
      if [[ "$argument" == sha=* ]]; then
        printf '%s\n' "${argument#sha=}" >"$state_dir/tag"
      fi
    done
    [[ -f "$state_dir/tag" ]] || exit 1
    exit 0
  fi
  if [[ "$endpoint" == *'/releases/assets/'* ]]; then
    asset_id="${endpoint##*/}"
    mapfile -t assets < <(find "$state_dir/assets" -maxdepth 1 -type f -printf '%f\n' | sort)
    index=$((asset_id - 1))
    [[ "$index" -ge 0 && "$index" -lt "${#assets[@]}" ]] || exit 1
    cat "$state_dir/assets/${assets[$index]}"
    exit 0
  fi
  if [[ "$endpoint" == *'/releases' && " $* " == *' --method POST '* ]]; then
    target=''
    for argument in "$@"; do
      if [[ "$argument" == target_commitish=* ]]; then
        target="${argument#target_commitish=}"
      fi
    done
    [[ -n "$target" ]] || exit 1
    printf '%s\n' "$target" >"$state_dir/release"
    printf '{"id":1,"draft":true,"prerelease":true,"target_commitish":"%s"}\n' "$target"
    exit 0
  fi
  if [[ "$endpoint" == *'/releases/1/assets?name='* ]]; then
    printf 'asset uploads must not use the default API host\n' >&2
    exit 1
  fi
  if [[ "$endpoint" == *'/releases/1' && " $* " == *' --method PATCH '* ]]; then
    [[ -f "$state_dir/tag" ]] || exit 1
    touch "$state_dir/published"
    exit 0
  fi
  printf 'unexpected gh api endpoint: %s\n' "$endpoint" >&2
  exit 1
fi

if [[ "$1" == 'release' && "$2" == 'upload' ]]; then
  [[ -f "$state_dir/release" ]] || exit 1
  for argument in "$@"; do
    if [[ "$argument" == *.nupkg ]]; then
      cp "$argument" "$state_dir/assets/$(basename "$argument")"
    fi
  done
  [[ "$(find "$state_dir/assets" -maxdepth 1 -type f -name '*.nupkg' | wc -l)" == '2' ]] || exit 1
  exit 0
fi

printf 'unexpected gh invocation: %q\n' "$*" >&2
exit 1
