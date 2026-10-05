#!/usr/bin/env bash
# Publish a newly generated preparation snapshot without updating an existing branch.
set -euo pipefail

usage() {
  echo 'Usage: publish-review-branch.sh --repo <repo> --base <sha> --upstream <sha> --branch <review/upstream/YYYY-MM-base-upstream> --snapshot <directory> [--dry-run]' >&2
}

repo=''
base=''
upstream=''
branch=''
snapshot=''
dry_run=false
while (($#)); do
  case "$1" in
    --repo|--base|--upstream|--branch|--snapshot)
      (($# >= 2)) || { usage; exit 2; }
      case "$1" in
        --repo) repo="$2" ;;
        --base) base="$2" ;;
        --upstream) upstream="$2" ;;
        --branch) branch="$2" ;;
        --snapshot) snapshot="$2" ;;
      esac
      shift 2
      ;;
    --dry-run) dry_run=true; shift ;;
    *) usage; exit 2 ;;
  esac
done

[[ -n "$repo" && -n "$base" && -n "$upstream" && -n "$branch" && -n "$snapshot" ]] || { usage; exit 2; }
[[ "$base" =~ ^[0-9a-fA-F]{40}$ ]] || { echo 'Base must be a full commit SHA.' >&2; exit 2; }
[[ "$upstream" =~ ^[0-9a-fA-F]{40}$ ]] || { echo 'Upstream must be a full commit SHA.' >&2; exit 2; }
[[ "$branch" =~ ^review/upstream/[0-9]{4}-(0[1-9]|1[0-2])-[0-9a-f]{12}-[0-9a-f]{12}$ ]] || { echo 'Unsafe review branch name.' >&2; exit 2; }
[[ -f "$snapshot/manifest.json" && -f "$snapshot/report.md" && -f "$snapshot/inventory.json" ]] || { echo 'Snapshot is incomplete.' >&2; exit 2; }

repo="$(cd "$repo" && pwd)"
snapshot="$(cd "$snapshot" && pwd)"
git_bin="${GIT_BIN:-git}"
relative_snapshot="${branch#review/upstream/}"
target="eng/upstream/reviews/$relative_snapshot"
origin="$("$git_bin" -C "$repo" remote get-url origin)"
[[ "$origin" =~ ^(https://github\.com/|git@github\.com:)Runic-Artifex/DynamicData(\.git)?$ ]] || { echo "Origin is not Runic-Artifex/DynamicData: $origin" >&2; exit 1; }
[[ -z "$("$git_bin" -C "$repo" status --porcelain)" ]] || { echo 'Refusing to publish from a dirty worktree.' >&2; exit 1; }

validate_manifest() {
  node - "$base" "$upstream" "$1" <<'NODE'
const { readFileSync } = require('node:fs');
const [base, upstream, file] = process.argv.slice(2);
const manifest = JSON.parse(readFileSync(file, 'utf8'));
if (manifest.base !== base.toLowerCase() || manifest.upstream !== upstream.toLowerCase()) {
  throw new Error('Snapshot manifest pins do not match publication arguments.');
}
NODE
}
validate_manifest "$snapshot/manifest.json"

set +e
"$git_bin" -C "$repo" ls-remote --exit-code --heads origin "refs/heads/$branch" >/dev/null 2>&1
remote_status=$?
set -e
if ((remote_status == 0)); then
  "$git_bin" -C "$repo" fetch --no-tags origin "refs/heads/$branch:refs/remotes/origin/$branch" >&2
  existing_manifest="$(mktemp)"
  trap 'rm -f "$existing_manifest"' EXIT
  "$git_bin" -C "$repo" show "refs/remotes/origin/$branch:$target/manifest.json" > "$existing_manifest"
  validate_manifest "$existing_manifest"
  echo "status=existing"
  echo "branch=$branch"
  exit 0
fi
if ((remote_status != 2)); then
  echo "Unable to determine whether review branch exists (git ls-remote exit $remote_status)." >&2
  exit "$remote_status"
fi

if $dry_run; then
  echo "status=would-create"
  echo "branch=$branch"
  echo "target=$target"
  exit 0
fi

[[ "$("$git_bin" -C "$repo" rev-parse HEAD)" == "$base" ]] || { echo 'Checkout no longer matches the recorded base.' >&2; exit 1; }
"$git_bin" -C "$repo" switch --create "$branch" "$base" >&2
mkdir -p "$repo/$(dirname "$target")"
[[ ! -e "$repo/$target" ]] || { echo 'Snapshot destination already exists.' >&2; exit 1; }
cp -a "$snapshot" "$repo/$target"
"$git_bin" -C "$repo" add -- "$target"
"$git_bin" -C "$repo" -c user.name='github-actions[bot]' -c user.email='41898282+github-actions[bot]@users.noreply.github.com' \
  commit -m "docs: prepare ${branch#review/upstream/} upstream review" >&2
"$git_bin" -C "$repo" push origin "HEAD:refs/heads/$branch" >&2
echo "status=created"
echo "branch=$branch"
echo "target=$target"
