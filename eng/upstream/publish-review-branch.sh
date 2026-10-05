#!/usr/bin/env bash
# Publish a newly generated preparation snapshot without updating an existing branch.
set -euo pipefail

usage() {
  echo 'Usage: publish-review-branch.sh --repo <repo> --base <sha> --branch <review/upstream/YYYY-MM-base-upstream> --snapshot <directory> [--dry-run]' >&2
}

repo=''
base=''
branch=''
snapshot=''
dry_run=false
while (($#)); do
  case "$1" in
    --repo|--base|--branch|--snapshot)
      (($# >= 2)) || { usage; exit 2; }
      case "$1" in
        --repo) repo="$2" ;;
        --base) base="$2" ;;
        --branch) branch="$2" ;;
        --snapshot) snapshot="$2" ;;
      esac
      shift 2
      ;;
    --dry-run) dry_run=true; shift ;;
    *) usage; exit 2 ;;
  esac
done

[[ -n "$repo" && -n "$base" && -n "$branch" && -n "$snapshot" ]] || { usage; exit 2; }
[[ "$base" =~ ^[0-9a-fA-F]{40}$ ]] || { echo 'Base must be a full commit SHA.' >&2; exit 2; }
[[ "$branch" =~ ^review/upstream/[0-9]{4}-(0[1-9]|1[0-2])-[0-9a-f]{12}-[0-9a-f]{12}$ ]] || { echo 'Unsafe review branch name.' >&2; exit 2; }
[[ -f "$snapshot/manifest.json" && -f "$snapshot/report.md" && -f "$snapshot/inventory.json" ]] || { echo 'Snapshot is incomplete.' >&2; exit 2; }

repo="$(cd "$repo" && pwd)"
snapshot="$(cd "$snapshot" && pwd)"
relative_snapshot="${branch#review/upstream/}"
target="eng/upstream/reviews/$relative_snapshot"

if git -C "$repo" ls-remote --exit-code --heads origin "refs/heads/$branch" >/dev/null 2>&1; then
  echo "status=existing"
  echo "branch=$branch"
  exit 0
fi

if $dry_run; then
  echo "status=would-create"
  echo "branch=$branch"
  echo "target=$target"
  exit 0
fi

git -C "$repo" diff --quiet || { echo 'Refusing to publish from a dirty worktree.' >&2; exit 1; }
[[ "$(git -C "$repo" rev-parse HEAD)" == "$base" ]] || { echo 'Checkout no longer matches the recorded base.' >&2; exit 1; }
git -C "$repo" switch --create "$branch" "$base"
mkdir -p "$repo/$(dirname "$target")"
[[ ! -e "$repo/$target" ]] || { echo 'Snapshot destination already exists.' >&2; exit 1; }
cp -a "$snapshot" "$repo/$target"
git -C "$repo" add -- "$target"
git -C "$repo" commit -m "docs: prepare ${branch#review/upstream/} upstream review"
git -C "$repo" push origin "HEAD:refs/heads/$branch"
echo "status=created"
echo "branch=$branch"
echo "target=$target"
