#!/usr/bin/env bash
# Idempotently open a fork review PR; a token restriction leaves manual evidence.
set -euo pipefail

usage() {
  echo 'Usage: open-review-pr.sh --repo Runic-Artifex/DynamicData --branch <branch> --base <sha> --upstream <sha> --month YYYY-MM --evidence-dir <directory>' >&2
}

repo=''
branch=''
base=''
upstream=''
month=''
evidence_dir=''
while (($#)); do
  case "$1" in
    --repo|--branch|--base|--upstream|--month|--evidence-dir)
      (($# >= 2)) || { usage; exit 2; }
      case "$1" in
        --repo) repo="$2" ;;
        --branch) branch="$2" ;;
        --base) base="$2" ;;
        --upstream) upstream="$2" ;;
        --month) month="$2" ;;
        --evidence-dir) evidence_dir="$2" ;;
      esac
      shift 2
      ;;
    *) usage; exit 2 ;;
  esac
done
[[ "$repo" == 'Runic-Artifex/DynamicData' ]] || { echo 'PR repository must be Runic-Artifex/DynamicData.' >&2; exit 2; }
[[ "$base" =~ ^[0-9a-fA-F]{40}$ && "$upstream" =~ ^[0-9a-fA-F]{40}$ && "$month" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]] || { usage; exit 2; }
[[ "$branch" =~ ^review/upstream/[0-9]{4}-(0[1-9]|1[0-2])-[0-9a-f]{12}-[0-9a-f]{12}$ ]] || { echo 'Unsafe review branch name.' >&2; exit 2; }
mkdir -p "$evidence_dir"
gh_bin="${GH_BIN:-gh}"
existing="$($gh_bin pr list --repo "$repo" --state open --head "$branch" --base main --json url --jq '.[0].url')"
if [[ -n "$existing" ]]; then
  printf 'status=existing\nurl=%s\n' "$existing"
  exit 0
fi

body="$evidence_dir/pull-request-body.md"
error="$evidence_dir/pull-request-error.txt"
printf '%s\n\n' "Automated preparation for the ${month} upstream review." > "$body"
printf '%s\n' "Pins Runic base \`${base}\` and upstream candidate \`${upstream}\`." >> "$body"
printf '%s\n\n' 'This PR contains an inventory and virtual-merge report only. It does not merge upstream, publish packages, or contact upstream.' >> "$body"
set +e
url="$($gh_bin pr create --repo "$repo" --base main --head "$branch" --title "docs: prepare ${month} upstream review" --body-file "$body" 2>"$error")"
create_status=$?
set -e
if ((create_status == 0)); then
  rm -f "$error" "$evidence_dir/pr-status.md"
  printf 'status=created\nurl=%s\n' "$url"
  exit 0
fi
manual_url="https://github.com/${repo}/compare/main...Runic-Artifex:${branch}?expand=1"
printf '%s\n' \
  '# Pull request creation deferred' \
  '' \
  'The review branch exists but this workflow token could not create its fork PR.' \
  "Open it manually: ${manual_url}" \
  '' \
  "This is commonly caused by the repository's GitHub Actions pull-request" \
  'restriction. The branch and review artifact remain authoritative; a later' \
  'workflow run will retry creation without changing the branch.' \
  > "$evidence_dir/pr-status.md"
printf 'status=deferred\nmanual_url=%s\n' "$manual_url"
