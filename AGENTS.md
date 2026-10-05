# Working on Runic DynamicData

Read [CONTRIBUTING.md](CONTRIBUTING.md), the
[maintenance policy](docs/maintenance.md) and the
[fork difference register](docs/fork-differences.md) before implementation.
The files under `docs/upstream/` are dated research, not live completion or
planning state. Verify a claim against the actual branch before acting on it.

## Ownership and delegation

GPT 6.1 Sol (`gpt-6.1-sol`) delegates are explicitly authorized for independent
implementation, research and review topics. Follow any narrower instruction
from the user. Do not use delegate models older than GPT 6. The integration owner
assigns exclusive files or separate worktrees, pins source/dependency commits,
orders dependent work and reviews every result. Delegates must report changed
files, validation and outstanding limitations, and preserve existing user work.
Only the integration owner merges into the maintained Runic `main`, commits
shared integration results or publishes unless the user explicitly delegates
those actions. Authorization to delegate does not authorize upstream messages
or contributions.

## Preserve the fork

- Keep existing Git history and upstream merge ancestry. Monthly syncs use a
  pinned upstream commit on a temporary sync branch and a reviewed real merge.
  Do not squash syncs, reconstruct the repository as a patch stack or introduce
  a submodule wrapper. Optional patch exports are derived artifacts.
- Keep .NET 10-only targets, the SDK pin in `global.json`, default Primitives
  9.0.0 and compatibility checks for 8.4.0. Preserve both shared-source flavors,
  `REACTIVE_SHIM`, TUnit and branded Runic package IDs. Do not inherit enclosing
  workspace central package versions.
- Protect expiration-disposal guards and recent targeted operator fixes listed
  in the register. Resolve reused `rerere` resolutions by review and testing.
- Use small logical topic commits with meaningful regression tests. Review API
  changes and both flavor baselines. Update the register when a difference is
  added, changed or retired; preserve retired entries with evidence.
- Published versions, tags and release assets are immutable. No upstream issues,
  PRs, comments or contributions are part of this work.

## Environment and checks

Read the SDK workspace's `flake.nix` and `.envrc` and inspect `direnv status`.
From this repository, use `direnv exec ../runic-sdk <command>` to reuse the locked
Nix environment when it is not already loaded. Keep `use flake` Git-aware;
do not replace it with `use flake path:.`, guess store paths or install duplicate
SDKs. Resolve pin mismatches and last-working-shell fallback before claiming
validation. See the maintenance policy for build/test commands and CI gates.

Run focused tests during iteration and the required final matrix once ready.
Use explicit synchronization and virtual time; keep meaningful contention in
concurrency regressions. Check disk space before expensive verification. On
ENOSPC, stop launching work and diagnose storage before retrying. Remove only
task-owned disposable artifacts, preserve shared caches and user work, and never
manually delete Nix store paths or broadly prune system storage.
