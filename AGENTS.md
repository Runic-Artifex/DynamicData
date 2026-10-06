# Working on Runic DynamicData

Read [CONTRIBUTING.md](CONTRIBUTING.md), the [maintenance guide](docs/maintenance.md)
and the [fork difference register](docs/fork-differences.md) before changing code.
Open upstream-derived work is tracked as
[GitHub issues](https://github.com/Runic-Artifex/DynamicData/issues); verify an
issue against the current code before acting on it.

## Preserve the fork

- Keep Git history and upstream merge ancestry; upstream syncs are real merges
  of a pinned upstream commit (see the maintenance guide).
- Keep .NET 10-only targets, the SDK pin in `global.json`, default Primitives
  9.0.0 with 8.4.0 compatibility, both shared-source flavors (`REACTIVE_SHIM`),
  TUnit and the branded `Runic.*` package IDs. Do not inherit enclosing
  workspace central package versions.
- Keep the original MIT license and authorship.
- Use small logical commits with meaningful regression tests in both flavors.
  Review public API baseline changes. Update the register when a difference is
  added, changed or retired.
- Published versions, tags and release assets are immutable.
- Do not open issues, PRs or comments upstream.

## Environment and checks

From the repository root, use the SDK workspace's locked shell:
`direnv exec <path-to>/runic-sdk <command>` (inspect its `.envrc` and
`direnv status` first; do not install duplicate SDKs or use `use flake path:.`).
Build and test commands are in the maintenance guide.

Run focused tests while iterating and the full matrix once ready. Use virtual
time and explicit synchronization; keep meaningful contention in concurrency
regressions. Check disk space before expensive verification; on ENOSPC stop and
diagnose storage before retrying. Remove only task-owned temporary outputs.
