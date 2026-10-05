# Maintaining the Runic DynamicData fork

Agreed policy, recorded **2026-10-05**. This document governs maintenance; the
[upstream investigation](upstream/README.md) is dated evidence and a proposed
implementation sequence, not a live roadmap or proof that work was completed.
The [fork difference register](fork-differences.md) records intentional behavior
and adaptations that must survive integration.

## Repository and integration ownership

Keep this repository and its existing upstream and Runic Git history. Runic
`main` is the maintained integration branch; topic branches carry small logical
changes with their regressions. On 2026-10-05, Runic adopted the existing fork
line at `edd2d175` and published maintenance documentation at `5887123c` on
`main`, preserving its upstream and Runic ancestry. The
[October integration review](upstream/reviews/2026-10.md) records the subsequent
pinned upstream merge, topic adaptations and their actual validation status.
Integrate future tested lines without rewriting published history.

One integration owner controls the target branch, pins upstream inputs, orders
dependent topics, reviews conflicts and test evidence, and integrates/releases
the result. Delegates may implement independent topics in separate worktrees
with explicit file ownership. Only the integration owner changes shared
integration state or publishes, unless the user explicitly delegates that
responsibility. Repository [agent instructions](../AGENTS.md) authorize GPT 6.1
Sol delegates for this work.

Keep upstream merge ancestry. Do not squash upstream syncs, rebase away inherited
upstream commits, replace the repository with a submodule wrapper, or reconstruct
the fork wholesale as a patch stack. Exported patches may be derived review or
distribution artifacts; the repository history and register remain authoritative.
Preserve MIT licensing and original authorship. This fork is maintained for
Runic; do not submit issues, PRs or other contributions upstream as part of this
workflow.

## Monthly upstream review

Review upstream once a month, and sooner for an urgent correctness or security
fix. The scheduled [upstream-review workflow](../.github/workflows/upstream-review.yml)
prepares an immutable, unassessed fork review branch and artifact on the first
of each month; it does not merge, publish, retag, or contact upstream. It pins
both the Runic base and an upstream commit, calculates conflicts without
checking out a merge, and writes a new `eng/upstream/reviews/` snapshot. The
branch name includes the month and both pins, so reruns never overwrite an
existing review. A manual run may select only a full SHA reachable from fetched
`upstream/main`. Review branches and their PRs are maintenance evidence, not
approval to integrate. Historical `docs/upstream/` assessments are intentionally
not copied into a new snapshot: every item must be assessed against that
month's pins. This workflow uses Git objects and the GitHub REST API only; it
does not run upstream workflows, build scripts, or project code. The current
fork setting may restrict GitHub Actions from creating pull requests. In that
case the workflow retains its immutable branch and artifact, writes a manual
compare link in `pr-status.md`, and retries PR creation on the next run without
changing the branch. Do not loosen repository settings merely to make this
automation succeed.

1. Start from the current Runic `main` in a clean checkout or isolated worktree.
   Preserve unrelated work. Record the Runic base SHA and review date.
2. Fetch `upstream`, inspect commits since the previous integrated upstream
   point, and choose one exact upstream commit. Prefer a suitable tested release
   or reviewed main commit; never integrate a moving branch without recording
   the pinned SHA. Read relevant changes and their dependencies, including
   changes already ported selectively.
3. Create a temporary `sync/upstream/YYYY-MM` branch from Runic `main`. Merge the
   pinned upstream commit with a real Git merge. Keep both parent histories and
   review all resulting differences, including cleanly merged build, dependency,
   test and workflow changes. Adapt those changes to the fork's policy.
4. Resolve conflicts deliberately, then keep follow-up behavioral adaptations in
   small topic commits with regressions. Preserve both flavors, dependency
   generations, public API policy, package identity and retained fixes. Update
   the difference register in the same integration that changes a difference.
5. Run the validation below. Record accepted/deferred changes, conflict decisions,
   exact tested SHAs, results and any failures in
   `docs/upstream/reviews/YYYY-MM.md`. A no-change review still records its pinned
   input and reason for deferral. Research catalogs alone do not record adoption.
6. The integration owner integrates the tested sync branch into Runic `main`
   without squashing. If `main` moved meanwhile, merge it into the sync branch,
   review the new result and rerun affected checks before integration. A
   fast-forward from the unchanged base preserves the upstream merge commit.
   Remove the temporary branch/worktree after integration and retained evidence.

Example commands below assume Runic `main` has already adopted the maintained
fork and that the checkout is clean. Replace the month with the actual review.
Do not run them over uncommitted work.

```sh
git fetch upstream
runic_base=$(git rev-parse main)
upstream_commit=$(git rev-parse upstream/main)
git switch -c sync/upstream/2026-10 "$runic_base"
git merge --no-ff --no-commit "$upstream_commit"
# Inspect, resolve and validate the merged tree before recording the merge.
# Record both SHAs, adaptations and validation in the monthly review.
```

Use `git merge-base --is-ancestor <pinned-upstream-sha> <tested-sync-sha>` to
verify the sync retained the selected upstream ancestry. Do not describe a
selective port as a full upstream sync.

## Urgent and topic changes

An urgent fix may be ported ahead of the monthly merge. Pin its source commit
and prerequisite commits; use `git cherry-pick -x` when it applies directly, or
record the original commit/PR in an adapted commit's message. Include a focused
regression and add/update its register entry. During the next merge, compare the
port with upstream's actual implementation and remove redundant adaptation only
after behavior is verified.

Use one logical behavior or operator group per topic commit. Separate mechanical
changes, public contract changes and broad refactors when they can be reviewed
independently. Translate upstream test APIs to awaited TUnit assertions instead
of introducing another runner. Pin completion, errors, subscription ownership,
disposal and ordering before overlapping queue/lifecycle refactors. Performance
changes require a relevant benchmark and correctness replay; source ancestry or
upstream test claims do not replace local validation.

## Conflict resolution and rerere

Use Git's recorded conflict resolution as a reviewed aid:

```sh
git config --local rerere.enabled true
git config --local rerere.autoupdate false
```

Inspect `git rerere diff` and the resulting source after each reused resolution;
stage it only after review. A remembered resolution can be syntactically valid
and behaviorally wrong when either side changed. Use `git rerere forget <path>`
for a stale resolution, resolve it again, and test the affected behavior in both
flavors. Document meaningful resolution decisions in the monthly review and
register. Do not use automatic staging as evidence of correctness.

## Validation and development environment

Follow [CONTRIBUTING](../CONTRIBUTING.md), `global.json`, the current project
files and CI matrix. On the Runic NixOS desktop, inspect `../runic-sdk/.envrc`,
its `flake.nix` and `direnv status`, then reuse its locked environment with
`direnv exec ../runic-sdk <command>`. Do not install another SDK or reconstruct
the shell from store paths. If that SDK workspace or its toolchain pin differs,
resolve the environment mismatch before claiming validation. Do not accept a
last-working-shell fallback as validation of changed Nix inputs.

Run focused checks while iterating, then build and run both test executables for
each supported Primitives version. The commands below run from this repository:

```sh
direnv exec ../runic-sdk dotnet build src/DynamicData.sln -c Release -p:ReactiveUIPrimitivesVersion=8.4.0
direnv exec ../runic-sdk dotnet src/DynamicData.Tests/bin/Release/net10.0/DynamicData.Tests.dll --maximum-parallel-tests 8
direnv exec ../runic-sdk dotnet src/DynamicData.Reactive.Tests/bin/Release/net10.0/DynamicData.Reactive.Tests.dll --maximum-parallel-tests 8
direnv exec ../runic-sdk dotnet build src/DynamicData.sln -c Release -p:ReactiveUIPrimitivesVersion=9.0.0
direnv exec ../runic-sdk dotnet src/DynamicData.Tests/bin/Release/net10.0/DynamicData.Tests.dll --maximum-parallel-tests 8
direnv exec ../runic-sdk dotnet src/DynamicData.Reactive.Tests/bin/Release/net10.0/DynamicData.Reactive.Tests.dll --maximum-parallel-tests 8
direnv exec ../runic-sdk dotnet pack src/DynamicData.sln -c Release --no-build -o artifacts/packages
```

Build immediately before running each version's binaries; outputs share paths
and cannot establish both versions if only one build was run. Finish with 9.0.0
before packing the default packages. Require the Linux/Windows CI matrix at the
tested final revision. Review both .NET 10 API baselines for intentional changes;
compile representative external consumers for overload/namespace changes.
Check package IDs and contents before release.

The [packaged acceptance gate](../eng/acceptance/README.md) exercises the actual
default 9.0.0 branded package pair outside the library namespaces. CI runs both
flavors managed on Linux and Windows, then Linux NativeAOT after both managed
jobs succeed. Require exact artifact hashes, repository SHA and the expected
restore graph. These three headless application workflows cover presentation
updates, child-session recovery and view-close ownership; they do not establish
actual Terra UI behavior or all-operator/platform NativeAOT support. Source
8.4.0 compatibility remains a separate matrix: never force a shipping package
below its declared Primitives minimum or suppress NU1605 to claim packaged
compatibility.

Before a large local matrix, check project and temporary filesystem free space
and container storage if used. Use conservative concurrency, reuse valid caches
and build outputs, and avoid overlapping full runs. On storage failure, stop
launching work and diagnose the measured filesystem. Remove only task-owned
disposable outputs; retain useful failure reports and shared caches. Do not
delete Nix store paths or broadly prune system storage.

## Releases and retirement

Release a tested commit with a unique version and tag. Published tags and package
assets are immutable: do not retag, replace an existing version's assets or
repack different code under the same version. Corrections receive a new version.
Record the source SHA, dependency generation and validation in release notes;
keep `Runic.DynamicData` and `Runic.DynamicData.Reactive` package IDs. The current
manual [release workflow](../.github/workflows/release.yml) creates prerelease
GitHub assets from a clean maintained `main` checkout at its exact workflow
SHA. The [release guard](../eng/release/README.md) requires the latest trusted
main Build run at that same SHA to complete successfully, including every
source matrix cell and the packaged acceptance jobs. It validates exactly one
matching default 9.0.0 package per branded ID, .NET 10 contents, original MIT
attribution and repository source metadata. It refuses an existing release or
tag, uploads a draft pair, downloads and compares the exact bytes, creates and
resolves a new tag at the tested SHA, then publishes. Promoting an already
verified CI pair follows the same source, version, byte and tag requirements;
record that provenance instead of rebuilding or replacing published assets.
This process is not a claim of stable/NuGet.org publication. GitHub server
immutability may be disabled; the no-replacement policy still applies.
Upstream's inherited `RELEASING.md` describes upstream automation, not this
fork's process.

Retire a difference only when its register condition is met. Record the upstream
replacement SHA or Runic policy decision, the removal commit and validation;
retain the entry as history with status `retired`. Keep useful regressions even
when the upstream implementation replaces a local adaptation.
