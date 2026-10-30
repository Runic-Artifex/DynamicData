# Maintaining the Runic DynamicData fork

Runic `main` is the maintained branch. Changes land through small, logical topic
branches with regression tests. Intentional differences from upstream are listed
in the [fork difference register](fork-differences.md); open upstream-derived
gaps are tracked as [fork issues](https://github.com/Runic-Artifex/DynamicData/issues).
This fork is maintained for Runic: no issues, PRs or other contributions go
upstream as part of this work.

## History and upstream syncs

- Keep the existing Git history and upstream merge ancestry. Sync with a real
  `git merge` of a pinned upstream commit; never squash a sync, rebase away
  upstream commits, rebuild the fork as a patch stack or wrap it in a submodule.
- Keep the original MIT license and authorship.

To sync, branch from `main`, merge the chosen upstream commit, review every
resulting change (including cleanly merged build, dependency, test and workflow
changes), resolve conflicts, validate, and merge the branch into `main` without
squashing:

```sh
git fetch upstream
git switch -c sync/upstream/YYYY-MM main
git merge --no-ff --no-commit <upstream-sha>
# Review and resolve, then run the validation below before committing.
```

Describe adopted and deferred upstream changes and conflict decisions in the
sync pull request. Update the register in the same change when a difference is
added, changed or retired. `git merge-base --is-ancestor <upstream-sha> HEAD`
confirms the sync kept upstream ancestry; a selective port is not a sync.

Urgent fixes may be ported ahead of a sync with `git cherry-pick -x` (or by
citing the upstream commit/PR in an adapted commit) plus a focused regression.
At the next sync, compare with upstream's implementation and drop redundant
adaptations only after the regression passes.

`git rerere` (`rerere.enabled true`, `rerere.autoupdate false`) may help with
repeated conflicts; review every reused resolution with `git rerere diff` and
test it in both flavors.

## Monthly upstream review

The [upstream review workflow](../.github/workflows/upstream-review.yml) runs on
the first of each month and on demand (optionally pinned to an upstream SHA). It:

1. fetches upstream `main` and collects the upstream issue/PR inventory
   ([eng/upstream](../eng/upstream));
2. compares it with the previous `upstream-review` issue, or with the fork's
   upstream baseline (the upstream parent of the newest upstream merge) for the
   first review;
3. uploads `inventory.json`, `manifest.json` and `summary.md` as the
   `upstream-review-YYYY-MM-<run>` workflow artifact;
4. opens or updates the `Upstream review YYYY-MM` issue with the summary: counts,
   new or updated upstream items, upstream commits since the baseline and
   whether a merge would conflict.

It commits nothing, pushes no branches and never merges upstream. To review,
read the issue, file or update fork issues for upstream work worth adopting,
start a sync if warranted, and close the review issue. Run the workflow's
fixtures with:

```sh
node eng/upstream/test-collect-inventory.mjs
node eng/upstream/test-prepare-review.mjs
node eng/upstream/test-review-issue.mjs
```

## Build and validation

On the Runic desktop, run commands through the SDK workspace's locked shell from
the repository root: `direnv exec <path-to>/runic-sdk <command>`. Build
immediately before running each Primitives version's tests, because both builds
share output paths, and finish with 9.0.0 before packing:

```sh
dotnet build src/DynamicData.sln -c Release -p:ReactiveUIPrimitivesVersion=8.4.0
dotnet src/DynamicData.Tests/bin/Release/net10.0/DynamicData.Tests.dll --maximum-parallel-tests 8
dotnet src/DynamicData.Reactive.Tests/bin/Release/net10.0/DynamicData.Reactive.Tests.dll --maximum-parallel-tests 8
dotnet build src/DynamicData.sln -c Release -p:ReactiveUIPrimitivesVersion=9.0.0
dotnet src/DynamicData.Tests/bin/Release/net10.0/DynamicData.Tests.dll --maximum-parallel-tests 8
dotnet src/DynamicData.Reactive.Tests/bin/Release/net10.0/DynamicData.Reactive.Tests.dll --maximum-parallel-tests 8
dotnet pack src/DynamicData.sln -c Release --no-build -o artifacts/packages
```

Review both .NET 10 public API baselines when the surface changes. CI
([ci-build.yml](../.github/workflows/ci-build.yml)) runs this matrix on Linux and
Windows, then the [packaged acceptance gate](../eng/acceptance/README.md)
(managed on both, NativeAOT on Linux). The [external consumer probes](../eng/verification/README.md)
cover overload resolution and NativeAOT outside the library namespaces. Check
free disk space before large local matrices.

## Releases

The manual [release workflow](../.github/workflows/release.yml) builds, tests and
packs `main`, then the [release guard](../eng/release/README.md) requires a green
Build run at the same SHA and validates the `RunicArtifex.DynamicData` and
`RunicArtifex.DynamicData.Reactive` pair. It creates a GitHub release (a
prerelease for prerelease versions) with those assets and the release notes, then pushes the pair to nuget.org with
trusted publishing from the protected `nuget` environment, and finally waits up
to an hour until nuget.org lists both versions. Versions come from NBGV
(`version.json`) on `main`: a prerelease line such as `10.0-runic.{height}`
gives `10.0.0-runic.N`, and a stable release sets an exact version such as
`10.0.0` for the one release commit; the next commit moves `main` to the next
prerelease line (`10.1-runic.{height}` after `10.0.0`), so later pushes never
produce the released version again. `10.0.0` was the first stable release;
`main` now produces `10.1.0-runic.N`. Earlier previews were GitHub
release assets only, under the IDs `Runic.DynamicData` and
`Runic.DynamicData.Reactive`.

1. Wait for a green Build run of the `main` commit.
2. Run **Package release** on `main` with `dry-run` checked (the default). It
   builds, tests, packs and runs every check, including keyless nuget.org lookups
   for the package globs, ID ownership and already published versions, without
   creating the release or pushing.
3. Run it again with `dry-run` unchecked and approve the `nuget` environment.
4. Wait for the `indexed` job before depending on the new version.

If the `nuget` job fails, use **Re-run failed jobs** in the same run; it skips
versions nuget.org already has with identical contents and refuses changed ones.
See the [release guide](../eng/release/README.md#retries) for retries and for the
one-time [nuget.org account setup](../eng/release/README.md#nugetorg-account-setup):
the trusted publishing policy for `Runic-Artifex`/`DynamicData`, workflow
`release.yml`, environment `nuget`; the `NUGET_USER` repository variable; the
`nuget` environment protection; and the optional `RunicArtifex.*` prefix
reservation. Keep [`nuget-policy.json`](../eng/release/nuget-policy.json) in step
with the nuget.org policy.

Published versions, tags and assets are immutable: never retag or
replace them; a correction gets a new version. Upstream's `RELEASING.md` does not
apply to this fork.
