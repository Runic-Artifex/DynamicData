# Release guard and nuget.org publishing

`release-assets.sh` is the narrow gate used by the manual package-release
workflow ([`release.yml`](../../.github/workflows/release.yml)). It accepts only
the two default `9.0.0` branded packages:

- `RunicArtifex.DynamicData.<version>.nupkg`, with one `ReactiveUI.Primitives`
  dependency declared at `9.0.0` (NuGet's minimum `9.0.0`).
- `RunicArtifex.DynamicData.Reactive.<version>.nupkg`, with one
  `ReactiveUI.Primitives.Reactive` dependency declared at `9.0.0` (NuGet's
  minimum `9.0.0`).

Earlier previews were published only as GitHub release assets, under the IDs
`Runic.DynamicData` and `Runic.DynamicData.Reactive`. Those assets stay as they
are; nuget.org has only the `RunicArtifex.*` IDs.

## Package checks

The pair must have the same version, a `net10.0` dependency group, MIT license,
and repository URL and commit matching the current workflow repository and
`GITHUB_SHA`. The fork attribution is checked too: authors exactly
`Roland Pheasant, Runic Artifex`, no `<owners>` element (nuget.org takes owners
from the account), the original `Copyright (c) Roland Pheasant 2011-` notice, and
a title and description that start by naming the Runic Artifex fork of
DynamicData for .NET 10.

## GitHub release

A prerelease version (one with a `-` label, such as `10.0.0-runic.118`) becomes
a GitHub prerelease; a stable version (such as `10.0.0`) becomes a regular
release. The draft's prerelease flag is checked before and after the upload.

The publish command refuses every existing tag or release,
creates a draft targeted at that SHA, uploads the exact two assets, downloads
them for byte comparison, creates and resolves a new non-force tag at that SHA,
and only then publishes the draft. The release carries generated release notes;
it is where the fork's release notes live. It uses the
current `GITHUB_REPOSITORY` explicitly for every GitHub operation. GitHub
publishing is restricted to a clean checkout of maintained `main` at
`GITHUB_SHA`; it also verifies that `main` remains the current default branch.
Before it creates a draft, the workflow requires a
completed successful Build workflow at that exact SHA, with the latest result of
each Linux/Windows and 8.4.0/9.0.0 matrix job succeeding. It intentionally does
not rerun CI; the maintained default branch is the supported release source.
`check` runs the same GitHub checks read-only.

## nuget.org publishing

`nuget-check` and `nuget-push` call [`nuget.py`](nuget.py), which uses only the
Python standard library and keyless, read-only lookups of the public nuget.org
service index, search and flat-container endpoints. Before anything is pushed it
checks every package of the pair:

- The ID must match a package glob of [`nuget-policy.json`](nuget-policy.json),
  the hand-kept mirror of the nuget.org trusted-publishing policy (owner
  `runic-artifex`, glob `RunicArtifex.*`). nuget.org publishes no policy, so the
  mirror is the only source; keep it in step with the account.
- If nuget.org already has that version, it is downloaded and compared with the
  local package entry by entry. nuget.org adds its repository signature
  (`.signature.p7s`) to every package it accepts, so that entry is ignored and
  every other entry must be byte-identical. An identical version is skipped; a
  different one fails, because published versions are immutable.
- A package still to be pushed must have an ID that is new on nuget.org or that
  `runic-artifex` owns. An ID nuget.org knows but whose search lists no version
  (all versions unlisted, or a push minutes ago not yet indexed) fails because
  its owners cannot be checked.

A failure names each package and the remedy, and nothing is pushed: the
trusted-publishing key would otherwise answer 403 during `dotnet nuget push`,
possibly after the first package was already published. `nuget-push` repeats
these checks and then runs `dotnet nuget push <package> --source
https://api.nuget.org/v3/index.json --api-key "$NUGET_API_KEY"` for each missing
package. `wait-indexed <version>` polls the flat-container version lists, which
`dotnet restore` uses, every 30 seconds for up to an hour until both list the
version.

## Workflow jobs and dry run

Run **Package release** on `main`. Its `dry-run` input defaults to checked.

- `packages` (`contents: read`, `actions: read`) builds, tests and packs, runs
  `check` and `nuget-check`, records the version and `sha256sum` of the pair, and
  uploads the pair as the `packages-<run id>` artifact.
- `github-release` (`contents: write`) downloads that artifact, checks it against
  the recorded hashes and runs `publish`.
- `nuget` runs after the GitHub release, in the `nuget` environment, and is the
  only job with `id-token: write`. It downloads and checks the same artifact,
  runs `nuget-check` before logging in, logs in with
  `NuGet/login` (`user: ${{ vars.NUGET_USER }}`), then runs `nuget-push`.
- `indexed` (`contents: read`, no environment or secret) runs `wait-indexed`.

A dry run ends after `packages`: every validation, the read-only GitHub checks
and the keyless nuget.org checks run, and the GitHub release, the NuGet login,
the push and the indexing wait are skipped. A failing dry-run check is the
failure the real run would hit.

## Retries

If `nuget` fails, use **Re-run failed jobs** in the same run: `github-release`
is not repeated, and `nuget` pushes the same artifact again, skipping any version
nuget.org already has with identical contents. If the push failed with 409
Conflict, nuget.org is still validating an earlier push of that version; wait
until it is listed, then rerun. If `indexed` times out, rerun it; nuget.org can
take up to an hour to list an accepted package. A new run for the same commit
cannot help: its GitHub release check refuses the existing tag, and a changed
package needs a new version.

## nuget.org account setup

Done once, by an administrator of the `runic-artifex` nuget.org organization
and of the GitHub repository:

1. On nuget.org, open **Trusted Publishing** for `runic-artifex` and add a
   policy for repository owner `Runic-Artifex`, repository `DynamicData`,
   workflow file `release.yml` and environment `nuget`. Allow it to push new
   packages and give it the package glob `RunicArtifex.*`, matching
   [`nuget-policy.json`](nuget-policy.json).
2. In the GitHub repository settings, add the repository variable `NUGET_USER`
   with the nuget.org profile name that owns the policy; `NuGet/login` exchanges
   the workflow's OIDC token for a short-lived key as that user.
3. Create the `nuget` environment, restricted to the `main` branch, with
   required reviewers so that each push to nuget.org is approved.
4. Optionally reserve the `RunicArtifex.*` ID prefix for `runic-artifex` on
   nuget.org (request it from nuget.org support), so no other account can
   create IDs under it and the packages show the verified-prefix mark.

Then run **Package release** with `dry-run` checked; it reports both IDs as new.

## Tests

Run the offline tests with:

```sh
bash eng/release/test-release-assets.sh
```

The tests generate small `.nupkg` fixtures and use a local `gh` mock, a local
`dotnet` mock and a mock nuget.org served on `127.0.0.1`. They cover new IDs,
an identical published version (skipped), a different published version, an ID
owned by another account or with unknown owners, an ID outside the package glob,
a retry after a partial push, the indexing wait and the dry-run commands, and
check that every nuget.org lookup is keyless. They do not contact GitHub or
nuget.org or create releases. `NUGET_TEST_SERVICE_INDEX` and `NUGET_POLICY`
exist for these tests only and are refused in GitHub Actions.
