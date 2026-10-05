# October 2026 operations follow-up

Recorded **2026-10-05 22:16 UTC** after [fork PR #1](https://github.com/Runic-Artifex/DynamicData/pull/1)
adopted the reviewed integration into Runic `main` at
`9e3039a597f912c6ddc2628e38bb1162ad9c4563`. This dated note records subsequent
operational work without changing the [integration review](2026-10.md) or
[historical assessment](../analysis.md). Main was held at this SHA through
release 30's publication. Operational topic adoption/execution states below
are a dated observation, not a live dashboard. Release, documentation and
package-consumer completion do not imply that another agent's NativeAOT work
has finished.

## Source and hosted verification

The final local library/API verification remains pinned to `8f05de4a`; the
`8f05de4a..9e3039a5` difference contains four documentation files only. This
establishes equivalent production source, not identical package or DLL bytes.

The main [Build run 37368650077](https://github.com/Runic-Artifex/DynamicData/actions/runs/37368650077)
uses exact source `9e3039a5`. The complete four-cell hosted run completed
successfully at **2026-10-05 22:12 UTC**, attempt 3. Each cell ran 3,522
Primitives tests and 3,520 System.Reactive tests: **28,168 passed**, with zero
failed, skipped, cancelled, timed-out or flaky tests. All four uploaded artifact
SHA-256 values match their downloaded archives and runner round-trip checks.
The complete proof is retained at
`artifacts/verification/main-ci-37368650077/verification-report.json`.

| Actual executed cell | Attempt | Actual job ID |
| --- | ---: | --- |
| Ubuntu / 9.0.0 | 1 | `111959919432` |
| Windows / 9.0.0 | 1 | `111959919586` |
| Ubuntu / 8.4.0 | 2 | `111985465540` |
| Windows / 8.4.0 | 3 | `111993412329` |

The initial 8.4.0 failures were hosted runner acquisition/communication
failures. Only those two jobs were rerun; no extra full workflow was dispatched.
GitHub carries successful cells into later attempts without executing their
tests again, so the table records actual test jobs rather than copied job IDs.
Builds retain seven existing CS0618 obsolete-Sort benchmark warnings; artifact
actions also emitted DEP0005 warnings. The passing result does not claim a
warning-free workflow.

## Reviewed operational topics

The isolated `integration/fork-operations-2026-10` branch at
`9290114da673fa4225268f8cef5e96bf38871c0f` contains these reviewed topics. At
the recorded time, their main adoption and new hosted execution were pending.

| Register entry | Topic revision | Current state |
| --- | --- | --- |
| DD-036: monthly upstream preparation | `c32a9dcda4356bcbb53af5dff0b19a4e70d67b9a` | Independently reviewed and integrated on the isolated operations branch; isolated Git fixtures, shell syntax, actionlint and diff checks passed. Main adoption and hosted execution pending. |
| DD-037: release guard | `4939b178d6739df35e8dcae521f7ce53225c6cd3` | Independently reviewed and integrated on the isolated operations branch; strict fixtures, actionlint, shell syntax, diff checks and actual candidate validation passed. Main adoption and hosted execution pending. |
| DD-038: packaged acceptance | `91a06958f5e97faa58db975ef46f02d88a4b0b6a`, CI follow-up `e1550b2f73d492107f1d19d6c176377ea6e6f60d` | Independently reviewed and integrated on the isolated operations branch; candidate package checks completed. Main adoption and hosted execution of the new jobs pending. |

Monthly preparation reads upstream Git objects and REST inventory at explicit
pins. Every inventory item is fresh and unassessed for that review; it does not
copy the October dispositions as current decisions. It traverses nested merge
history and reports ambiguity rather than inventing an upstream adoption pin.
Publication targets only the Runic fork and refuses dirty checkouts, foreign
push destinations or an existing branch creation race. A repeated run verifies
existing snapshot pins without changing that branch. The current fork Actions
setting restricts PR creation; deferred creation retains the branch, artifact,
error and manual compare link. No repository setting is loosened to bypass it.
The workflow performs no real upstream merge, package release or upstream
message/contribution. A human still chooses and reviews every integration.

The release guard checks unique version/tag, branded identity, .NET 10,
default dependency cohort, repository SHA and original MIT attribution. It
requires the latest exact-source main Build run and its complete hosted matrix,
retains draft state
until downloaded assets match, and resolves the new tag before publication.
Its review and deployment must be reported independently of release 30's
one-time promotion of already tested CI bytes.
An independent corrupt-download probe was rejected before tag creation or
publication; offline mocks and candidate validation do not establish a hosted
release workflow run. Its retained evidence is under
`artifacts/verification/release-guard-4939b178/`.

The packaged acceptance follow-up adds managed jobs for Linux and Windows,
then Linux NativeAOT after both managed jobs succeed. It verifies the exact
downloaded CI artifact pair, asset hashes and repository commit before running
the three workflows in both flavors. The four source matrix cells stay
unchanged. After adoption, a successful overall Build run also requires these
packaged jobs; their local presence is not evidence of hosted execution.

## Release 30 publication

Candidate version **10.0.0-runic.30** was produced by the successful Linux
9.0.0 job of main Build run `37368650077`. Artifact `11370601153`
(`dynamicdata-ubuntu-latest-9.0.0`) records source `9e3039a5` and archive digest
`sha256:bdeda0dca754a730bd09bccf1781d63403e57e49bde9e3343e8a003d748bbd1b`.
The package pair below is already downloaded and independently inspected.

| Asset | Size (bytes) | SHA-256 |
| --- | ---: | --- |
| `Runic.DynamicData.10.0.0-runic.30.nupkg` | 689,166 | `cf0d369f43774c2ba1535db8c468c0214d9a82c7f3d1a3d6fa1bf74918f8cbbc` |
| `Runic.DynamicData.Reactive.10.0.0-runic.30.nupkg` | 690,348 | `23cc3f33c029157585a2521cc1c04afbaba21efc6dc6659a855ecafc67217fc5` |

Both nuspecs name the Runic repository and full `9e3039a5` source SHA,
target `net10.0`, retain Roland Pheasant/MIT attribution, and package source-
identical README and `LICENSE/LICENSE`. Their distinct dependencies are
`ReactiveUI.Primitives` and `ReactiveUI.Primitives.Reactive`, each declared at
minimum version 9.0.0. The local NBGV version query agrees on
`10.0.0-runic.30` for both projects.

[Prerelease v10.0.0-runic.30](https://github.com/Runic-Artifex/DynamicData/releases/tag/v10.0.0-runic.30)
was published at **2026-10-05 22:14:09 UTC**, release ID `404131470`, with
`draft: false` and `prerelease: true`. Both its full target SHA and the new
lightweight tag resolve to `9e3039a597f912c6ddc2628e38bb1162ad9c4563`.
Authenticated absence checks preceded tag/release creation. The verified Linux
CI bytes were promoted through a draft; no packages were rebuilt, repacked or
replaced. Exactly the two named assets above have the expected server digests
and sizes. Fresh public downloads match both the verified draft downloads and
the CI candidate byte for byte. Retained promotion evidence is at
`artifacts/verification/main-ci-37368650077/release-promotion-report.json`.

GitHub reports this release as `immutable: false`; the fork's no-replacement
rule is operational policy, not a claim of server enforcement. Corrections
must receive a new version rather than replacing these assets or retagging.

## Packaged application evidence and limits

The retained candidate acceptance reports at
`artifacts/verification/package-acceptance-candidate/results-managed.json` and
`artifacts/verification/package-acceptance-candidate/results-native-linux-x64.json` record
`asset_source: ci-candidate`, null release tag, completed true, the exact hashes
above and source `9e3039a5`. With SDK 10.0.401, two managed executables and two
Linux x64 NativeAOT executables passed the same three application workflows in
both flavors: live presentation/feed/viewport/aggregate updates; child-session
failure followed by explicit fresh-session recovery; and view-close ownership
and subscription disposal. Native binaries executed successfully.

These are headless application-model tests, with no actual Terra window,
browser or UI interaction. They do not prove every operator, arbitrary model,
platform or NativeAOT scenario. Feed switching occurs before virtualization;
the specialized contextual Switch still returns a base static changeset type
and synthetic reset batches have no virtual-context guarantee. One disposal
branch owns each transformed row.

The shipping 9.0.0 dependency minimum is respected. Source compatibility with
8.4.0 is proven by a separate source matrix; the packaged gate does not force
8.4.0 or suppress NU1605 to manufacture packaged compatibility. These candidate
runs do not by themselves prove the public release path.

Fresh public release replay using `--tag v10.0.0-runic.30` subsequently passed
both managed executables and both Linux x64 NativeAOT executables, with all
three workflows passing in each. These runs downloaded the public assets into
a previously absent release-asset directory; they did not substitute the local
CI feed. Both public hashes and repository commits match the promoted pair.
The retained mode reports under
`artifacts/verification/package-acceptance-release-30/` record
`asset_source: github-release`, the actual tag, expected full `9e3039a5` source
commit and `completed: true`; their four runtime logs each contain the three
workflow PASS results. Public managed acceptance ran on Linux; Windows managed
execution of the new gate remains part of the future hosted operational jobs.

## ReactiveUI.Validation coordination

The user deferred the ReactiveUI.Validation upgrade/adoption until the separate
NativeAOT agent has finished. Its isolated package-consumer work at
`56572b6b614e1915c657b175a9b3b5ef9da994cd` is preserved. DynamicData release
completion is not evidence that this other agent has finished and does not
authorize resuming the deferred Validation work. No Validation upgrade or
adoption is claimed by this operations record.
