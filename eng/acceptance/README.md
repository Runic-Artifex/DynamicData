# Runic package acceptance

This gate is a small external application, built only against the two immutable
`Runic.DynamicData` GitHub-release assets. It is deliberately separate from
[`eng/verification`](../verification/README.md): that suite uses project
references to prove the library tree, whereas this one proves that a released
package restores and runs in a consumer graph.

The shared application code is outside the library namespaces and is compiled
twice: once with `Runic.DynamicData`, once with `Runic.DynamicData.Reactive`.
It does not contain a `ProjectReference` or substitute sibling source for a
package asset.

## Covered workflows

`live presentation switches keyed feeds before virtualization and refreshes
aggregates` mirrors the Runic SDK model boundary. It switches the base keyed
feed before `AutoRefreshOnObservable` and `SortAndVirtualize`, so each
presentation receives current virtual metadata. It proves an initial ordered
viewport, feed handoff, refresh-driven reordering, `Sum`/`Avg`/`Minimum`/
`Maximum`, mixed edits, zero-size viewport metadata and an empty selected feed.
It then mutates a superseded feed to prove that it cannot update the selected
presentation or retain its row invalidation subscriptions.

`child failure closes its session and a new session recovers` uses `MergeMany`
for per-row result streams. A child error is terminal for that session: sibling
subscriptions are released, late results are ignored, and recovery explicitly
opens a fresh session. This is an application recovery policy, not a claim that
`MergeMany` retries or preserves state after errors.

`view close releases transformed rows and all control subscriptions` models a
window whose shared transformed rows are owned by one `DisposeMany` branch.
Replacing and removing rows release their previous presentation objects; closing
the window releases an off-screen row too, and detaches the selected-feed,
viewport and per-row invalidation subscriptions. Updates after close do not
change the bound snapshot.

The specialized contextual `Switch` overload remains intentionally limited: it
returns base keyed changesets and clearing batches have no virtual context. The
fixture follows the documented application composition of switching the keyed
feed before virtualizing it. It does not claim atomic handoff frames or a
context-preserving `Switch` contract. Each transformed row is also owned by
exactly one `DisposeMany` branch; applications must not share the same owner
object through several disposal branches.

## Running against a release

For a published release, the command fetches the two named GitHub release assets itself, stores them in
`artifacts/acceptance/release-assets/<version>`, verifies their SHA-256 values,
checks their branded identity, `.NET 10` contents and embedded Runic repository
commit, then requires that both embedded commits match the supplied expected
source commit before restoring from that local feed. It rejects upstream and
opposite-flavor DynamicData packages in the resolved graph.

For every run, the verifier serializes a task-owned `NuGet.Config` with cleared
sources, a portable file URI for the exact local package directory, and the
NuGet.org HTTPS endpoint. Source mapping confines `Runic.DynamicData*` to the
local feed. The verifier parses the generated XML back and checks those sources
and mappings before restore; it does not pass repeated `--source` arguments.

Use the release tag, version and hashes copied from the immutable GitHub release
record. The published packages declare the shipping `ReactiveUI.Primitives`
`9.0.0` cohort, so packaged managed and native acceptance each run that cohort
in both flavors. DynamicData's source CI separately establishes the `8.4.0`
and `9.0.0` source compatibility matrix. This gate must never force a package
downgrade or suppress `NU1605` merely to call it packaged `8.4.0` support.
Native mode uses a matching host RID.

```sh
direnv exec /home/viktor/Development/RunicArtifex/runic-sdk \
  python3 eng/acceptance/verify-package-acceptance.py managed \
  --tag v<version> --version <version> \
  --primitives-sha256 <sha256> --reactive-sha256 <sha256> \
  --expected-source-commit <commit>

df -h . /tmp
direnv exec /home/viktor/Development/RunicArtifex/runic-sdk \
  python3 eng/acceptance/verify-package-acceptance.py native --rid linux-x64 \
  --tag v<version> --version <version> \
  --primitives-sha256 <sha256> --reactive-sha256 <sha256> \
  --expected-source-commit <commit>
```

Before publication, an exact CI-produced candidate pair may be preflighted
without claiming it is a release. Supply the directory that holds the verified
pair instead of a tag; the mode-specific result report records
`asset_source: ci-candidate`.

```sh
direnv exec /home/viktor/Development/RunicArtifex/runic-sdk \
  python3 eng/acceptance/verify-package-acceptance.py managed \
  --asset-directory /absolute/path/to/verified/packages --version <version> \
  --primitives-sha256 <sha256> --reactive-sha256 <sha256> \
  --expected-source-commit <commit>
```

After promotion, repeat the GitHub-release command and require the downloaded
hashes to match the preflight assets. Inspect the retained restore, build/publish and runtime logs together with
the mode-specific `artifacts/acceptance/results-managed.json` or
`results-native-<rid>.json`. A completed report is written only after both
flavors pass; a failed rerun leaves `completed: false`. A release package can declare a stricter
minimum Primitives dependency than a source compatibility matrix: in that case
the `8.4.0` managed restore must fail visibly rather than being reported as
packaged compatibility. Native success covers these three paths only; it does
not establish NativeAOT support for every DynamicData operator, model or
platform.
