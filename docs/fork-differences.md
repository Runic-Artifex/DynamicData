# Runic fork difference register

Versioned baseline: **2026-10-05**, Runic `runic/reactiveui25` at
[`edd2d175794afc86e964a06cb55329f44793009f`](https://github.com/Runic-Artifex/DynamicData/commit/edd2d175794afc86e964a06cb55329f44793009f).
This is a source/history baseline, not a claim of tests run for this documentation
or a record of subsequent implementation. Follow the
[maintenance policy](maintenance.md) when integrating or retiring entries.

Keep stable IDs. Each entry records its source, reason/scope, dependencies,
validation to preserve and retirement condition. Update entries with adopting or
removing commits and evidence; retain retired entries. `active` means intentional
fork policy/adaptation, `retained` means inherited behavior to protect when merging,
and `retired` means verified removal/replacement. Exact ancestry does not establish
behavioral equivalence with another upstream branch.

## Platform, packaging and lifecycle baseline

| ID / status | Source and scope | Dependencies / validation | Retirement condition |
| --- | --- | --- | --- |
| DD-001 / active | Runic merge `c879de9a`; [global.json](../global.json) and project targets pin SDK 10.0.401 and .NET 10 only. Removes upstream's broader target/approval matrix. | Both libraries, tests and API baselines must stay aligned; validate Linux/Windows builds and both .NET 10 API surfaces. | Explicit Runic platform policy change, with replacement SDK/target/API validation. An upstream target change alone is insufficient. |
| DD-002 / active | Foundation [upstream PR #1116](https://github.com/reactivemarbles/DynamicData/pull/1116), `15189189`, `c879de9a`; [build props](../src/Directory.Build.props) default to Primitives 9.0.0 and accept `ReactiveUIPrimitivesVersion=8.4.0`. `edd2d175` adds [independent dependency policy](../Directory.Packages.props). | ReactiveUI 25 compatibility uses 8.4.0; current Runic uses 9.0.0. Test both versions and flavors without inheriting enclosing SDK central package versions. | Runic explicitly ends a dependency generation or changes dependency ownership; validate consumers before dropping checks. |
| DD-003 / active | #1116 foundation and `c879de9a`; [Primitives project](../src/DynamicData/DynamicData.csproj) and [Reactive project](../src/DynamicData.Reactive/DynamicData.Reactive.csproj) compile shared sources with `REACTIVE_SHIM` and flavor namespaces. | Changes must compile and behave in both flavors. Keep conditional imports/types consistent and review both API baselines. | Explicit decision to end one flavor or replace this architecture, supported by consumer migration and validation. |
| DD-004 / active | `c879de9a`; [TUnit tests](../src/DynamicData.Tests/DynamicData.Tests.csproj) and [Reactive tests](../src/DynamicData.Reactive.Tests/DynamicData.Reactive.Tests.csproj) use TUnit/Microsoft.Testing.Platform; Reactive tests share test sources except flavor-specific API tests. | Await assertions; use virtual time and explicit synchronization. Translate upstream tests instead of adding xUnit/VSTest dependencies. Both executables must run for each dependency generation. | Explicit test-platform change with equivalent operator, concurrency and API coverage. |
| DD-005 / active | `c879de9a`, `e1424fd4`; Runic package IDs, metadata, [versioning](../version.json) and [release workflow](../.github/workflows/release.yml). Namespaces remain `DynamicData` / `DynamicData.Reactive`; original MIT license/authorship retained. | Ship `Runic.DynamicData` and `Runic.DynamicData.Reactive` GitHub assets. Validate package identity/content, tested source SHA and unique immutable version/tag. Current release workflow creates prereleases. | Explicit packaging/distribution policy change. Never replace a published version or remove original licensing/attribution. |
| DD-006 / active | `bca31a2a`, `4effb83c`, `edd2d175`; [list expiration](../src/DynamicData/List/Internal/ExpireAfter.cs) and [cache source expiration](../src/DynamicData/Cache/Internal/ExpireAfter.ForSource.cs) guard callbacks racing subscription/source disposal. | Preserve [list regressions](../src/DynamicData.Tests/List/ExpireAfterFixture.cs) and [cache disposal race regression](../src/DynamicData.Tests/Cache/ExpireAfterFixture.DisposalRace.cs), including synchronized scheduling. Queue/list ports must preserve ownership and late-callback safety in both flavors. | A verified upstream replacement provides equivalent subscription/source-disposal protection; retain the regression tests and record replacement/removal SHAs. |
| DD-007 / active | `b8e09eaa` plus Runic `c879de9a`; [StdDev](../src/DynamicData/Aggregation/StdDevEx.cs) uses sample variance and incremental central moments. Integral/decimal selectors use decimal accumulators. | Preserve [numerical regressions](../src/DynamicData.Tests/AggregationTests/StdDevFixture.cs): divisor, fallback, offsets and removal. Decimal central moments must fit decimal range; floating-point rounding limits remain. Refresh-aware aggregation is separate. | Verified replacement preserves sample-statistic contracts and numerical behavior; retain regressions and documented limits. |

## Recent targeted fixes already represented

These source commits are already in the baseline ancestry. They are retained
behavior, not claims that upstream main lacks the fix. Their common dependencies
are the shared-source/flavor policy (DD-003) and TUnit adaptation (DD-004). Before
replacing any local adaptation, test the cited regression in both flavors and
record the verified replacement SHA; then retire the adaptation while keeping
the regression. A newer upstream merge by itself does not meet that condition.

| ID / status | Source | Retained behavior / validation |
| --- | --- | --- |
| DD-008 / retained | [#1188](https://github.com/reactivemarbles/DynamicData/pull/1188), `bd9070ae` | Switch subscription ownership and reentrant handoffs; [subscription lifetime fixture](../src/DynamicData.Tests/Cache/SwitchFixture.SubscriptionLifetime.cs). Protect newer Runic queue/flavor adaptations during terminal-event ports. |
| DD-009 / retained | [#1193](https://github.com/reactivemarbles/DynamicData/pull/1193), `76cd4514` | Release property handlers when initialization throws; [property behavior fixture](../src/DynamicData.Tests/Binding/WhenPropertyChangedBehaviorFixture.cs). |
| DD-010 / retained | [#1194](https://github.com/reactivemarbles/DynamicData/pull/1194), `e05d9194` | Evaluate value-changing property-chain conversions; [conversion fixture](../src/DynamicData.Tests/Binding/WhenPropertyChangedBehaviorFixture.Conversions.cs). Preserve DD-009 cleanup. |
| DD-011 / retained | [#1190](https://github.com/reactivemarbles/DynamicData/pull/1190), `3d213602` | Serialize Optional initialization with source notifications; [initial-value fixture](../src/DynamicData.Tests/Cache/ToObservableOptionalFixture.InitialValue.cs). |
| DD-012 / retained | [#1189](https://github.com/reactivemarbles/DynamicData/pull/1189), `c7787940` | Named timer BatchIf overload compatibility; [overload fixture](../src/DynamicData.Tests/Cache/BatchIfFixture.Overloads.cs) and external consumer compilation for API changes. |
| DD-013 / retained | [#1185](https://github.com/reactivemarbles/DynamicData/pull/1185), `95cb78cb` | Validate legacy TransformSafeAsync factories before forwarding; [argument-validation fixture](../src/DynamicData.Tests/Cache/TransformSafeAsyncFixture.ArgumentValidation.cs). |
| DD-014 / retained | [#1186](https://github.com/reactivemarbles/DynamicData/pull/1186), `a7e68bea` | Filtering assertions reject stale extras; [static-filter fixture](../src/DynamicData.Tests/Cache/FilterFixture.Static.cs). Preserve exact contents, not containment-only assertions. |
| DD-015 / retained | [#1191](https://github.com/reactivemarbles/DynamicData/pull/1191), `b8e09eaa` | Correct sample StdDev divisor; validate together with the later numerical adaptation DD-007. |

## Scope of future updates

The [research assessment](upstream/analysis.md) identifies proposed queue,
terminal-event, identity and viewport work. Those proposals are not completed
entries here. Add entries when changes are actually adopted, with exact source
and adaptation commits, dependencies, regression evidence and retirement
conditions. Cache `AsyncDisposeMany` already exists with unit/integration tests;
list parity is optional follow-up work, not evidence that the cache API is absent.
Existing tests do not establish all disposal semantics: its value-equality
replacement check currently differs from the documented reference-identity
contract and remains targeted follow-up work.
