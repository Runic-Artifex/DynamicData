# Runic fork differences

Intentional differences from [reactivemarbles/DynamicData](https://github.com/reactivemarbles/DynamicData)
that must survive upstream merges. Keep IDs stable. Update an entry in the same
change that adds, changes or removes a difference; when upstream provides an
equivalent, remove the adaptation but keep its regression tests, and mark the
entry `retired`. Upstream status is as last reviewed; open gaps are tracked as
[fork issues](https://github.com/Runic-Artifex/DynamicData/issues).

| ID | What differs | Why | Upstream status |
| --- | --- | --- | --- |
| DD-001 | .NET 10 only; SDK pinned in [global.json](../global.json). | Runic targets .NET 10; one API baseline per flavor. | Upstream multi-targets. |
| DD-002 | Default ReactiveUI.Primitives 9.0.0; `-p:ReactiveUIPrimitivesVersion=8.4.0` keeps ReactiveUI 25 source compatibility. Independent [package versions](../Directory.Packages.props). | Current Runic uses Primitives 9; 8.4 consumers still build. | Based on open PR [#1116](https://github.com/reactivemarbles/DynamicData/pull/1116). |
| DD-003 | Two flavors from shared sources: Primitives (`DynamicData`) and System.Reactive (`DynamicData.Reactive`, `REACTIVE_SHIM`). | Consumers of either Rx stack. | Based on open PR #1116. |
| DD-004 | Tests use TUnit / Microsoft.Testing.Platform; upstream xUnit tests are translated. | One runner for both flavors. | Upstream uses xUnit. |
| DD-005 | Packages `Runic.DynamicData` and `Runic.DynamicData.Reactive`, released as GitHub release assets; namespaces unchanged, original MIT license and authorship kept. | Avoid clashing with upstream package IDs. | Fork only. |
| DD-006 | List and cache `ExpireAfter` guard callbacks racing subscription/source disposal. | Late timer callbacks after disposal. | Fork fix. |
| DD-007 | `StdDev` uses incremental central moments; integral/decimal selectors use decimal accumulators. | Precision for large offsets ([#1196](https://github.com/reactivemarbles/DynamicData/issues/1196)). | Upstream fixed only the sample divisor (#1191). |
| DD-016 | Queue-serialized operators use gate-free merge helpers that honour the intended Rx selector/empty-input/null-value contracts. | Cross-cache deadlocks ([#1073](https://github.com/reactivemarbles/DynamicData/issues/1073)). | #1097 merged; helper contract repairs are fork only. |
| DD-017 | `RemoveKey` keeps per-subscription key positions. | Equal values lost identity ([#1182](https://github.com/reactivemarbles/DynamicData/issues/1182), #1119). | Open PR [#1192](https://github.com/reactivemarbles/DynamicData/pull/1192). |
| DD-018 | List `Transform`/`TransformAsync`, `EditDiff` and `TransformMany` remove one occurrence at a time (reference, then equality). | Dropped or duplicated rows (#1169, #1072). | Open PR [#1171](https://github.com/reactivemarbles/DynamicData/pull/1171) covers part. |
| DD-019 | Property observation subscribes to the runtime chain target, not only the declared type. | Missed notifications through interfaces (#1156). | Open issue. |
| DD-020 | Cache `AsyncDisposeMany` disposes a replaced distinct reference even when values are equal. | Leaked owned objects. | Fork fix. |
| DD-021 | `BufferInitial` uses one one-shot timer for both collection types. | Spinning with tiny timeouts (#1170). | Open issue. |
| DD-022 | Default-comparer `ToSortedCollection` overloads. | #949. | Open issue. |
| DD-023 | Sorted test aggregator exposes `SortedItems`. | #820. | Open issue. |
| DD-024 | List `Batch` convenience operator (Buffer + FlattenBufferResult). | #1019. | Open issue. |
| DD-025 | CI: SHA-pinned Node 24 actions, Linux/Windows × Primitives 8.4/9.0 matrix, artifact round-trip check. | Supported runners; real artifact validation. | Upstream workflows differ. |
| DD-026 | External consumer / NativeAOT probes in [eng/verification](../eng/verification/README.md). | Overload resolution and AOT outside library namespaces (#1160, #983). | Fork only. |
| DD-027 | Cache operators forward completion and errors; `MergeMany` child errors now propagate. | Rx terminal contract (#1143). | Open PR [#1146](https://github.com/reactivemarbles/DynamicData/pull/1146). |
| DD-028 | List operators forward completion and errors; selected list operators use shared delivery queues. | Rx terminal contract (#1144) and list deadlocks. | Open PRs [#1147](https://github.com/reactivemarbles/DynamicData/pull/1147), [#1115](https://github.com/reactivemarbles/DynamicData/pull/1115). |
| DD-029 | Zero-size viewports are valid; specialized `Switch` for virtualized changesets (returns base keyed changesets). | #1037, #1164. | Open PR [#1166](https://github.com/reactivemarbles/DynamicData/pull/1166). |
| DD-030 | Refresh-aware `Sum`, new `SumImmutable`, refresh-aware `Minimum`/`Maximum`. | Stale aggregates after inline updates (#896). | Open PR [#1167](https://github.com/reactivemarbles/DynamicData/pull/1167) (Sum only). |
| DD-031 | `Avg` fallback and direct mutable projections; inherited skipped tests enabled. | Follow-up to #1168. | Fork only. |
| DD-032 | List `ExpireAfter` tracks deadlines per occurrence. | Duplicate rows expired together (#1115, #1025). | Fork only. |
| DD-033 | List observable filtering excludes initially false items and tracks occurrences. | #1039, #1044. | Open issues. |
| DD-034 | `Sort` emits one move for a single outlier. | Move amplification (#1066). | Open issue. |
| DD-036 | Monthly [upstream review](maintenance.md#monthly-upstream-review) as workflow artifact plus tracking issue. | Fork maintenance. | Fork only. |
| DD-037 | [Release guard](../eng/release/README.md) for immutable branded GitHub releases. | Fork distribution. | Fork only. |
| DD-038 | [Packaged acceptance](../eng/acceptance/README.md) of the released package pair, managed and NativeAOT. | Fork distribution. | Fork only. |

DD-008 to DD-015 were upstream fixes (#1185–#1194) that upstream has since
merged, and DD-035 was a test-only audit; they are no longer fork differences,
but their regression tests stay.
