# DynamicData upstream summaries

Human-readable summaries of [reactivemarbles/DynamicData](https://github.com/reactivemarbles/DynamicData)
issues and pull requests, collected on 2026-10-05, and how the fork handled the
open ones. Upstream numbers below refer to that repository.

| Document | Contents |
| --- | --- |
| [Issues](issues.md) | 409 issues (35 open) with summaries |
| [Pull requests](pull-requests.md) | 703 pull requests (23 open, 574 merged) with summaries |

Open gaps between upstream and this fork are tracked as
[GitHub issues](https://github.com/Runic-Artifex/DynamicData/issues), not in
these files. Intentional differences are in the
[fork difference register](../fork-differences.md).

Later upstream activity is summarized each month by the
[upstream review workflow](../maintenance.md#monthly-upstream-review) in an
`Upstream review YYYY-MM` issue. Its raw inventory is a workflow artifact and is
never committed.

## Adopted

| Upstream items | Fork difference |
| --- | --- |
| #1073, #1097 (merged), #1130, #1198 (merged), #1168 (merged) | DD-016, DD-031 |
| #1182, #1119, #1192 | DD-017 |
| #1169, #1171, #1072 | DD-018 |
| #1156, #1127 (comparison only) | DD-019 |
| #990 (cache async disposal) | DD-020 |
| #1170 | DD-021 |
| #949, #820, #1019 | DD-022, DD-023, DD-024 |
| #1089, #1090, #1121, #1172, #1195 | DD-025 |
| #1160, #983, #1086 | DD-026 |
| #1142, #1143, #1146 | DD-027 |
| #1142, #1144, #1147, #1115 (selected list queues) | DD-028 |
| #1037, #1164, #1166, #1115 (list viewport) | DD-029 |
| #1167, #896, #1196 | DD-030 |
| #1115, #1025 (list expiration) | DD-032 |
| #1039, #1044 | DD-033 |
| #1066 | DD-034 |
| #1099 | Existing behavior kept; concurrency tests enabled |

## Open gaps

Verified against the code after the October 2026 integration:

| Gap | Upstream | Fork issue |
| --- | --- | --- |
| List `AutoRefresh` / `FilterOnObservable` hold a lock across downstream delivery | #1073, #1115 | [#3](https://github.com/Runic-Artifex/DynamicData/issues/3) |
| `StdDev` ignores Refresh | #896, #1196 | [#4](https://github.com/Runic-Artifex/DynamicData/issues/4) |
| Lock-free cache `Watch` and `Watcher` drop terminal notifications | #1142 | [#5](https://github.com/Runic-Artifex/DynamicData/issues/5) |
| Cache `EditDiff` over an observable drops late errors | #1142 | [#6](https://github.com/Runic-Artifex/DynamicData/issues/6) |
| List `AutoRefreshOnObservable` child-error policy differs from the cache | #1142, #1147 | [#7](https://github.com/Runic-Artifex/DynamicData/issues/7) |
| `Switch` emits removals and additions as two changesets | #1150 | [#8](https://github.com/Runic-Artifex/DynamicData/issues/8) |

## Deferred

| Upstream items | Revisit when |
| --- | --- |
| #1114 Orchestrator refactor | Terminal/queue behavior is stable and a benefit is measured. |
| #1174 TransformVirtual | A workload needs lazy pure projection; virtualize before expensive projections meanwhile. |
| #981 inline-update scheduler | Removal/disposal ordering and scheduler ownership are defined; use explicit stages meanwhile. |
| #954 MaximumBy/MinimumBy | Empty, tie and comparer behavior is needed and defined. |
| #899 forced TransformMany | A measured need exists; prefer child changesets. |
| #887 R3, #853 bidirectional BindingList, #836 LINQ aliases | Runic adopts the ecosystem or needs the adapter. |
| #1151, #1197 global usings | Correctness ports have settled. |
| #373 custom key equality, #516 strict disposal | A concrete use case exists; normalize domain keys and dispose owned proxies meanwhile. |

## Not applicable

- #1116 is the fork's foundation (DD-002, DD-003); do not re-import the branch.
- #1165 (upstream 9.5 release branch) is a comparison source, not a merge target.
- #669, #963, #861: keep current behavior (no initial capture in `ChangeAwareCache`,
  composable `Sort`, independent dependency policy).
- #1103, #1104, #1105, #1107, #1108, #1109, #1117: dependency updates for packages
  the fork does not use or already pins (TUnit, .NET 10 only).
