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

## Monthly reviews

Outcome of each `Upstream review YYYY-MM` issue. The tables below include its items.

| Review | Upstream main | Outcome |
| --- | --- | --- |
| [2026-11](https://github.com/Runic-Artifex/DynamicData/issues/46) | [`fd20189`](https://github.com/reactivemarbles/DynamicData/commit/fd20189036554e2b68017846500a13431a0e32ab) | No sync: deps/CI only. The six commits since the baseline `65016e5` are dependency and workflow bumps (#1090, #1108, #1117, #1121, #1172, #1204). The virtual merge conflicts only in the fork's own workflows and `src/Directory.Packages.props`. #1213 ported as a test; #1214/#1219 already covered by DD-030/DD-031, with the incremental extremum deferred. New gaps from #1208 and #1209 are fork issues #47 and #48. #1167 merged upstream on 2026-11-07, after this review; the next review compares it with DD-030. |

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
| #1142, #1144, #1147, #1115 (selected list queues), #1073 (list `AutoRefresh`/`FilterOnObservable` queues) | DD-028 |
| #1037, #1164, #1166, #1115 (list viewport) | DD-029 |
| #1167, #896, #1196 | DD-030 |
| #1214, #1219 (refresh-aware `Avg`, `StdDev`, `Maximum`, `Minimum`; upstream's randomized aggregate oracle passes against the fork) | DD-030, DD-031 |
| #1115, #1025 (list expiration) | DD-032 |
| #1039, #1044 | DD-033 |
| #1066 | DD-034 |
| #1142, #1147 (list `AutoRefreshOnObservable` child errors) | DD-041 |
| #1073 (`ExpireAfter`, `ToObservableChangeSet` and list predicate-state `Filter` gates) | DD-043 |
| #1099 | Existing behavior kept; concurrency tests enabled |
| #896, #1196 (fork issue #4) | DD-007, DD-030 |
| #1142, #1205, #1207 (fork issue #5) | DD-039 |
| #1142, #1206 (fork issue #6) | DD-040 |
| #1150 (fork issue #8) | DD-042 |
| #1213 | Test port: deterministic cross-feeding `CacheParentSubscription` test |

## Open gaps

Verified against `main` in the 2026-11 review (the October gaps #4, #5, #6 and #8 are closed; see Adopted):

| Gap | Upstream | Fork issue |
| --- | --- | --- |
| Parallel `SubscribeMany` rethrows source errors | #1208, #1142 | [#47](https://github.com/Runic-Artifex/DynamicData/issues/47) |
| Sorted, paged and distinct `AsAggregator` rethrow source errors | #1209, #1142 | [#48](https://github.com/Runic-Artifex/DynamicData/issues/48) |

## Deferred

| Upstream items | Revisit when |
| --- | --- |
| #1114 Orchestrator refactor | Terminal/queue behavior is stable and a benefit is measured. |
| #1174 TransformVirtual | A workload needs lazy pure projection; virtualize before expensive projections meanwhile. |
| #981 inline-update scheduler | Removal/disposal ordering and scheduler ownership are defined; use explicit stages meanwhile. |
| #1219 incremental `Maximum`/`Minimum` (rebuild only when the extreme leaves) | A workload shows the O(n) recompute per Refresh, Update or Remove batch matters. |
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
- #1199, #1200, #1202, #1203, #1204, #1215, #1216, #1217, #1218, #1220, #1221, #1222, #1223:
  upstream polyfill, Central Package Management, dependency and action updates; the fork
  has its own package management and SHA-pinned workflows (DD-025).
- #1210, #1211 (upstream 9.5.0 release) and #1212 (Lock Threads workflow permission):
  upstream release and repository automation.
