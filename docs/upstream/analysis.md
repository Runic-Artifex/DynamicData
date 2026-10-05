# Upstream investigation for Runic DynamicData

Research date: **2026-10-05**. This is a research snapshot and proposed sequence,
not canonical roadmap state. No operator implementation, package release or
upstream contribution was performed during this investigation.

## Recommendation

Prioritize **queue/deadlock correctness, terminal notifications and keyed list
identity**, then the viewport edge cases that directly affect Runic. Keep the
fork's .NET 10, Primitives/System.Reactive flavors and disposal improvements.
Feature and import refactors should follow behavioral fixes.

The most consequential new finding is [merged PR #1097](https://github.com/reactivemarbles/DynamicData/pull/1097):
it removes secondary Rx gates that can undo the existing cross-cache deadlock
protection. The fork already uses delivery queues, but its
`SortAndVirtualize` still combines queue-serialized inputs with ordinary
`.Merge().Merge()`. That is the same shape the upstream fix addresses, and
Runic's proposed integration uses this operator directly.

| Proposed sequence | Work | Why it matters to Runic | Completion scope |
| --- | --- | --- | --- |
| 1 | Port merged #1097; adapt #1198 stress-test isolation | Cross-cache callbacks can still deadlock; reliable verification is needed | Queue helper/affected operators, focused concurrency and terminal regressions in both flavors |
| 2 | Port remaining #1146/#1147 terminal repairs | Hanging completion and swallowed errors affect teardown, joins, bindings and tests | Per-operator groups; deliberate inner-error changes documented separately |
| 3 | Port #1192 keyed identity | Equal-valued entries can update/remove the wrong row after RemoveKey | Per-subscription key positions, duplicates, indexed operations and performance coverage |
| 4 | Repair zero viewport #1037 and virtualized Switch resolution #1164/#1166 | Hidden presentations and switching feeds must retain correct rows and metadata | Zero-window/context contract and external consumer compilation |
| 5 | Complete list #1171 and assess remaining list queue migration #1115 | Value-type removals and some list operators remain less protected | Occurrence-safe removal and selective migration retaining expiration fixes |
| 6 | Profile #1066 move amplification; fix #1170 initial timer | Excess deltas can trigger full snapshots; tiny timers can waste CPU | Replay correctness plus bounded-window throughput/allocation measurements |
| 7 | Add #1167 refresh-aware aggregation if a real aggregate needs it | Mutable item refreshes currently produce stale results in some aggregations | Sum plus immutable fast path; Maximum/Avg tracked as separate follow-ups |
| Parallel small maintenance | Artifact/action updates and optional #820/#949 convenience APIs | Keep CI usable and make regressions easier to express | Small independent edits, actual artifact download and API compilation checks |

“Port” means adapt and validate the relevant behavior. It does not mean blindly
merge an upstream branch. Several proposals use xUnit, System.Reactive-specific
helpers or API shapes that differ from this fork.

## Scope and evidence

Upstream is [reactivemarbles/DynamicData](https://github.com/reactivemarbles/DynamicData),
confirmed by the local `upstream` remote. The reviewed fork is
[Runic-Artifex/DynamicData](https://github.com/Runic-Artifex/DynamicData), local
branch `runic/reactiveui25`, commit
[`edd2d175794afc86e964a06cb55329f44793009f`](https://github.com/Runic-Artifex/DynamicData/commit/edd2d175794afc86e964a06cb55329f44793009f).
The current upstream main was
[`c98ab809a99fcf7849debac62cd32699dd2fb9a6`](https://github.com/reactivemarbles/DynamicData/commit/c98ab809a99fcf7849debac62cd32699dd2fb9a6).

The full REST collection returned **409 issues and 703 PRs**: 35 open issues,
374 closed issues, 23 open PRs, 106 closed unmerged PRs and 574 merged PRs.
The 58 open objects agree with repository metadata. Two historical PRs,
[#234](https://github.com/reactivemarbles/DynamicData/pull/234) and
[#274](https://github.com/reactivemarbles/DynamicData/pull/274), appeared only in
the pulls endpoint; collecting both endpoints avoids dropping them.

The inventory includes all open/closed records available from these paginated
endpoints. Deleted or inaccessible objects and GitHub Discussions are outside
this scope. Bodies, available issue comments and file diffs informed the open
backlog assessment; PR review threads and CI runs were not exhaustively audited.
Historical entries have conservative title-based summaries. Closed issue state
is not proof of resolution.

**552 of 574 merged PR merge commits are in the fork's ancestry.** The other
22 include alternate release history as well as new main work; lack of exact
ancestry does not rule out an equivalent implementation. Every PR carries that
distinction in [inventory.json](inventory.json).

Local upstream tracking was three main commits behind current GitHub main:

| Newly merged item | Assessment |
| --- | --- |
| [#1097](https://github.com/reactivemarbles/DynamicData/pull/1097), `7ca68d57` | Important missing queue/gate fixes; detailed below |
| [#1198](https://github.com/reactivemarbles/DynamicData/pull/1198), `600f0cad` | Useful concurrency-test organization; translate to TUnit |
| [#1168](https://github.com/reactivemarbles/DynamicData/pull/1168), `c98ab809` | Useful expanded Avg regressions; test improvement rather than an operator fix |

No C# builds or runtime tests were run for this documentation task. Findings
marked present/missing are based on source, diffs and ancestry; proposed fixes
have not been validated locally. Upstream PR claims about test results remain
claims from their authors.

## How Runic changes the priorities

The [SDK integration guide](https://github.com/Runic-Artifex/runic-sdk/blob/feat/dynamicdata-collections/docs/guides/application/guides/dynamicdata.md)
and [example](https://github.com/Runic-Artifex/runic-sdk/tree/feat/dynamicdata-collections/examples/dynamicdata)
were read from the local `feat/dynamicdata-collections` branch. This work is on
that branch; it should not be described as already available on SDK main.

The design keeps a large keyed source in .NET and gives each presentation its
own viewport and bound collection. `ObserveOn` transfers delivery to the model
sequencer; `BatchBridgeSnapshots` wraps the actual downstream binding. The
browser receives keyed, indexed collection changes. Stable identity and order
are therefore correctness requirements, and move amplification is a transport
and rendering cost as well as a binding cost.

The guide specifies a 4,096-operation snapshot fallback and bounded pending
delivery. An unnecessarily large move sequence can force a full snapshot even
when only one row changed. Improving backend move emission should be measured
through this boundary, rather than only in a standalone sorting benchmark.

This fork targets .NET 10 only, uses Primitives 9.0 by default, checks 8.4
compatibility, and compiles a separate System.Reactive flavor from shared
sources. Those choices make upstream multi-framework, xUnit and compatibility
dependency proposals less useful than their titles suggest.

## 1. Queue correctness and lifecycle

Relevant sources: [issue #1073](https://github.com/reactivemarbles/DynamicData/issues/1073),
[merged #1079](https://github.com/reactivemarbles/DynamicData/pull/1079),
[merged #1097](https://github.com/reactivemarbles/DynamicData/pull/1097),
[list proposal #1115](https://github.com/reactivemarbles/DynamicData/pull/1115),
[stress issue #1130](https://github.com/reactivemarbles/DynamicData/issues/1130),
[merged #1198](https://github.com/reactivemarbles/DynamicData/pull/1198).

The fork's [ObservableCache](../../src/DynamicData/Cache/ObservableCache.cs)
and [SourceList](../../src/DynamicData/List/SourceList.cs) already queue
notifications. The cache comment explicitly states that delivery occurs after
releasing its mutation gate. Existing cross-write regressions are useful evidence
of intended behavior, but they do not establish that every composed pipeline is
safe.

The source-level remaining gap is visible in
[SortAndVirtualize](../../src/DynamicData/Cache/Internal/SortAndVirtualize.cs):
inputs first pass through `SynchronizeSafe(queue)`, then ordinary Rx merges.
The newly merged diff also covers SortAndPage, Page, Virtualise, AutoRefresh,
grouping, querying and related queue-serialized paths. Inspect the actual merged
files rather than treating the original PR body's list as exhaustive.

Implementation approach:

1. Port the merged helper changes from #1097 with conditional flavor namespaces
   and the fork's existing `SharedDeliveryQueue`/`DeliveryQueue` conventions.
2. Replace ordinary merge gates only where all inputs share serialized delivery.
   The gate-free helper is unsafe as a general substitute for Rx Merge.
3. Preserve separate observer terminal state for each input, all-input completion,
   first-error behavior and disposal ordering. A single stopped observer shared
   across sources can lose later completions.
4. Adapt the helper and bidirectional stress regressions, including concurrent
   request/comparer/refresher signals. Assert exact final state and delivery
   serialization, not merely that the test returns before a timeout.
5. Organize expensive concurrency fixtures as nonparallel TUnit integration
   tests, using #1198 as a reference. Preserve meaningful contention within each
   fixture; suite isolation should not become a way to suppress a real race.

The list proposal #1115 is not wholly absent: SourceList already has queue
groundwork. However, BufferIf and multiple list operators still use private Rx
synchronization. Port the remaining changes selectively. Its item-equality-based
expiration strategy also needs scrutiny for duplicate list occurrences and
different expiry times. Do not discard the fork's newer callback/disposal guards
while replacing expiration code.

## 2. Completion and error contracts

Relevant sources: [umbrella #1142](https://github.com/reactivemarbles/DynamicData/issues/1142),
[cache #1143](https://github.com/reactivemarbles/DynamicData/issues/1143),
[list #1144](https://github.com/reactivemarbles/DynamicData/issues/1144),
[PR #1146](https://github.com/reactivemarbles/DynamicData/pull/1146),
[PR #1147](https://github.com/reactivemarbles/DynamicData/pull/1147).

The fork has newer targeted Switch, property, Optional and expiration fixes, but
the broader terminal repair proposals have not been fully applied. A clear
example is [list QueryWhenChanged](../../src/DynamicData/List/Internal/QueryWhenChanged.cs):
its custom subscription forwards `OnNext` alone. List BufferIf similarly needs
terminal-path review. These are direct source findings; no hanging runtime test
was executed during this investigation.

Implement repairs in coherent groups: queries and buffers, sorting and
virtualized bindings, grouping, then merge/combiners/joins. `Observable.Defer`
with ordinary projection operators can remove handwritten subscription plumbing
where that preserves semantics. For custom operators, forward errors and
completion explicitly and release resources exactly once.

Do not indiscriminately replace control `Never` streams with `Empty`: a real
control signal may legitimately remain open, and live child streams can affect
completion. Decide whether source completion waits for children and whether
buffered changes must flush before completion. The proposal's change from
swallowing child errors in MergeMany to fail-fast is a public behavior change,
not merely missing plumbing.

Validation must distinguish empty/populated source, completion/error during
subscription versus later, still-live children, buffered items, and writes after
disposal. Compile and exercise each relevant overload independently. Upstream's
reported forwarding-recursion defect must be checked against the fork's actual
signature set; this fork has different equality-comparer overloads, so the
reported upstream crash is not established here.

## 3. Keyed identity and list conversion

Relevant sources: [#1119](https://github.com/reactivemarbles/DynamicData/issues/1119),
[#1182](https://github.com/reactivemarbles/DynamicData/issues/1182),
[PR #1192](https://github.com/reactivemarbles/DynamicData/pull/1192).

[RemoveKey](../../src/DynamicData/Cache/ObservableCacheEx.RemoveKey.cs)
is still a stateless per-changeset projection through RemoveKeyEnumerator.
Different keys with equal values cannot reliably be distinguished downstream.
That can cause a wrong row to be refreshed, replaced or removed. The older
equality-based workaround for invalid indexes cannot reconstruct keyed identity.

**The current PR diff and body disagree.** The body describes a position list
and dictionary; the reviewed head
`1c6fdfd9222348d43d2d12abf33f7c6222df30bc` contains `KeyPositionIndex`, an
order-statistic tree with subtree sizes, parent pointers and randomized priorities.
Use the actual head/diff as the implementation reference.

The proposed tree gives expected logarithmic insertion/removal/position lookup
with linear worst case and per-key memory. This avoids rewriting all following
positions when removing from a large collection. Preserve per-subscription state
through `Defer`; updates remain remove/add and refresh remains self-replacement
under this proposal's compatibility contract.

Finish the port by adding flavor conditionals for the new internal type,
translating the tests to TUnit and defining partial-history behavior. The reviewed
head assumes a complete history from an empty projection; unknown/contradictory
changes can report unknown positions rather than inventing them. Do not promise
correct indexed binding from a stream that omitted its original additions.

Required regressions: equal objects under distinct keys, the same reference under
two keys, equal structs, remove/update/refresh of the second occurrence, indexed
move, independent subscribers and resubscription. A randomized replay oracle
should compare the resulting list after every operation with an ordinary keyed
reference model. Benchmark mass removals at the 100,000-row scale as well as
small viewport workloads.

For Runic, keep filtering and identity-sensitive work keyed as long as possible;
drop keys only at the list boundary when necessary. Stable browser keys do not
repair already corrupted backend list indexes.

## 4. Viewport and Switch behavior

Relevant sources: [#1037](https://github.com/reactivemarbles/DynamicData/issues/1037),
[#1164](https://github.com/reactivemarbles/DynamicData/issues/1164),
[PR #1166](https://github.com/reactivemarbles/DynamicData/pull/1166),
[#1150](https://github.com/reactivemarbles/DynamicData/issues/1150).

SortAndVirtualize filters requests to `Size > 0`. In this fork its initial
default window is **25 rows**, so rejecting a zero request leaves an old/default
window rather than producing an empty view. The issue's description of emitting
all rows should not be repeated as this fork's universal behavior.

Allow a valid size of zero to produce an empty visible collection with accurate
total/start/size context. Reject negative inputs according to an explicit contract.
Ensure a zero request is observed before connecting a populated source if the
presentation must never materialize the default window. Also review suppression
of empty changesets: context-only changes, such as total count while the viewport
stays empty, may still need delivery to keep browser scroll extent correct.

The virtualized Switch proposal addresses overload selection from namespaces
outside DynamicData, where plain Rx Switch can win. However, its new overload
returns a base keyed changeset and loses `VirtualContext`. That can change which
Bind overload is selected and how sorted viewport rows are maintained. Correct
overload selection alone does not establish the full binding behavior.

For the current Runic architecture, prefer switching the keyed source feed
**before** each presentation's SortAndVirtualize. If switching already virtualized
streams is required, preserve context explicitly and document what happens when
the new feed is empty. Add consumer compilation and runtime regressions outside
the library namespace in both flavors.

Atomic Switch handoff (#1150) is a separate improvement. Combine old removals
with the new source's synchronous initial snapshot when possible, but define
delayed source and error behavior. Preserve the fork's #1188 exclusive subscription
and reentrant-selection protection; waiting indefinitely for a new first batch
could retain stale rows or hold state unnecessarily.

## 5. Async list transforms, diffing and refresh races

Relevant sources: [#1169](https://github.com/reactivemarbles/DynamicData/issues/1169),
[PR #1171](https://github.com/reactivemarbles/DynamicData/pull/1171),
[#1072](https://github.com/reactivemarbles/DynamicData/issues/1072),
[#1099](https://github.com/reactivemarbles/DynamicData/issues/1099),
[#1039](https://github.com/reactivemarbles/DynamicData/issues/1039),
[#1044](https://github.com/reactivemarbles/DynamicData/issues/1044).

The fork's [list TransformAsync](../../src/DynamicData/List/Internal/TransformAsync.cs)
already compares the stored source with the requested removal, unlike upstream's
original comparison against its own wrapper. It still uses `ReferenceEquals`,
which is insufficient for value types. Its indexless range path can remove all
matching references instead of exactly the requested number of occurrences.

Port the equality/occurrence part of #1171 while retaining cancellation and
semaphore ownership. Test mixed remove/add batches, value types, duplicate
references, custom equality and disposal during an awaited transform. Prefer a
reference match before equality for reference types where that preserves intended
identity, and remove exactly one occurrence per range item.

[List EditDiff](../../src/DynamicData/List/Internal/EditDiff.cs) still uses
`Except` in both directions, which implements set difference rather than list
multiplicity. An occurrence-aware matcher can repair duplicates without committing
to an unbounded quadratic LCS. If preserving reordering is part of the API,
define it explicitly and use a bounded diff plus reset fallback for adversarial
inputs. Runic's uniquely keyed DTO pipeline reduces the immediate impact of this
list-only feature.

[Cache AutoRefresh](../../src/DynamicData/Cache/Internal/AutoRefresh.cs) already
suppresses synchronous initial refresh signals, checks child subscription identity
and removes pending refreshes on removal. That directly addresses the mechanism
in #1099, but retain the exact same-batch add/remove regression when importing
queue or orchestration changes.

The discussion repro for #1039 uses list filtering. Evaluate cache and list paths
separately and assert that an initially false item never reaches an expensive
downstream transform. Static list Filter now has explicit move handling and
movement regressions (#1044); dynamic filtering and the exact collection swap
repro still deserve separate coverage.

## 6. Performance and timer changes

Relevant sources: [#1066](https://github.com/reactivemarbles/DynamicData/issues/1066),
[#1019](https://github.com/reactivemarbles/DynamicData/issues/1019),
[#1170](https://github.com/reactivemarbles/DynamicData/issues/1170).

For #1066, first measure Runic's actual sorted keyed viewport path. The report
concerns a list reorder algorithm; applicability and magnitude can differ from
SortAndVirtualize. A one-key move should ideally emit one meaningful indexed move,
but coalescing arbitrary move sequences is not safe without replay equivalence.
Use a reference list to validate every emitted index and measure move count,
allocation, encoded rows and snapshot fallback frequency.

For batching, `Buffer` plus `FlattenBufferResult` already preserves changes.
Rx `Throttle` directly on changesets drops changes and corrupts downstream state.
To sample the latest state, apply every changeset to a materialized state first,
then throttle/sample snapshots. A list Batch convenience operator is reasonable,
but benchmark it against existing buffering and Runic's BatchBridgeSnapshots.

Both [cache](../../src/DynamicData/Cache/ObservableCacheEx.BufferInitial.cs) and
[list](../../src/DynamicData/List/ObservableListEx.BufferInitial.cs) BufferInitial
still use recurring Buffer windows followed by Take(1)/Concat. A one-shot initial
buffer is a better candidate than clamping all tiny intervals to one millisecond.
Specify whether the timer starts at subscription or first actual data; the
existing source-loading gate is part of that behavior. Own and dispose the timer,
flush exactly once on completion, propagate errors and prevent post-disposal
emission. Test virtual time boundaries and a small bounded real-scheduler
allocation check in both flavors.

## 7. Aggregation and numerical behavior

Relevant sources: [#896](https://github.com/reactivemarbles/DynamicData/issues/896),
[PR #1167](https://github.com/reactivemarbles/DynamicData/pull/1167),
[#954](https://github.com/reactivemarbles/DynamicData/issues/954),
[#1196](https://github.com/reactivemarbles/DynamicData/issues/1196),
[merged Avg tests #1168](https://github.com/reactivemarbles/DynamicData/pull/1168).

Refresh-aware aggregation must retain the **previous projected value**, not merely
the mutable object. When an item has already mutated, the object can no longer
tell an accumulator what to subtract. #1167 uses a per-key dictionary or ordered
list of cached projections and offers SumImmutable as a lower-state fast path.

This is useful for live counters or totals over mutable rows. It is less urgent
for the example's immutable replacement DTOs. Port it only with an explicit
behavior choice, numeric overflow/nullable coverage and allocation benchmarks.
Maximum (#896), MaximumBy (#954), Avg and StdDev are independent consumers of
this design concern; fixing Sum does not fix all of them. Start with a correct
recompute path for uncommon extrema changes, then optimize if measured cost
requires a multiset or priority structure with stale-entry handling.

The fork already differs significantly from #1196's affected upstream formula:
[StdDevEx](../../src/DynamicData/Aggregation/StdDevEx.cs) tracks central moments,
uses double for floating selectors and decimal for integral/decimal selectors,
and handles add/remove with a sample divisor inside the square root. Thus the
exact catastrophic-cancellation formula reported upstream is no longer present.

Do not interpret that as exhaustive numerical proof. Add the reported float
`100000,100001,100002` and double `1e8,1e8+1,1e8+2` pipelines; assert deviation
one and translation invariance with centered reference inputs. Add randomized
removal/replacement sequences, large offsets, nonfinite-input policy and decimal
range coverage. Existing integral-offset tests are useful but do not substitute
for those floating regressions. Refresh handling remains a separate gap.

## 8. Features worth deferring or keeping small

The [complete open-item table](open-assessment.md) contains an individual approach
for all 35 issues and 23 PRs. The lower-priority themes are:

| Proposal | Decision and implementation idea |
| --- | --- |
| [Orchestrator #1114](https://github.com/reactivemarbles/DynamicData/pull/1114) | Useful long-term consolidation of child subscriptions, queue routing and completion. First pin those contracts, then migrate one operator group at a time; avoid a broad refactor while applying overlapping fixes. |
| [Runtime notification casts #1156](https://github.com/reactivemarbles/DynamicData/issues/1156) | A concrete gap remains: ExpressionBuilder checks the property's declared type. Select notification support from the runtime chain target, preserving conversion, null, replacement and cleanup behavior. |
| [Ambiguity tests #1160](https://github.com/reactivemarbles/DynamicData/issues/1160) | Useful now. Compile small consumers outside DynamicData namespaces; reflection candidate scanning alone cannot reproduce C# overload resolution. |
| [SortedItems #820](https://github.com/reactivemarbles/DynamicData/issues/820) | Small useful test helper. Expose the last ordered snapshot or use the last sorted message directly; update public baselines if adding API. |
| [Default comparer #949](https://github.com/reactivemarbles/DynamicData/issues/949) | Small convenience overload with inference/ambiguity tests; no new algorithm required. |
| [External TransformMany invalidation #899](https://github.com/reactivemarbles/DynamicData/issues/899) | Prefer child changesets; add a forced retransform signal only for a measured scenario with defined child-key collisions. |
| [Inline scheduler #981](https://github.com/reactivemarbles/DynamicData/issues/981) | Runic has a model sequencer. Splitting only updateAction onto a scheduler can reorder removals and disposal; use explicit stages first. |
| [Composable Sort #963](https://github.com/reactivemarbles/DynamicData/issues/963) | Preserve composition; benchmark shared sorting before changing API policy. Runic already has a suitable bounded viewport path. |
| [TransformVirtual #1174](https://github.com/reactivemarbles/DynamicData/issues/1174) | Experimental. The posted wrapper still creates Transform state per Connect subscriber and can recreate object identities during enumeration. Define pure projection, identity, Preview and disposal before adoption. |
| [LINQ #836](https://github.com/reactivemarbles/DynamicData/issues/836) | API convenience with substantial ambiguity risk. Keep aliases opt-in; no present Runic need. |
| [Bidirectional BindingList #853](https://github.com/reactivemarbles/DynamicData/issues/853) | Low relevance to command-based Runic editing. A future native adapter needs origin suppression and explicit feedback ownership. |
| [R3 #887](https://github.com/reactivemarbles/DynamicData/issues/887) | Different observable model. Defer a third ecosystem; the external port linked in comments was not audited. |
| [Global imports #1151](https://github.com/reactivemarbles/DynamicData/pull/1151), [#1197](https://github.com/reactivemarbles/DynamicData/pull/1197) | Overlapping mechanical changes; select one later and keep opt-in aliases local. They add merge conflict risk during operator ports. |
| [Legacy profile cleanup #1195](https://github.com/reactivemarbles/DynamicData/pull/1195) | Small optional removal after confirming no consumer relies on the excluded project; retain maintained benchmarks. |
| [Release 9.5 #1165](https://github.com/reactivemarbles/DynamicData/pull/1165) | Reference individual backports; do not import a 9.x release policy into this .NET 10 fork. |

## 9. Historical work that still matters

Historical states can hide useful work. The full catalogs retain all entries,
and this review identified the following additional considerations:

| Historical topic | Runic assessment |
| --- | --- |
| [AOT #983](https://github.com/reactivemarbles/DynamicData/issues/983), [#1086](https://github.com/reactivemarbles/DynamicData/issues/1086) | The fork enables AOT/trim analysis and has BindingList annotations. AOT compatibility flags are not an executed native publish proof. Add small native consumers of the actual cache/viewport and property-observation paths in both flavors when validating implementation ports. |
| [Async disposal #990](https://github.com/reactivemarbles/DynamicData/issues/990) | Cache `AsyncDisposeMany` is already present: public/internal APIs and UnitTests/IntegrationTests exist. Use its separate disposals-completed signal for awaited shutdown of owned rows and verify completion/error semantics. The internal replacement check uses value equality despite the public reference-identity contract, so equal-valued distinct replacements need a targeted regression/fix. List parity is a separate optional feature; do not infer absence from the different `DisposeManyAsync` spelling. |
| [Expiration retention #1025](https://github.com/reactivemarbles/DynamicData/issues/1025) | Preserve fork commits `bca31a2a` and `4effb83c` protecting callbacks during subscription/source disposal. Retention and late-callback regressions remain relevant to queue/expiration ports. |
| [Custom key comparer PR #373](https://github.com/reactivemarbles/DynamicData/pull/373) | Closed unmerged; source-only comparer behavior does not solve equality consistently through downstream operators. Normalize Runic domain keys first; defer broad comparer propagation. |
| [Strict disposal PR #516](https://github.com/reactivemarbles/DynamicData/pull/516) | Ownership reference, not an automatic port. A per-presentation consumer should dispose its owned transformed proxies rather than shared source rows. |
| [Property error routing PR #1127](https://github.com/reactivemarbles/DynamicData/pull/1127) | Current queue/property implementation has diverged. Preserve explicit getter/observer/error-handler tests rather than restoring a catch based only on a closed proposal. |

Recent improvements already represented in the fork should not be treated as
unfinished simply because a related issue was open in a cached web listing:

| Fix | Evidence / retained behavior |
| --- | --- |
| [Switch ownership #1188](https://github.com/reactivemarbles/DynamicData/pull/1188) | Commit `bd9070ae`; preserve exclusive subscriptions and reentrant source selection |
| [Property cleanup #1193](https://github.com/reactivemarbles/DynamicData/pull/1193) | Commit `76cd4514`; dispose partially initialized subscriptions on failure |
| [Conversion evaluation #1194](https://github.com/reactivemarbles/DynamicData/pull/1194) | Commit `e05d9194`; value-changing conversions are evaluated rather than ignored |
| [Optional initialization #1190](https://github.com/reactivemarbles/DynamicData/pull/1190) | Commit `3d213602`; serialize initial value with arriving changes |
| [Named timer BatchIf #1189](https://github.com/reactivemarbles/DynamicData/pull/1189) | Commit `c7787940`; retain named-argument call compatibility |
| [Legacy factory validation #1185](https://github.com/reactivemarbles/DynamicData/pull/1185) | Commit `95cb78cb`; validate before forwarding |
| [Filtering assertions #1186](https://github.com/reactivemarbles/DynamicData/pull/1186) | Commit `a7e68bea`; tests reject stale extras rather than checking only containment |
| [StdDev divisor #1191](https://github.com/reactivemarbles/DynamicData/pull/1191) | Commit `b8e09eaa` plus fork central-moment implementation; retain numerical regressions |

## 10. Dependency and CI disposition

The fork's workflow still pins checkout v6, setup-dotnet v5.3.0 and
upload-artifact v4.6.2. Review [#1089](https://github.com/reactivemarbles/DynamicData/issues/1089),
[#1090](https://github.com/reactivemarbles/DynamicData/pull/1090),
[#1121](https://github.com/reactivemarbles/DynamicData/pull/1121) and
[#1172](https://github.com/reactivemarbles/DynamicData/pull/1172) as small fork
workflow changes. The old issue's platform deprecation dates were not independently
verified here. The actionable evidence is the current old upload pin and the
need to validate a supported version against real package/test artifacts.

The remaining Renovate proposals largely target upstream's different build:

| Dependency proposals | Fork disposition |
| --- | --- |
| [BCL #1103](https://github.com/reactivemarbles/DynamicData/pull/1103), [monorepo #1117](https://github.com/reactivemarbles/DynamicData/pull/1117) | No current direct Bcl.AsyncInterfaces reference; SourceLink already 10.0.401. Do not add compatibility packages to mimic upstream's framework matrix. |
| [Verify.Xunit #1104](https://github.com/reactivemarbles/DynamicData/pull/1104), [#1109](https://github.com/reactivemarbles/DynamicData/pull/1109) | Neither applies: TUnit plus PublicApiGenerator and the fork's own approval helper replace that integration. |
| [Test SDK #1105](https://github.com/reactivemarbles/DynamicData/pull/1105), [coverlet #1108](https://github.com/reactivemarbles/DynamicData/pull/1108) | Main tests use Microsoft.Testing.Platform/TUnit and already reference Microsoft.Testing.Extensions.CodeCoverage 18.11.2. Do not add an unused VSTest collector. |
| [Bogus #1107](https://github.com/reactivemarbles/DynamicData/pull/1107) | Both test flavors already reference 35.*. No new operator feature follows from this dependency change. |

## Implementation acceptance checklist

These checks apply when implementing the proposed changes, not as claims that
they were run for this research:

1. Start with a focused regression showing the actual fork failure or missing
   contract; use virtual time and explicit synchronization.
2. Modify shared source for both flavors and preserve Runic package IDs and
   .NET 10 API policy. Translate upstream test APIs instead of adding a second
   runner.
3. Run focused operator/lifecycle and external consumer compilation checks.
   Use ordered replay assertions and exact final contents for identity/move work.
4. Review both .NET 10 API baselines for deliberate changes. Verify Primitives
   8.4 and 9.0 and Linux/Windows CI for the final implementation.
5. Measure benchmarks appropriate to the change: source size, visible window,
   changes emitted, allocation and bridge snapshot fallback. Claim no native
   input-to-paint improvement until the separate end-to-end benchmark proves it.
6. Check disk space before larger matrices; reuse the SDK's locked environment
   via `direnv exec ../runic-sdk ...` as documented in CONTRIBUTING. Preserve
   current disposal regressions and remove only task-owned temporary outputs.

All individual source links, dates, head commits, categories and proposed
approaches remain available in [inventory.json](inventory.json) and the
[open assessment](open-assessment.md). The full historical catalogs are
[issues](issues.md) and [pull requests](pull-requests.md).
