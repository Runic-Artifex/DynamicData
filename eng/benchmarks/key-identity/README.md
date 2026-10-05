# RemoveKey diagnostic benchmark

This bounded console runner takes five samples for each combination of 25 or
100,000 rows, clear/reverse/shuffled removal order, and raw source or `RemoveKey`.
It warms up the projection with 1,000 rows first. It checks removal totals and
sequential index ranges; clear/reverse also check every exact removal index.
The focused regression fixtures supply the independent mixed-operation replay
oracle and front/back insertion coverage.

Run either flavor from the repository root in the locked SDK environment:

```sh
direnv exec /home/viktor/Development/RunicArtifex/runic-sdk dotnet build eng/benchmarks/key-identity/KeyIdentity.csproj -c Release -m:2 -p:ReactiveUIPrimitivesVersion=9.0.0
direnv exec /home/viktor/Development/RunicArtifex/runic-sdk dotnet eng/benchmarks/key-identity/bin/Release/net10.0/KeyIdentity.dll
direnv exec /home/viktor/Development/RunicArtifex/runic-sdk dotnet build eng/benchmarks/key-identity/KeyIdentity.csproj -c Release -m:2 -p:UseReactive=true -p:ReactiveUIPrimitivesVersion=9.0.0
direnv exec /home/viktor/Development/RunicArtifex/runic-sdk dotnet eng/benchmarks/key-identity/bin/Release/net10.0/KeyIdentity.dll
```

Local observations on 2026-10-05, NixOS, SDK 10.0.401, runtime 10.0.12,
Primitives 9.0.0, adapted from upstream PR #1192 head
`1c6fdfd9222348d43d2d12abf33f7c6222df30bc`:

| Flavor | 100,000-row order | Raw source median | RemoveKey median |
| --- | --- | ---: | ---: |
| Primitives | Clear | 8.220 ms | 39.630 ms |
| Primitives | Reverse | 1.273 ms | 25.284 ms |
| Primitives | Shuffled | 3.500 ms | 39.777 ms |
| System.Reactive | Clear | 3.303 ms | 26.623 ms |
| System.Reactive | Reverse | 1.574 ms | 9.111 ms |
| System.Reactive | Shuffled | 2.031 ms | 32.376 ms |

The 25-row RemoveKey medians were 0.009–0.053 ms. Removing 100,000 rows allocated
about 13.74 MB with RemoveKey versus 7.34 MB for the raw source; this includes
the outgoing individual list changes. The subscription retained about 9.18 MB
versus about 1 KB for the raw source, approximately 92 extra bytes per tracked
integer key. Retained-memory estimates use `GC.GetTotalMemory(true)` after
subscription to an already populated cache, excluding the existing cache.

These are bounded local diagnostics, without CPU isolation or statistical
confidence intervals. They include source mutation and observer delivery and
exclude downstream list materialization, whose shifting costs would otherwise
obscure the position index. They do not establish an end-to-end UI throughput
or a worst-case complexity guarantee. Treap operations have expected logarithmic
cost and linear worst case, with one node and one dictionary entry per key.
