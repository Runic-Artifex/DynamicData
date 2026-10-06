# Building the Runic fork

Follow the [maintenance guide](docs/maintenance.md) for upstream syncs, the
monthly upstream review, validation and releases, and update the
[fork difference register](docs/fork-differences.md) when changing an
intentional adaptation. Agent instructions are in [AGENTS.md](AGENTS.md).

Install the .NET SDK pinned in `global.json` (10.0.401). Both libraries and all
tests target .NET 10 only. On the Runic development desktop, reuse the SDK
workspace's locked shell: `direnv exec <path-to>/runic-sdk dotnet ...`.

```sh
dotnet build src/DynamicData.sln -c Release
dotnet src/DynamicData.Tests/bin/Release/net10.0/DynamicData.Tests.dll --maximum-parallel-tests 8
dotnet src/DynamicData.Reactive.Tests/bin/Release/net10.0/DynamicData.Reactive.Tests.dll --maximum-parallel-tests 8
dotnet pack src/DynamicData.sln -c Release --no-build -o artifacts/packages
```

`Runic.DynamicData` uses ReactiveUI.Primitives directly; `Runic.DynamicData.Reactive`
compiles the shared implementation with System.Reactive conventions. Preserve both
flavors when changing shared operators. The default dependency is Primitives 9.0.0;
`-p:ReactiveUIPrimitivesVersion=8.4.0` builds against ReactiveUI 25's generation.

Use awaited TUnit assertions, virtual time and explicit synchronization. Review
public API diffs before changing the .NET 10 approval baselines. Add focused
regressions for values, completion, errors and subscription disposal when changing
an operator. Performance work should include a relevant benchmark.

CI builds and tests both flavors and dependency generations on Linux and Windows.
The manual release workflow attaches the branded packages to a GitHub release; it
does not publish under upstream package names. Download the assets into a local
NuGet feed to consume the fork. This fork will not be proposed upstream.
