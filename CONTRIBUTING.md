# Building the Runic fork

Install .NET SDK 10.0.401, pinned in `global.json`. Both libraries and all tests
target .NET 10 only. On the Runic development desktop, reuse the SDK's locked
shell: `direnv exec ../runic-sdk dotnet ...`.

```sh
dotnet build src/DynamicData.sln -c Release
dotnet src/DynamicData.Tests/bin/Release/net10.0/DynamicData.Tests.dll --maximum-parallel-tests 8
dotnet src/DynamicData.Reactive.Tests/bin/Release/net10.0/DynamicData.Reactive.Tests.dll --maximum-parallel-tests 8
dotnet pack src/DynamicData.sln -c Release --no-build -o artifacts/packages
```

`Runic.DynamicData` uses ReactiveUI.Primitives directly; `Runic.DynamicData.Reactive`
compiles the shared implementation with System.Reactive conventions. Preserve both
flavors when changing shared operators. The default dependency is Primitives 9.0.0
for current Runic/ReactiveUI. `-p:ReactiveUIPrimitivesVersion=8.4.0` tests the
ReactiveUI 25 dependency generation on the same .NET 10 target.

Use native awaited TUnit assertions, virtual time and explicit synchronization.
Review public API diffs before changing the .NET 10 approval baselines. Add focused
regressions for values, completion, errors and subscription disposal when changing
an operator. Performance work should include a relevant benchmark.

CI builds both flavors on Linux and Windows. The manual release workflow attaches
the branded NuGet packages to a GitHub release; it does not publish under upstream
package names. Download the assets into a local NuGet feed when consuming the fork.
This fork is maintained for Runic and will not be proposed upstream.
