# External consumer and NativeAOT probes

These two .NET 10 executables share application code in
`Runic.ExternalConsumer`, outside both DynamicData namespaces. One imports the
Primitives flavor; the other imports the System.Reactive flavor. They are excluded
from the production solution and package output.

The probes check ordered cache binding, property-driven resorting, zero-sized
viewports and their total-count metadata, nested property observation with a
numeric conversion, observer detachment, disposal of transformed owned rows,
and the modern virtual-context `Switch` overload's return-type inference and
source handoff. The specialized `Switch` deliberately returns a base keyed
changeset; it does not preserve virtualization metadata. Applications needing
per-presentation ordering should switch keyed feeds before `SortAndVirtualize`.

Run from a checkout containing the final integrated viewport and Switch changes.
The baseline before those changes is expected to fail the corresponding runtime
checks. Compile checks alone do not prove that C# selected the specialized
`Switch`: the executable also checks the inferred observable element type.

On the Runic desktop, use the SDK workspace's locked Nix shell. The script runs
sequentially, with MSBuild limited to two nodes and parallel project builds
disabled. Native compilation also uses the SDK's `IlcSingleThreaded` option.
`compile` checks both flavors against Primitives 8.4.0 and 9.0.0;
`managed` also executes those four consumers. `native` publishes and executes
both flavors against default Primitives 9.0.0 using the supplied host runtime ID.
Native execution must happen on the target operating system and architecture.

```sh
direnv exec /home/viktor/Development/RunicArtifex/runic-sdk bash eng/verification/verify-consumers.sh compile
direnv exec /home/viktor/Development/RunicArtifex/runic-sdk bash eng/verification/verify-consumers.sh managed
df -h . /tmp
direnv exec /home/viktor/Development/RunicArtifex/runic-sdk bash eng/verification/verify-consumers.sh native linux-x64
```

Inspect all warnings in the native publish logs. No trimming/AOT warnings are
suppressed, and no reflection preservation roots are added to conceal a library
failure. Native success establishes these exercised paths only, rather than
compatibility of every operator, model shape or platform. The property expression
paths deliberately execute in the native binary; analyzer flags alone do not
establish that they work.

`artifacts/verification/` retains the tested source SHA, SDK version, host,
build/run logs and native executables. `bin/` and `obj/` are ignored incremental
outputs. Preserve useful failure evidence; remove task-owned native executables
and temporary outputs after verification consumers finish. This script does not
run the repository's complete test matrix, publish packages or create releases.
