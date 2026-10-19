// Copyright (c) 2011-2025 Roland Pheasant. All rights reserved.
// Roland Pheasant licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

// Runic: System, System.Collections.Generic, System.Linq and System.Threading(.Tasks) come from
// ImplicitUsings. Each project imports its Rx stack (ReactiveUI.Primitives or System.Reactive) in
// its csproj, and the DynamicData namespaces below follow the shared-source flavour.
global using System.Collections;
global using System.Collections.Concurrent;
global using System.Collections.ObjectModel;
global using System.Collections.Specialized;
global using System.ComponentModel;
global using System.Diagnostics;
global using System.Diagnostics.CodeAnalysis;
global using System.Linq.Expressions;
global using System.Reflection;
global using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
global using DynamicData.Reactive.Binding;
global using DynamicData.Reactive.Cache;
global using DynamicData.Reactive.Cache.Internal;
global using DynamicData.Reactive.Diagnostics;
global using DynamicData.Reactive.Internal;
global using DynamicData.Reactive.Kernel;
global using DynamicData.Reactive.List.Internal;
global using DynamicData.Reactive.List.Linq;
global using DynamicData.Reactive.Operators;
#else
global using DynamicData.Binding;
global using DynamicData.Cache;
global using DynamicData.Cache.Internal;
global using DynamicData.Diagnostics;
global using DynamicData.Internal;
global using DynamicData.Kernel;
global using DynamicData.List.Internal;
global using DynamicData.List.Linq;
global using DynamicData.Operators;
#endif

[assembly: InternalsVisibleTo("DynamicData.Tests")]
[assembly: InternalsVisibleTo("DynamicData.Reactive.Tests")]
[assembly: InternalsVisibleTo("DynamicData.Benchmarks")]
