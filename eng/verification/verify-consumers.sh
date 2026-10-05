#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repo_root"
mode=${1:-managed}
if [[ "$mode" != compile && "$mode" != managed && "$mode" != native ]]; then
  echo 'Usage: verify-consumers.sh [compile|managed|native <host-runtime-id>]' >&2
  exit 2
fi
if [[ "$mode" == native && $# != 2 ]]; then
  echo 'Native verification requires the host runtime ID (for example linux-x64).' >&2
  exit 2
fi

output="$repo_root/artifacts/verification"
mkdir -p "$output"
{
  git rev-parse HEAD
  dotnet --version
  uname -sm
} > "$output/$mode-environment.txt"

versions=(8.4.0 9.0.0)
if [[ "$mode" == native ]]; then
  versions=(9.0.0)
fi
for version in "${versions[@]}"; do
  for flavor in Primitives Reactive; do
    name="${flavor}Consumer"
    project="eng/verification/$name/$name.csproj"
    prefix="$output/$mode-$flavor-$version"
    if [[ "$mode" == native ]]; then
      publish="$output/native/$flavor"
      dotnet publish "$project" -c Release -r "$2" --self-contained true \
        -p:PublishAot=true -p:ReactiveUIPrimitivesVersion="$version" \
        -p:BuildInParallel=false -p:IlcSingleThreaded=true -m:2 -o "$publish" 2>&1 | tee "$prefix-build.log"
      executable="$publish/$name"
      if [[ "$2" == win-* ]]; then executable+=.exe; fi
      "$executable" 2>&1 | tee "$prefix-run.log"
    else
      dotnet build "$project" -c Release -p:ReactiveUIPrimitivesVersion="$version" \
        -p:BuildInParallel=false -m:2 2>&1 | tee "$prefix-build.log"
      if [[ "$mode" == managed ]]; then
        dotnet "eng/verification/$name/bin/Release/net10.0/$name.dll" 2>&1 | tee "$prefix-run.log"
      fi
    fi
  done
done
