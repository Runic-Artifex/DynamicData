#!/usr/bin/env bash
# Offline stand-in for `dotnet nuget push`. It records each push and publishes the
# package into the mock nuget.org case directory, adding a repository signature
# entry the way nuget.org does. MOCK_DOTNET_FAIL=<package id> answers 403 for it.
set -euo pipefail

case_dir="${MOCK_NUGET_CASE:?MOCK_NUGET_CASE is required}"
[[ "${1:-}" == 'nuget' && "${2:-}" == 'push' ]] || {
  printf 'unexpected dotnet invocation: %q\n' "$*" >&2
  exit 1
}
package="$3"
shift 3
source='' key=''
while (($#)); do
  case "$1" in
    --source) source="$2"; shift 2 ;;
    --api-key) key="$2"; shift 2 ;;
    *) printf 'unexpected dotnet nuget push option: %s\n' "$1" >&2; exit 1 ;;
  esac
done
[[ "$source" == 'https://api.nuget.org/v3/index.json' ]] || { echo "wrong push source: $source" >&2; exit 1; }
[[ -n "$key" ]] || { echo 'missing API key' >&2; exit 1; }

python3 - "$package" "$case_dir" "${MOCK_DOTNET_FAIL:-}" <<'PY'
import os
import sys
import xml.etree.ElementTree as ET
import zipfile

package, case, fail = sys.argv[1:]
with zipfile.ZipFile(package) as archive:
    nuspec = next(name for name in archive.namelist() if '/' not in name and name.endswith('.nuspec'))
    metadata = {child.tag.rsplit('}', 1)[-1]: child.text for child in ET.fromstring(archive.read(nuspec))[0]}
    package_id, version = metadata['id'], metadata['version']
    if package_id == fail:
        raise SystemExit('Response status code does not indicate success: 403 (Forbidden).')
    directory = os.path.join(case, 'packages', package_id.lower())
    os.makedirs(directory, exist_ok=True)
    with zipfile.ZipFile(os.path.join(directory, f'{version.lower()}.nupkg'), 'w') as published:
        for item in archive.infolist():
            published.writestr(item, archive.read(item))
        published.writestr('.signature.p7s', b'repository signature')
os.makedirs(os.path.join(case, 'owners'), exist_ok=True)
with open(os.path.join(case, 'owners', package_id.lower()), 'w', encoding='utf-8') as owners:
    owners.write('runic-artifex\n')
with open(os.path.join(case, 'pushes'), 'a', encoding='utf-8') as pushes:
    pushes.write(f'{package_id} {version}\n')
PY
