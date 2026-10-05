#!/usr/bin/env python3
"""Verify immutable Runic DynamicData release assets with application consumers."""

import argparse
import base64
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import urllib.request
import xml.etree.ElementTree as ET
import zipfile


ROOT = Path(__file__).resolve().parents[2]
ACCEPTANCE = ROOT / "eng" / "acceptance"
OUTPUT = ROOT / "artifacts" / "acceptance"
NUGET_NAMESPACE = {"n": "http://schemas.microsoft.com/packaging/2013/05/nuspec.xsd"}
FLAVORS = (
    ("Primitives", "Runic.DynamicData", "ReactiveUI.Primitives"),
    ("Reactive", "Runic.DynamicData.Reactive", "ReactiveUI.Primitives.Reactive"),
)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command: list[str], log: Path) -> None:
    print("+", " ".join(str(part) for part in command), flush=True)
    result = subprocess.run(command, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    log.write_text(result.stdout)
    if result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {log}")


def fetch_asset(tag: str, package: str, version: str, expected: str, feed: Path) -> Path:
    destination = feed / f"{package}.{version}.nupkg"
    if not destination.exists() or digest(destination) != expected:
        url = f"https://github.com/Runic-Artifex/DynamicData/releases/download/{tag}/{destination.name}"
        print(f"Downloading {url}", flush=True)
        with urllib.request.urlopen(url, timeout=60) as response:
            destination.write_bytes(response.read())
    actual = digest(destination)
    if actual != expected:
        raise ValueError(f"Hash mismatch for {destination.name}: expected {expected}, got {actual}")
    return destination


def inspect_package(path: Path, expected_id: str, version: str) -> str:
    with zipfile.ZipFile(path) as archive:
        metadata = ET.fromstring(archive.read(f"{expected_id}.nuspec"))
        frameworks = {name.split("/")[1] for name in archive.namelist() if name.startswith("lib/") and name.endswith(".dll")}
    actual_id = metadata.findtext("n:metadata/n:id", namespaces=NUGET_NAMESPACE)
    actual_version = metadata.findtext("n:metadata/n:version", namespaces=NUGET_NAMESPACE)
    repository = metadata.find("n:metadata/n:repository", NUGET_NAMESPACE)
    if actual_id != expected_id or actual_version != version:
        raise ValueError(f"Unexpected package identity in {path.name}: {actual_id} {actual_version}")
    if frameworks != {"net10.0"}:
        raise ValueError(f"{path.name} must contain only a net10.0 library, found {sorted(frameworks)}")
    if repository is None or repository.get("url") != "https://github.com/Runic-Artifex/DynamicData" or not repository.get("commit"):
        raise ValueError(f"{path.name} has no Runic repository commit provenance")
    return repository.get("commit")


def verify_graph(project: Path, package: str, version: str, primitives: str, reactive: bool, asset: Path) -> None:
    assets = json.loads((project.parent / "obj" / "project.assets.json").read_text())
    libraries = {identity.rsplit("/", 1)[0].casefold(): (identity.rsplit("/", 1)[1], library) for identity, library in assets["libraries"].items()}
    forbidden = {
        "dynamicdata",
        "dynamicdata.reactive",
        "runic.dynamicdata.reactive" if not reactive else "runic.dynamicdata",
        "reactiveui.primitives.reactive" if not reactive else "reactiveui.primitives",
    }
    if not reactive:
        forbidden.add("system.reactive")
    expected_sha512 = base64.b64encode(hashlib.sha512(asset.read_bytes()).digest()).decode()
    restored_version, restored = libraries.get(package.casefold(), (None, {}))
    primitives_version, primitives_library = libraries.get(primitives.casefold(), (None, {}))
    if restored_version != version or restored.get("type") != "package" or restored.get("sha512") != expected_sha512:
        raise ValueError(f"{project} did not restore the exact verified {package}/{version} bytes")
    if primitives_version != "9.0.0" or primitives_library.get("type") != "package":
        raise ValueError(f"{project} did not restore the shipping {primitives}/9.0.0 cohort")
    if forbidden & set(libraries):
        raise ValueError(f"Wrong package graph for {project}: {sorted(forbidden & set(libraries))}")
    projects = [name for name, (_, library) in libraries.items() if library.get("type") == "project"]
    if projects:
        raise ValueError(f"{project} resolved project inputs instead of packages: {projects}")
    target = assets["targets"].get("net10.0", {})
    if f"{package}/{version}" not in target:
        raise ValueError(f"{project} did not restore {package}/{version}")
    project_file = project.read_text()
    if "ProjectReference" in project_file:
        raise ValueError(f"{project} must consume the package, not a project reference")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("managed", "native"))
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--tag", help="Immutable DynamicData GitHub release tag")
    source.add_argument("--asset-directory", type=Path, help="Exact CI candidate package directory; not a published release")
    parser.add_argument("--version", required=True, help="The identical version of both package assets")
    parser.add_argument("--primitives-sha256", required=True)
    parser.add_argument("--reactive-sha256", required=True)
    parser.add_argument("--rid", help="Required target runtime identifier for native execution")
    args = parser.parse_args()
    if args.mode == "native" and not args.rid:
        parser.error("native mode requires --rid")
    if args.mode == "managed" and args.rid:
        parser.error("--rid applies only to native mode")

    OUTPUT.mkdir(parents=True, exist_ok=True)
    suffix = f"-{args.rid}" if args.mode == "native" else ""
    report_path = OUTPUT / f"results-{args.mode}{suffix}.json"
    report = {
        "asset_source": "github-release" if args.tag else "ci-candidate",
        "tag": args.tag,
        "version": args.version,
        "environment": {
            "dotnet_sdk": subprocess.check_output(["dotnet", "--version"], text=True).strip(),
            "host": platform.platform(),
            "rid": args.rid,
        },
        "assets": [],
        "checks": [],
        "completed": False,
    }
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    feed = OUTPUT / "release-assets" / args.version if args.tag else args.asset_directory.resolve()
    feed.mkdir(parents=True, exist_ok=True)
    assets = []
    for (_, package, _), expected in zip(FLAVORS, (args.primitives_sha256, args.reactive_sha256), strict=True):
        if args.tag:
            asset = fetch_asset(args.tag, package, args.version, expected, feed)
        else:
            candidate = args.asset_directory / f"{package}.{args.version}.nupkg"
            if not candidate.is_file():
                raise ValueError(f"Missing exact CI candidate asset: {candidate}")
            asset = candidate
            actual = digest(asset)
            if actual != expected:
                raise ValueError(f"Hash mismatch for {asset.name}: expected {expected}, got {actual}")
        commit = inspect_package(asset, package, args.version)
        assets.append({"id": package, "version": args.version, "sha256": expected, "repository_commit": commit, "path": str(asset)})

    commits = {item["repository_commit"] for item in assets}
    if len(commits) != 1:
        raise ValueError(f"The package pair has mismatched source commits: {sorted(commits)}")

    report["assets"] = assets
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    versions = ("9.0.0",)
    for primitives_version in versions:
        for flavor, package, primitives in FLAVORS:
            project = ACCEPTANCE / f"{flavor}Consumer" / f"{flavor}Consumer.csproj"
            prefix = f"{args.mode}-{flavor}-{primitives_version}"
            restore = ["dotnet", "restore", project, "--source", feed, "--source", "https://api.nuget.org/v3/index.json",
                       "-p:RestorePackagesPath=" + str(OUTPUT / "packages"), "-p:RunicDynamicDataVersion=" + args.version,
                       "-p:ReactiveUIPrimitivesVersion=" + primitives_version, "-p:RestoreIgnoreFailedSources=false"]
            common = ["-c", "Release", "--no-restore", "-m:2", "-warnaserror",
                      "-p:RunicDynamicDataVersion=" + args.version, "-p:ReactiveUIPrimitivesVersion=" + primitives_version,
                      "-p:BuildInParallel=false", "-p:EnableTrimAnalyzer=true", "-p:EnableAotAnalyzer=true",
                      "-p:TreatWarningsAsErrors=true", "-p:TrimmerSingleWarn=false"]
            if args.mode == "native":
                restore.extend(["--runtime", args.rid, "-p:PublishAot=true"])
            run(restore, OUTPUT / f"{prefix}.restore.log")
            asset = next(item["path"] for item in assets if item["id"] == package)
            verify_graph(project, package, args.version, primitives, flavor == "Reactive", Path(asset))
            if args.mode == "managed":
                run(["dotnet", "build", project, *common], OUTPUT / f"{prefix}.build.log")
                executable = project.parent / "bin" / "Release" / "net10.0" / f"{flavor}Consumer.dll"
                run(["dotnet", executable], OUTPUT / f"{prefix}.run.log")
            else:
                publish = OUTPUT / "native" / flavor
                run(["dotnet", "publish", project, *common, "-r", args.rid, "--self-contained", "true", "-o", publish,
                     "-p:PublishTrimmed=true", "-p:TrimMode=full", "-p:PublishAot=true", "-p:IlcSingleThreaded=true"], OUTPUT / f"{prefix}.publish.log")
                executable = publish / (f"{flavor}Consumer.exe" if args.rid.startswith("win-") else f"{flavor}Consumer")
                run([str(executable)], OUTPUT / f"{prefix}.run.log")
            report["checks"].append({"name": prefix, "package": package, "primitives": primitives, "passed": True})
            report_path.write_text(json.dumps(report, indent=2) + "\n")
    report["completed"] = True
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Package acceptance passed: {report_path}", flush=True)


if __name__ == "__main__":
    main()
