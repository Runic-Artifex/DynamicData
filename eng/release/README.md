# Release asset guard

`release-assets.sh` is the narrow gate used by the manual package-release
workflow. It accepts only the two default `9.0.0` branded packages:

- `Runic.DynamicData.<version>.nupkg`, with one `ReactiveUI.Primitives`
  dependency declared at `9.0.0` (NuGet's minimum `9.0.0`).
- `Runic.DynamicData.Reactive.<version>.nupkg`, with one
  `ReactiveUI.Primitives.Reactive` dependency declared at `9.0.0` (NuGet's
  minimum `9.0.0`).

The pair must have the same version, a `net10.0` dependency group, MIT license,
and repository URL and commit matching the current workflow repository and
`GITHUB_SHA`. The publish command refuses every existing tag or release,
creates a draft targeted at that SHA, uploads the exact two assets, downloads
them for byte comparison, creates and resolves a new non-force tag at that SHA,
and only then publishes the draft. It uses the
current `GITHUB_REPOSITORY` explicitly for every GitHub operation. GitHub
publishing is restricted to a clean checkout of maintained `main` at
`GITHUB_SHA`; it also verifies that `main` remains the current default branch.
Before it creates a draft, the workflow requires a
completed successful Build workflow at that exact SHA, with the latest result of
each Linux/Windows and 8.4.0/9.0.0 matrix job succeeding. It intentionally does
not rerun CI; the maintained default branch is the supported release source.

Run the offline tests with:

```sh
bash eng/release/test-release-assets.sh
```

The tests generate small `.nupkg` fixtures and use a local `gh` mock. They do
not contact GitHub or create releases.
