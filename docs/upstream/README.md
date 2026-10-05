# DynamicData upstream research

Collected on **2026-10-05** for the Runic-Artifex .NET 10 fork.

This directory is a dated research snapshot, not current canonical planning or
completion state. The agreed [maintenance policy](../maintenance.md) governs
integration; the [fork difference register](../fork-differences.md) records
intentional adaptations and their retirement conditions. Verify research findings
against the current branch before implementing them.

Start with the [recommendations and implementation investigation](analysis.md).
The [open-item assessment](open-assessment.md) gives every open issue and PR a
summary, usefulness assessment, fork status and implementation approach.

| Catalog | Total | Open | Closed without merge | Merged |
| --- | ---: | ---: | ---: | ---: |
| [Issues](issues.md) | 409 | 35 | 374 | — |
| [Pull requests](pull-requests.md) | 703 | 23 | 106 | 574 |

The historical catalogs include links and conservative scope summaries for all
available records. Historical summaries are based on titles; detailed code and
discussion investigation covers the current backlog and selected relevant
historical work. See the analysis for evidence and limits.

[inventory.json](inventory.json) contains the same records with labels, dates,
PR head/merge commits, categories, exact merge ancestry and authored assessments.
[assessments.json](assessments.json) is the editable research input. These files
do not change canonical planning state.

To refresh the generated catalogs, with Node and authenticated `gh` available:

```sh
node docs/upstream/refresh.mjs
```

This reads GitHub and the local Git history and rewrites `inventory.json`,
`issues.md`, `pull-requests.md` and `open-assessment.md`. It reports any newly open
items without an authored assessment. Update `assessments.json`, this index and
`analysis.md` after reviewing changes; the script does not refresh the prose
investigation. It creates no issues, PRs, comments or Git commits.
