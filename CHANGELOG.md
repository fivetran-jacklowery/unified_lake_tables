# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Added

- **Pattern-based source namespace discovery.** `config.yaml` now accepts
  `source_namespace_pattern` (a glob, e.g. `"tenant_*"`) as an alternative to
  hand-enumerating `source_namespaces`, resolved against the catalog's real
  namespaces at run time via `resolve_source_namespaces()` (`fnmatch`
  matching against `RestCatalog.list_namespaces()`, excluding the target
  namespace, always logged, `SystemExit` if fewer than 2 namespaces match).
  Directly analogous to dbt-utils' `get_relations_by_pattern` macro. Exactly
  one of `source_namespaces` / `source_namespace_pattern` must be set --
  `load_config()` enforces this. Threaded through
  `register_consolidation.py`'s `run()`, `verify_consolidation.py`'s
  `main()`, `config.example.yaml`, `docs/HOW_IT_WORKS.md`, `README.md`,
  `SKILL.md`, and the AWS Lambda example (`SOURCE_NAMESPACE_PATTERN` env
  var). Validated live against a real Polaris catalog (`synth_src_0*` and
  `synth_wide_*` patterns resolving to the expected exact namespace sets;
  confirmed `SystemExit` on a too-narrow pattern).
- `register_consolidation.py`'s core loop extracted into a standalone
  `run(cfg, tables=None) -> dict` function (returning a JSON-serializable
  summary), so `main()` is now a thin CLI wrapper and the same logic can be
  called from other entry points (e.g. the AWS Lambda example's
  `lambda_function.py`).
- `examples/aws-lambda/`: a real, working example of running
  `register_consolidation.py` as a scheduled AWS Lambda function (handler,
  packaging script, IAM/EventBridge setup, and a walkthrough README),
  including a Docker-based cross-platform build path for producing a
  correct Linux/arm64 deployment package regardless of build-host OS.

### Fixed

- `verify_consolidation.py` would raise `KeyError` on `cfg["source_namespaces"]`
  if a config used the new `source_namespace_pattern` instead of an explicit
  list (the pattern was never resolved before use). Fixed by calling the same
  `resolve_source_namespaces()` helper `register_consolidation.py`'s `run()`
  uses, immediately after opening the catalog connection.

### Confirmed (previously only hypothesized)

- The README's Troubleshooting entry "Everything works except reading actual
  data ... outside its own default storage location" -- previously written
  as a known catalog-integration risk in the abstract -- was reproduced live
  during this release's testing: an independent row-count check in
  `verify_consolidation.py`, run against a real consolidated table, hit
  `OSError: ... AWS Error ACCESS_DENIED during HeadObject operation` reading
  a spliced-in Parquet file that physically lives under a different source
  namespace's default storage location. This is not a regression from the
  pattern-discovery work above -- pattern resolution and table discovery
  both completed correctly first -- it's confirmation of a pre-existing,
  previously-untested gap in how vended, per-table storage credentials
  interact with this tool's whole-point cross-namespace file splicing. See
  the README's Troubleshooting section for the current guidance (broaden the
  reading engine's catalog-integration storage-location allowlist).

## [1.0.0]

Initial public-facing release, adapted from an internal R&D reference
implementation (`register_consolidated_v1.py` through `v7.py`) that
validated this technique end to end against a real Fivetran-managed
destination, Polaris Iceberg catalog, and downstream Snowflake/DuckDB
reads.

### Included

- `scripts/register_consolidation.py`: no-rewrite consolidation of N source
  namespaces' structurally-identical tables into one target table per table
  name, via direct manifest/partition-value injection. Includes the full
  validated three-layer schema-drift fix (reserved out-of-band field id for
  the target's own bookkeeping column, widen-for-free on non-colliding new
  columns, delta-scoped rewrite-on-collision for genuinely colliding new
  columns) and idempotent re-runs.
- `scripts/verify_consolidation.py`: independent post-run sanity checks --
  per-source row-count reconciliation, duplicate-file detection in the
  target manifest, and a basic partition-pruning check.
- Vended-credential authentication to Polaris (OAuth client-credentials
  only; no static AWS keys anywhere in this repo or its scripts).
- Config-driven source discovery (`config.yaml`: explicit list of source
  namespaces, auto-discovery of table names common to all of them),
  replacing the internal reference scripts' `{prefix}_{NN}` naming-
  convention assumption.
- `docs/HOW_IT_WORKS.md`: full technical walkthrough of the mechanism.
- `.env.example`, `config.example.yaml`, `requirements.txt` (pinned to the
  exact pyiceberg/pyarrow versions this was checked against).

### Explicitly out of scope for this release

Carried forward honestly from the internal architecture review and PRD this
was adapted from -- these are not implementation gaps that got missed, they
are cases that were never tested against this specific technique and are
deliberately deferred:

- **Renamed columns, dropped columns, and changed data types on a source.**
  Only "a source adds a new nullable column" was validated. These are a
  different Iceberg schema-evolution mechanism and have not been exercised
  against this cross-table splicing pattern.
- **Merge-on-read sources (delete files).** The registration script only
  ever constructs `DataFileContent.DATA` entries. Splicing in a source that
  produces position-delete or equality-delete files would silently drop the
  delete and resurrect a logically-deleted row, with no error. Whether
  Fivetran's managed-lake writer produces delete files for any given
  connector was not checked as part of this release.
- **Order-independent collision resolution.** The rewrite-on-collision path
  picks a deterministic "owner" for a colliding field id based on the order
  sources are processed in during a given run. This is stable within a run
  but not yet a safe rule for a system where sources are added, removed, or
  reordered over time.
- **Concurrent registration runs.** Only exercised as a single, serial job
  (with internal thread-pool parallelism across sources/tables within one
  run). Overlapping runs against the same target table, or a source syncing
  mid-run, have not been tested.
- **Source-side file compaction/lifecycle awareness.** If a source's own
  routine file maintenance moves or expires the physical files a
  consolidated table's manifest points at, those references can go stale
  with nothing currently watching for it.
- **Automated recreation of downstream catalog-linked objects** (e.g. a
  Snowflake-side `ICEBERG TABLE`) after a consolidated table is rebuilt at
  the catalog level. This is a known, confirmed gap (a plain refresh fails
  with a UUID mismatch); see the README's Troubleshooting section. It's a
  manual runbook step in this release, not an automated one.
- **A fully live, end-to-end re-validation of the vended-credentials path**
  against a real Polaris catalog. The vended-credentials approach in this
  release is grounded in reading pyiceberg 0.11.1's own source code (see
  `docs/HOW_IT_WORKS.md`, "Credentials: vended, not static"), not a rerun of
  the original internal validation, which used static AWS keys against a
  sandbox with direct broad bucket access.
