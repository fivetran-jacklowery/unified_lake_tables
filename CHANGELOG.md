# Changelog

All notable changes to this project are documented here.

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
