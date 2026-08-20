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

### Known issue: collision-rewrite path can hit ACCESS_DENIED under vended credentials

Reproduced live (not hypothetical) by deliberately running `register_consolidation.py`
against a real two-source field-id collision (`synth_src_changing_03` and
`synth_src_changing_04` both independently added a new column that landed on
the same physical field id in a `warehouses`-style table). The no-rewrite
splice and the free widen-for-free step both completed and committed
correctly. The run then crashed inside **step 4's own idempotency check**
(`register_table()`, the `already_rewritten = target_table.scan(...)` call
right before the real per-collision rewrite) with:

```
OSError: ... AWS Error ACCESS_DENIED during HeadObject operation ...
'<source-namespace>/<table>/data/<file>.parquet' in bucket '<lake bucket>'
```

**Why this happens:** by the time step 4 runs, the target table's manifest
already contains file references spliced in from every source namespace
(that's step 2's whole job). Polaris vends AWS credentials scoped to a
table's own default storage location, not to every namespace whose files
happen to be referenced in that table's manifest. Reading the *target*
table's data -- which step 4's idempotency check does, to see whether this
collision was already resolved in a prior run -- means reading files that
physically live under a *different* namespace's prefix, which the vended
credential doesn't cover. This is the same root cause as the
`verify_consolidation.py` finding above, but a more serious instance: it's
inside the core registration script's own collision-handling path, not a
separate, optional post-run check.

**Practical impact:** on a genuine field-id collision, the run fails instead
of resolving it, contradicting the "detects and safely resolves" collision
claim in this script's own docstring. The no-rewrite splice for every
non-colliding file still commits successfully first (confirmed: all 8
sources' base data landed correctly, and the winning column's free-widen
data was correctly isolated to only its real source), so a crash here does
not corrupt what's already been written -- it just means the specific
colliding column's fresh data is left un-rewritten (schema column created,
but empty) until the run can complete successfully.

**Confirmed workaround, without touching the script:** DuckDB's Iceberg
extension, attached directly to the same Polaris REST catalog, reads these
exact same cross-namespace spliced files without issue -- confirmed live
against the very table that crashed via `pyiceberg` above (row counts and
per-column null checks across all 8 partitions succeeded via DuckDB in the
same session). Read-only verification or idempotency-style checks that need
to read across a consolidated table's full manifest should use DuckDB (or
another reader that requests/receives broader-scoped credentials) rather
than `pyiceberg`'s own `RestCatalog` client, until vended-credential storage
scoping for cross-namespace manifests is resolved upstream or worked around
in this tool directly. **Not yet changed in the script itself** -- this is
documentation of a real, reproduced gap and a validated workaround, not a
code fix.

### Fixed: copy-on-write updates/deletes were silently duplicating and resurrecting rows

Found via a piece of external feedback on this technique, then reproduced
live (not hypothetical) before being fixed. The 1.0.0 release's "Explicitly
out of scope" section named the wrong mechanism for this risk -- it said
"merge-on-read sources (delete files)," but reading Fivetran's actual
Managed Data Lake writer source (`ManagedDataLakeWriter.java`) shows there
are no delete files anywhere in that writer at all. `upsert()`, `update()`,
and `delete()` all use pure copy-on-write: the writer finds every existing
physical file that could contain an affected primary key, rewrites it in
full via an internal `UpdateWorkflow`/`DeleteWorkflow`, and atomically swaps
old file for new via Iceberg's `OverwriteFiles` transaction
(`deleteFile()` + `addFile()` in one commit). No position or equality
deletes, no `write.delete.mode`/`write.update.mode` table properties set
anywhere -- COW is the only path this writer implements.

**Why that broke this tool:** `already_registered_paths()` (as it existed
before this fix) only ever asked "is this source file path new to me,
splice it in." It never asked "did a file I already spliced in get retired
at the source." Since COW removes the old file from the source's current
snapshot the moment it rewrites it, the source itself reads correctly (new
file replaces old), but this tool's target kept referencing both -- the
stale file was never revisited or removed, so every row it held became a
permanent duplicate the moment its replacement got spliced in on a later
run, and a genuinely-updated row ended up present with both its stale AND
current values simultaneously, with nothing distinguishing which was
current.

**Reproduced live** with two minimal test connectors
(`connectors/cow_test_01`/`cow_test_02`, 30 rows each, one table) before
touching any code: baseline consolidation of 60 rows, clean. Re-synced
`cow_test_01` with a connector that re-upserts one *existing* primary key
with changed values instead of inserting a new one. Confirmed directly
against the source's Iceberg snapshot that its original single file (30
rows) was gone from the current snapshot, replaced by two new files (1 row
+ 29 rows = 30) -- real COW, not an insert. Re-running
`register_consolidation.py` unchanged spliced both new files in as "never
seen before," leaving 90 rows instead of 60: every one of `cow_test_01`'s
30 rows duplicated (the file swap touched all of them, not just the
changed one), and the one genuinely-updated row present twice --
`(green, active, v1)` and `(orange, UPDATED, v2)` simultaneously.

**The fix:** `already_registered_paths()` became
`already_registered_by_source()`, returning the actual `DataFile` objects
(not just path strings) grouped by which source they came from (via the
partition value already recorded on every spliced entry). Each source's
current file listing is now diffed both directions: new paths still splice
in exactly as before, but any previously-registered path no longer present
in that source's current snapshot is collected as orphaned and retired via
`transaction.update_snapshot().overwrite()` -- `delete_data_file()` on each
orphan, `append_data_file()` on its replacement(s), in one atomic commit.
This is a public pyiceberg 0.11.1 API, not a private hack, and it mirrors
Fivetran's own `OverwriteFiles` pattern exactly. Confirmed via the target's
own snapshot history after the fix: the two prior splice commits recorded
as `Operation.APPEND`; the retirement commit recorded as
`Operation.OVERWRITE`, 1 file deleted -- exactly the swap that was missing
before.

**Re-ran the exact broken scenario after the fix, no config changes:**
retired the 1 orphaned file, removed the 30 stale rows, table back to the
correct 60, zero duplicates, the previously-updated widget now shows only
its current value. A second run immediately after was a clean no-op (0
spliced, 0 rewritten, 0 orphaned) -- idempotency holds under the new logic
too.

**Overhead, measured, not estimated:** a pure no-op run (nothing new,
nothing orphaned) took 5.6s; the original splice-only baseline run took
7.2s; the orphan-retirement run took 7.3s. Retiring an orphan costs about
the same as a normal splice commit, not meaningfully more, and -- confirmed
via the manifest count in the target's snapshot history -- it only rewrites
the small manifest file(s) that actually contained a deleted entry, never
the underlying Parquet data. **One real scaling caveat, not just a
footnote:** a manifest batches many `DataFile` entries together (thousands,
at production scale), and `_OverwriteFiles` rewrites the *whole* manifest
if it contains even one orphaned entry -- so the cost of a retirement scales
with the size of the manifest(s) the orphan happens to share, not with the
number of orphaned files itself. Invisible at this test's scale (one file,
one tiny manifest); worth re-measuring against a production-sized,
frequently-updated source before assuming this stays cheap indefinitely.

**Still not covered by this fix:** genuinely dropped rows where the source
never re-adds a replacement (a real delete, not an update) are correctly
retired the same way -- confirmed by the mechanism, not separately
re-tested with a dedicated delete-only scenario in this pass. Renamed
columns, dropped columns, and changed data types are still untouched by any
of this and remain out of scope (see 1.0.0's "Explicitly out of scope"
section, which is otherwise superseded by this entry on the merge-on-read
question specifically).

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
