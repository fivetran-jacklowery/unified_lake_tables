# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Fixed: collision rewrite appended one duplicate copy per drifted file

Found by reading and then **reproduced live against real infrastructure**,
2026-09-22. In `register_consolidation.py`'s step 4 rewrite pass:

```python
for f in to_rewrite:
    tab = src_table.scan(row_filter=f"{name} is not null").to_arrow()
```

The loop variable `f` is never used in the body. The scan covers the
**whole source table** and is re-run identically on each iteration, so
`tabs` ends up holding `len(to_rewrite)` identical copies, and the loop
below appends every one of them. With 2+ drifted files for a single
colliding field id, that source's rows land in the target N times over and
`rewritten_rows` reports N times the true count.

**The reproduction.** Nothing existing could trigger this -- the eight
`synth_src_changing_*` fixtures are byte-identical by construction, so they
never collide. Built a purpose-built two-source fixture instead, at
`../connectors/synth_collide_01` and `../connectors/synth_collide_02`
(Connector SDK; that directory is outside this repo, alongside the other
connectors). Both declare an identical 9-column base table `widgets`, then
each grows ONE new column with a different name -- `alpha_metric` in 01,
`beta_flag` in 02 -- which is emitted as an undeclared extra key on the
second sync, the way real source drift actually arrives. Because both
tables sit at the same highest field id when the new column lands, both
new columns take **field id 12**, with different names. That is the
collision. Sync 1 lands 20,000 rows with no drift column; sync 2 lands
25,000 rows all carrying it, which the writer spread over **7 files**.

Measured, first run, before the fix:

```
COLLISION at field_id=12: 'alpha_metric' already owns it,
  'synth_collide_02'.'beta_flag' needs a fresh field id + rewrite
synth_collide_02: 4 file(s) spliced in, 20000 rows (7 pending physical rewrite)
rewrite: 'beta_flag' (from synth_collide_02) field_id=100001,
  175000 row(s) rewritten + appended
```

175,000 where 25,000 was correct -- an exact **7x**, one copy per file in
`to_rewrite`. Target-side: `synth_collide_02` held 195,000 rows (20,000
correctly spliced + 175,000 duplicated) against a 45,000-row source, while
`synth_collide_01` was correct at 45,000.

**The fix.** Not "use `f`" -- that single scan already returns every drifted
row across all of the source's files, so the loop was simply redundant and
collapses to one scan. `to_rewrite` still decides *whether* there is
anything to rewrite and how many rows to expect. After the fix, rebuilt
from scratch: 25,000 rows rewritten, both sources exactly 1.00x, and the
run dropped from **23.6s to 8.8s** (six redundant whole-table scans gone).
A third run is a clean no-op (0 spliced, 0 rewritten, 0 orphaned), so
idempotency still holds through the collision path.

Note `verify_consolidation.py`'s duplicate-file check would **not** have
caught this -- each append writes a new, distinct file path, so there is no
duplicated path to find. Its row-count check does catch it, and did.

Residual, not reproduced: the skip guard compares `already_rewritten`
(rows where the column is non-null) against `expected_rows` (the full
`record_count` of every drifted file). Those are the same number only when
drifted files contain nothing but drifted rows, which was true here. A file
mixing null and non-null values for the new column could make the guard
compare mismatched quantities.

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

### Fixed: the ACCESS_DENIED gap above, in the script itself

New `scripts/multi_source_io.py`: a `MultiSourceFileIO` that wraps a
table's own `FileIO` alongside one or more other tables' `FileIO`
instances (each carrying that table's own vended credential), and tries
each in turn to open a given file -- caching which one worked per storage
prefix so repeat opens of the same source's files don't retry every
candidate. `Table.io` turned out to be a plain settable attribute
(`self.io = io` in `pyiceberg.table.Table.__init__`, confirmed by reading
the installed 0.10.0 source) that `Table.scan()` reads fresh on every call
(`DataScan(..., io=self.io, ...)`), so both fixed call sites work by
temporarily swapping `target_table.io` for a `MultiSourceFileIO` built from
`[target_table.io, src_table.io]` around just the one scan that needs
cross-namespace reads, then restoring the target's own IO immediately
after -- writes always go through the target's own credential, unaffected.

Applied to both places this same root cause showed up:
`register_consolidation.py`'s collision-rewrite idempotency check (the
exact crash reproduced above), and `verify_consolidation.py`'s per-source
row-count check (the "same root cause... but [less severe]" instance this
entry's own writeup pointed to).

Verified live against the same sandbox catalog and the same `warehouses`
collision that originally crashed: re-running `register_consolidation.py`
completed the previously-pending rewrite (1 file, 50 rows) with no error,
and a subsequent `verify_consolidation.py` run passed all checks (row
counts, duplicate-file check, partition-pruning check) for all 15 tables,
`warehouses` included (452,499 total rows across 8 sources, all matching
source counts exactly). The `MultiSourceFileIO` fallback/caching logic was
also unit-tested standalone against fake `FileIO`s simulating this exact
failure mode before the live re-run.

Note: this fixes the two call sites that actually hit the bug today. It is
not a general "any reader can see any file in this catalog" credential
model -- a future cross-namespace read added somewhere else would need the
same treatment (wrap the relevant tables' `FileIO`s) rather than assuming
this is fixed globally.

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

**Follow-up: full insert/update/delete lifecycle, two independent cycles,
both test sources.** The gap noted above -- a genuine delete, not just an
update, wasn't separately exercised -- is now closed, with a real finding
along the way: **Fivetran's Managed Data Lake destination soft-deletes.**
`op.delete()` from a connector does not physically remove a row -- a
`_fivetran_deleted` BOOLEAN column is added to every table automatically
(not declared by any connector's own `schema()`), and a "deleted" row stays
physically present in the current snapshot, just flagged `true`. Confirmed
directly: querying the two rows explicitly deleted in testing showed both
still present, with their original `color`/`status`/`version` values
untouched, `_fivetran_deleted = true`. Mechanically, this means a delete on
this destination type is **identical to an update** -- the same
copy-on-write rewrite of the file containing that row, just flipping one
column instead of several -- so there is no separate "orphan with zero
replacement" scenario to worry about on MDLS specifically; the retirement
logic above already covers it without any changes.

Extended `connectors/cow_test_01`/`cow_test_02` to do all three, for real,
every sync after the historical load: 5 inserts (brand-new primary keys), 3
updates (re-upsert existing primary keys with changed values), 2 deletes
(`op.delete()` on other existing primary keys), reseeded per-cycle so the
exact rows touched vary sync to sync. Ran two full cycles across both
sources and re-ran `register_consolidation.py` after each:

| Stage | Rows/source | Consolidated total | Files spliced | Orphans retired | Stale rows removed | Wall clock |
|---|---|---|---|---|---|---|
| Baseline (insert-only) | 30 | 60 | 2 | 0 | 0 | 7.2s |
| Cycle 1 (insert+update+delete) | 35 | 70 | 2 | 3 | 60 | 11.1s |
| Cycle 2 (insert+update+delete) | 40 | 80 | 2 | 2 | 70 | 9.4s |

Verified independently via DuckDB after every stage, not just trusted from
the script's own log: total row count matched exactly at each step (60 ->
70 -> 80), zero duplicate `(source, widget_id)` pairs at any point, every
updated row present exactly once with its latest value (never a stale
pre-update duplicate sitting alongside it), and every soft-deleted row
present exactly once with `_fivetran_deleted = true` (never duplicated,
never silently resurrected back to `false`). The second cycle's clean
result matters as much as the first: it confirms the detect-and-retire
logic doesn't accumulate drift, leftover state, or degrade on repeated
real-world-style churn -- it isn't a one-shot fix that only happens to work
the first time. Overhead held steady across both cycles too (11.1s and
9.4s against a 7.2s pure-splice baseline), consistent with the earlier
single-update measurement -- retiring 2-3 files in one run still costs
about the same as a normal splice commit at this scale.

**Still genuinely out of scope:** dropped columns and changed data types
remain untouched by any of this (see 1.0.0's "Explicitly out of scope"
section, which is otherwise superseded by this entry on the merge-on-read
question specifically). Renamed columns are no longer untested -- see the
next entry.

### Fixed: a source's own schema evolution could vanish into a stale baseline

Found while testing renamed columns -- specifically, the case where a
connector stops emitting one column name and starts emitting another (e.g.
`color` -> `hue`). First confirmed, by reading
`ManagedDataLakeSchemaMigrator.java`, that this isn't a real Iceberg rename
at all: there's no name-similarity inference anywhere in Fivetran's
migration-step planner, so a new column name from a connector is always
plain `addColumn()` with a brand-new field id, mechanically identical to
any other new-column case this tool already handles. A true
field-id-preserving rename only ever fires from an explicit dashboard
`customName` edit, which no connector can trigger -- see the next entry for
that case tested separately.

Extended `connectors/cow_test_01`/`cow_test_02` (both already carrying
`color`) to switch to emitting `hue` instead starting on their 3rd
post-historical sync cycle, including on rows being inserted or updated in
that same cycle (stacking the "rename" on top of the copy-on-write path
already validated above). Deployed, synced, and re-ran
`register_consolidation.py` -- expecting a normal widen, exactly like any
other new column.

**What actually happened:** `hue` never showed up in the target's schema
at all. Not a delay -- confirmed via direct schema inspection (both
sources had `hue` at field id 8; the target capped out at field id 7) and
by re-running consolidation a second time with zero source changes: still
missing, still a clean no-op with nothing to widen. Genuinely stuck, not
just slow.

**Root cause:** `register_table()` computed a `baseline_max_field_id`
(a single integer) fresh on every run from `source_namespaces[0]`'s (the
first-listed source's) *live* schema, and treated any field id at or below
that number as "already expected" -- never drift. By the time
consolidation ran, Fivetran had already evolved `cow_test_01`'s own schema
to include `hue`, so the baseline was already 8 before the drift check
ever looked at a single file. Every subsequent run re-derived the same
already-evolved baseline from the same source, so this wasn't a one-time
miss -- it was permanent, for as long as that config existed. This has
nothing specifically to do with renames: it's a blind spot for *any*
column `source_namespaces[0]` gains, for any reason, the moment it gains
it before the very first consolidation run that could have seen it as
new. Every earlier add-column test in this project happened to land the
new column on a source other than the first-listed one, which is exactly
why this was never caught before.

**The fix:** `baseline_max_field_id` (an int threshold, borrowed from one
source) became `known_field_ids` (a set, sourced from the *target* table's
own current schema, computed once per run right after the target is
loaded or created). `file_has_drift_beyond()`'s check changed from `field_id
> baseline_max_field_id` to `field_id not in known_field_ids`. The target's
own schema is the only thing that actually defines "what does this tool
already know about" -- borrowing any one source's schema as a stand-in
never made sense once you remember that source field ids are independently
numbered per table (the exact reason the field-id-collision logic already
existed). Every call site (drift scan, splice-skip check, rewrite-pass
matching) took the same value, so this was a one-definition change, not a
scattered patch.

**Re-ran the exact broken scenario after the fix, no other changes:** log
showed `widening (no-rewrite): 'hue' at physical field_id=8`, and the
target's schema immediately included `hue`. This recovered the data that
had already been stuck -- for free. The files carrying `hue` values were
already spliced into the target's manifest from the earlier broken run;
widening the schema to declare field id 8 made that already-present data
queryable immediately, with zero re-splice and zero rewrite, since Iceberg
schema evolution is purely additive metadata. A follow-up run was a clean
idempotent no-op (0 spliced, 0 rewritten, 0 orphaned).

**Overhead:** effectively free. `known_field_ids` is built from
`target_table.schema().fields`, metadata already sitting in memory from a
table load this code already performs every run -- no added network round
trip, no added file read. The recovery itself cost one small
schema-evolution call, not a rewrite.

### Confirmed, not fixed: a true (dashboard-level) rename goes stale silently

Tested the other rename scenario the fix above doesn't touch: an existing
field genuinely renamed with its field id preserved -- what a Fivetran
dashboard `customName` edit does, and the only way a true Iceberg rename
can happen on this destination at all. Simulated directly (no dashboard
access from this test harness) by calling
`table.update_schema().rename_column("status", "state")` against
`cow_test_01.widgets` only, leaving `cow_test_02` untouched as a control.
Confirmed the field id was preserved across the call (`field_id=3` before
and after, name only) before treating this as a valid simulation of the
real mechanism.

**Result:** `register_consolidation.py` ran as a completely clean no-op --
0 files spliced, 0 drift detected, no warning, no log line of any kind.
The target's schema still shows `status` for field id 3 (matching the
untouched `cow_test_02` control) while `cow_test_01`'s own current schema
now says `state` for that same field id. Re-read the actual row data
directly from `cow_test_01` afterward to confirm the rename didn't touch a
single byte -- values were exactly as expected from prior test cycles.

**Why this one isn't fixed:** this tool's only per-column signal is a
field id's presence in `known_field_ids`; once an id is known, nothing
ever re-checks whether its *name* still matches a source's current schema
-- there's no hook that could notice, since a rename touches zero data
files (no new file, no orphan, nothing for the per-file drift scan to see
in the first place). A real fix would mean re-checking every already-known
field id's current name against every source on every run, not just
resolving newly-drifted ids -- a different, larger piece of work than the
baseline fix above, and not undertaken here. Left as an open, clearly
documented gap (see README.md's "What this does NOT yet handle" and
Troubleshooting sections) rather than silently declared handled.

### Fixed: silent field-id collision misattribution

Reproduced live (not hypothetical), against the exact `warehouses` /
`synth_src_changing_03` / `synth_src_changing_04` collision this project's
own prior entries above already describe: after the ACCESS_DENIED fix let
the collision-rewrite path's idempotency check run without crashing, a
subsequent run of `register_consolidation.py` against a real customer-style
target (`for_casey.warehouses`, built via an external VS Code integration
attempt) left `synth_src_changing_03`'s 50 `loading_dock_type` rows silently
readable as `dock_supervisor_name` instead -- no error, no warning, wrong
data. Root cause was a real gap in `file_has_drift_beyond()`, not the
ACCESS_DENIED path: it only ever checked whether a field id was *present* in
`known_field_ids`, never whether the target's registered *name* for that id
agreed with what the file's own source calls it. Two distinct ways this bit
us, both reproduced directly:

1. **Day one, no interruption needed at all.** `create_target_table()` seeds
   the target's starting schema from `source_namespaces[0]`'s full schema,
   so a field id source #1 already happens to use is "known" from table
   creation -- before drift-detection is ever called for any OTHER source's
   files. A second source landing a differently-named column on that same
   numeric id (routine for independently-evolving sources, since each
   starts counting from its own schema's next free id) was never flagged as
   drift and got silently spliced in under source #1's name for that id.
   Reproduced against two disposable two-source test tables with no
   interruption of any kind -- a clean, uninterrupted first run mislabels
   the data.
2. **An interrupted collision-rewrite** (the scenario this project's own
   prior entries assumed was the only risk here): the "owner" name for a
   colliding id is committed as soon as the collision is first detected,
   independently of whether the *losing* source's rewrite (a separate,
   later commit under the old two-commit design) ever completes. If that
   rewrite is interrupted after its schema-evolution commit but before its
   data-append commit, a rerun sees the id as "known" from the owner's
   already-committed widening and never re-flags the loser's still-
   unresolved file, even though its rewrite never actually happened. This
   is what happened to `for_casey.warehouses` in practice.

**The fix**, in `file_has_drift_beyond()`: compare the file's own source's
name for a field id (via that source's live schema) against the target's
registered name for the same id, not just whether the id is present.
`known_field_ids` (a set) became `known_field_names` (a `{field_id: name}`
dict) threaded through all three call sites. A name mismatch is still-
unresolved drift regardless of whether the id itself is otherwise "known."

This also required fixing the widening step in `register_table()`: it used
to assume the first distinct name seen in a given run could always freely
claim a drifted field id (`if owner_name not in already_field_names`), which
is only safe if the target doesn't *already* have a different name
registered for that exact id. With the corrected drift-detection now
catching the day-one scenario too, the target frequently already has a name
for the id in play (seeded at creation, per case 1 above) -- and Polaris
correctly rejects a schema-evolution commit that tries to claim an id the
target's schema already uses for something else (`HTTP 400 Bad Request`,
reproduced live while validating this fix). Fixed by checking
`known_field_names` for an existing name first: if the target already has
one, that name is the true owner outright and *every* distinct name seen
this run goes through the rewrite path, not just "everyone after the
first."

Verified end-to-end against disposable two-source test tables (day-one
scenario: clean first run now correctly detects and resolves the collision
instead of mislabeling; three consecutive runs settle into a stable,
correct, zero-churn steady state) and against the real, already-corrupted
`for_casey.warehouses`: removed the single 50-row culprit manifest entry
(identified via its file-level `null_value_counts`, which pinpointed the
exact physical file down to its record count matching the known-bad row
count), then reran the fixed script, which correctly re-detected the
collision and re-derived the 50 rows under `loading_dock_type` -- confirmed
via a cross-namespace scan (`MultiSourceFileIO`) that `synth_src_changing_03`
now shows 0 rows under `dock_supervisor_name` / 50 under `loading_dock_type`,
while `synth_src_changing_04`'s legitimately-owned 40 `dock_supervisor_name`
rows were untouched throughout.

### Fixed: the collision-rewrite path's two commits weren't atomic

Companion fix to the misattribution bug above, closing the actual
interruption window rather than just detecting its aftermath more
correctly. Before this, `register_table()`'s rewrite pass committed a
drifted column's schema evolution (`add_column`) as one standalone commit,
then appended each drifted file's data as one or more *separate* commits
after it -- real physical writes, not the cheap no-rewrite splice path, so
there was a real window between them where the process could be
interrupted, crash, or lose its network connection with the schema change
durable and the data not.

**The fix:** wrap the schema evolution (when the column is new) and every
file's data append for a given collision into one `pyiceberg`
`Transaction` -- `Transaction.table_metadata` layers pending `update_schema`
calls onto the in-progress metadata, so a subsequent `Transaction.append()`
in the *same* transaction correctly resolves the not-yet-committed column,
and `commit_transaction()` (called automatically on a clean `__exit__`)
writes both as a single atomic snapshot. Confirmed directly (not just by
reading `pyiceberg`'s source) against a disposable test table: raising an
exception partway through an open `with table.transaction() as txn:` block
-- after `txn.update_schema()` and one `txn.append()` had already run --
left the catalog completely unchanged on read-back: no new column, no new
rows, snapshot count unchanged. Either everything in the transaction lands,
or nothing does.

A run from *before* this fix can still have left a column registered with
none of its data appended (exactly the `for_casey.warehouses` case above).
The rewrite pass now checks for that explicitly rather than assuming
"column exists" always means "fully done": if the column already exists but
an idempotency-style row-count check finds fewer rows than expected, it logs
a clear warning and appends the missing rows as a data-only commit (no
schema change needed), self-healing the pre-fix state instead of silently
leaving it broken.

### Fixed: rewrite-path files were perpetually treated as copy-on-write orphans

Found while verifying the two fixes above, not part of the original ask --
a genuine rerun of the fixed script surfaced it immediately (an orphan
retirement + rewrite churn cycle on every single run of a table with any
resolved collision, which shouldn't happen). `already_registered_by_source()`
returned every file registered under a source's partition, used both for
the plain "already spliced, skip" check and for copy-on-write orphan
detection (does this path still appear in the source's own current file
listing). Those two checks need different inputs: a file created by the
collision-rewrite path (`Transaction.append()`, see above) is a brand-new
physical file written under the *target's own* storage location, not a
splice-by-reference of a source file -- it will never appear in any
source's own file listing, by construction, regardless of whether anything
is wrong with it. Using the full set for orphan detection meant every
rewritten/appended file got flagged as orphaned and deleted, then
re-derived from scratch, on every subsequent run.

**The fix:** `already_registered_by_source()` now returns
`(by_source, spliced_by_source)` -- the second only includes files whose
path doesn't start with the target table's own storage location, i.e.
files still genuinely spliced in by reference from a source. The orphan-
check loop in `register_table()`'s no-rewrite pass now uses
`spliced_by_source` instead of the full set; the plain idempotency check
(`already`) is unaffected and still uses the full set, since re-splicing an
already-registered path is the thing it's guarding against regardless of
which mechanism registered that path.

Verified directly: three consecutive runs against the same disposable
two-source collision test table now show 0 orphaned files retired and 0
rows rewritten on runs 2 and 3 (previously, run 2 alone deleted and
re-appended the rewritten file's data), and the table's snapshot count
stayed at 2 across all three runs (the two commits from run 1; nothing
added by the no-op reruns).

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
