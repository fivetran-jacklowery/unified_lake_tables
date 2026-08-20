# How it works

This is the deep technical version. For the plain-language summary, prerequisites,
and quick start, see the [README](../README.md).

## The problem this solves

If you run many connections of the same connector into Fivetran-managed Iceberg
tables on Managed Data Lake Service (MDLS) -- one per tenant, one per regional
database, one per customer -- you end up with N structurally identical tables per
table type instead of one queryable table. The usual fixes both treat
"consolidate" as an operation on data: a `UNION ALL` view re-scans every source's
full data on every query, and a materialized merge job physically rewrites
everything, every time it runs, whether or not anything actually changed.

## The core idea: manifest splicing, not data movement

An Iceberg table is mostly metadata. The table itself is a manifest: a list of
which physical Parquet files exist, which partition each belongs to, and some
column-level statistics about what's inside them. The actual bytes live in
ordinary Parquet files in object storage; the manifest is just the index a query
engine follows to know which files to read.

Because the manifest is separate from the data, you can build a brand-new
target table whose manifest points at files that already belong to a
different (source) table -- without copying, rewriting, or even reading those
files' contents. This is what `register_consolidation.py` does: for every
table name common to all your configured source namespaces, it constructs a
target table and appends `DataFile` entries that point at the *same physical
files* the source tables already reference, tagged with a partition value
identifying which source they came from.

## Why this isn't "just" a zero-copy clone

The zero-copy trick by itself isn't novel -- Delta Lake's shallow clone does
something similar for a single table. The interesting (and risky) part shows up
the moment you splice files from **multiple independently-evolving sources**
into one target, instead of cloning one static table.

Iceberg identifies every column by an internal integer, a **field id**, baked
into each physical file's footer when it's written. A SQL `UNION ALL` doesn't
have this problem because it resolves columns by name at query time. But once
you're splicing files directly by their internal field-id numbering, nothing
stops two of your sources from independently landing a brand-new column on the
exact same field id with two completely different meanings. This was
reproduced, not just reasoned about: one source added a new column whose
internal numbering happened to land on a field id already claimed by
unrelated bookkeeping. The result wasn't an error -- a column silently
returned the wrong data for every affected row, with nothing anywhere
flagging it. That's the bug this tool's correctness layer exists to prevent.

## Choosing which sources to consolidate

`config.yaml`'s `source_namespaces` accepts either an explicit list or a
glob pattern (`source_namespace_pattern`, e.g. `"tenant_*"`) -- pick
whichever fits your scale. An explicit list is fine for a handful of
sources. At real scale (the scale this tool is actually built for --
Ecolab-style 400+ Azure SQL databases, Andersen-style 10,000+ ERP tables),
hand-listing every namespace defeats the point.

`resolve_source_namespaces()` handles the pattern case: it calls the
catalog's `list_namespaces()` to get every namespace that currently
exists, then keeps whichever ones match the glob (`fnmatch` semantics --
`*`, `?`, `[seq]`, not SQL `LIKE`'s `%`/`_`), excluding the target
namespace itself in case the pattern would otherwise catch it too. This
was validated against the real catalog used throughout this project's
development: `synth_src_0*` correctly matched exactly the 8
`synth_src_01`..`synth_src_08` namespaces and nothing from the
differently-prefixed `synth_src_changing_*` set sitting alongside them in
the same catalog.

The mental model is deliberately close to dbt-utils'
`get_relations_by_pattern` macro -- discover what matches a schema
pattern, rather than maintaining an exhaustive explicit list by hand. The
practical difference from that macro: this resolves namespaces (Iceberg's
unit of "schema"), not individual tables -- table-level discovery within
each matched namespace is a separate, already-existing step
(`discover_common_tables()`, unchanged by this).

A pattern is resolved fresh on every run, not cached -- add a new tenant
namespace in Fivetran and it's picked up automatically next time this
runs, no config change needed. The resolved list is always logged at INFO
level, since silently discovering which namespaces feed a
schema-modifying tool is exactly the kind of thing that should stay
visible rather than being a hidden side effect of a glob matching more
(or less) than intended.

## The four-step mechanism

### 1. Reserve

When `register_consolidation.py` creates a target table
(`create_target_table()` in `scripts/register_consolidation.py`), the
consolidation-only bookkeeping column (`source_id_column` from your
`config.yaml`) is assigned a field id from a permanently out-of-band range
(`RESERVED_FIELD_ID_BASE = 100000`), not "the next free id." No realistic
source schema grows anywhere near that range, so it structurally can't
collide with anything a source adds later.

Getting an explicit field id to actually stick took a specific two-step
sequence, discovered by inspecting the client library's source and raw
request/response payloads, not from any documented API contract:

1. Passing the desired field id directly into a schema at `CREATE TABLE` time
   does **not** work. The catalog server renumbers all field ids on create,
   regardless of what the client requests.
2. What does work: create the table first with only the source's original
   fields, then force the desired field id in a **separate schema-evolution
   call** made after the table already exists. The catalog honors an
   explicitly-requested id sent this way, unlike at creation time.

The mechanism for forcing that specific id is itself an undocumented,
private pyiceberg implementation detail: `UpdateSchema` tracks the next id to
assign via a plain Python `itertools.count` instance attribute
(`_last_column_id`) with no public setter. Setting it directly before calling
`add_column()` is how the reserved id gets assigned. This has no public API
and is called out explicitly in `requirements.txt` and the README as the
top dependency risk of this whole technique -- see [apache/iceberg-python#1284
](https://github.com/apache/iceberg-python/issues/1284), open since November
2024, requesting exactly this capability.

### 2. Splice (the no-rewrite path)

For every file in every source table, `file_has_drift_beyond()` checks
whether that file carries data for any field id beyond the source's recorded
schema baseline. The check compares `null_value_counts` (per field id)
against `record_count` -- **not** whether a field id merely appears as a key
in the file's stats. Iceberg backfills a stats entry for every field in a
table's *current* schema onto every file, including files written before
that field existed, so key presence alone produces false positives on old
files. A field id beyond baseline with fewer nulls than the file has records
means that file genuinely carries non-null data Iceberg's own schema
evolution added after the file was written.

Files with no drift, or drift that resolves to a field id the target
already understands (see step 3), get spliced straight in: a `DataFile`
entry pointing at the source's existing physical file, with a partition
value identifying the source namespace, appended to the target table's
manifest via a real Iceberg transaction (`_append_snapshot_producer`). No
bytes are read or rewritten. This is the "widen-for-free" case, and it's
standard, correct Iceberg schema-evolution read behavior, not a workaround:
older files across other sources correctly read back `NULL` for a field
they don't have.

### 3. Detect and resolve collisions

Because every source numbers its own schema independently starting from the
same base, two sources landing a genuinely different new column on the exact
same field id is an expected occurrence at scale, not a freak edge case.
`register_table()` resolves this deterministically within a single run:
the first source encountered for a given drifted field id becomes that id's
"owner" (its column name gets adopted at that id, free path). Every other
source whose column shares that same id but has a **different name** is a
real collision, and gets routed to step 4 instead of being spliced in.

(Known limitation, documented in the README and PRD: "first source
encountered" is based on the order sources happen to be processed in during
a given run, which isn't yet a stable, order-independent rule for a system
where sources get added or reordered over time. See "What this does NOT yet
handle.")

### 4. Rewrite (rare, delta-scoped, real cost)

For every collision, the colliding column gets a **fresh, non-colliding**
field id via a normal `add_column()` call (letting the catalog auto-assign,
since this path doesn't need a specific reserved value). Then, only the
**newly-drifted rows** from the colliding source -- not that source's full
history -- are read via `scan(row_filter=...)`, tagged with the source
identity column, and appended for real via `target_table.append()`. This is
the only place actual bytes get rewritten in the whole pipeline, and the
cost is bounded by the size of the drift, not the size of the table.

**Confirmed live gap in this step's idempotency check:** right before doing
the real rewrite, this step checks whether a prior run already handled this
collision (`already_rewritten = target_table.scan(row_filter=..., ...)`).
That check reads the *target* table, whose manifest by this point already
references files spliced in from every source namespace -- and Polaris's
vended credentials are scoped to the target table's own default storage
location, not to every namespace whose files that manifest happens to
reference. Reproduced live: this check can fail with
`AWS Error ACCESS_DENIED during HeadObject operation` reading a
cross-namespace file, crashing the run at exactly this point rather than
completing the collision resolution. The splice path (steps 2-3) is
unaffected and commits first regardless. See `README.md`'s Troubleshooting
section and `CHANGELOG.md`'s "Known issue" entry for the full reproduction
and a confirmed workaround (DuckDB's Iceberg extension reads the same files
without issue) -- not yet fixed in this script itself.

### 5. Retire orphaned files (copy-on-write cleanup)

Steps 1-4 assume a source only ever adds files -- new data lands in new
physical files, and nothing already spliced in ever stops existing at the
source. That assumption is wrong the moment a source does a genuine
row-level UPDATE or DELETE, and it went unverified in this tool's 1.0.0
release, which cited the wrong mechanism for the risk ("merge-on-read
sources / delete files" in the README and CHANGELOG). Reading Fivetran's
actual Managed Data Lake writer source
(`ManagedDataLakeWriter.java`) shows there are no delete files anywhere in
it: `upsert()`, `update()`, and `delete()` all locate every existing
physical file that could contain an affected primary key, rewrite each one
in full via an internal `UpdateWorkflow`/`DeleteWorkflow`, and commit the
swap atomically via Iceberg's `OverwriteFiles` transaction
(`deleteFile()` + `addFile()` in one commit, confirmed in the writer's
`commit()` method). No `write.delete.mode`/`write.update.mode`/
`write.merge.mode` table properties are set anywhere -- copy-on-write (COW)
is the only path this writer implements, not a configurable choice.

**Why that broke the four-step mechanism above:** `already_registered_paths()`
(now `already_registered_by_source()`) only ever asked "is this source file
path new to me, splice it in." It never asked the reverse question: "did a
file I already spliced in disappear from this source's current snapshot?"
COW retires the old file from the source's current snapshot the instant it
rewrites it -- the source itself reads correctly (new file replaces old in
its own manifest) -- but the target kept referencing the retired file
forever, because nothing here ever revisited a file once it was spliced in.
Every row that stale file held became a permanent duplicate against its
replacement, and a genuinely-updated row ended up present in the target
with both its stale and current values simultaneously, with no column
distinguishing which was which.

**Reproduced live** with two minimal single-table test connectors
(`connectors/cow_test_01`/`cow_test_02`) before any fix was written: a
30-row baseline consolidated cleanly to 60 rows across both sources.
Re-syncing `cow_test_01` with a connector that re-upserts one *existing*
primary key with changed values (instead of inserting a new one) confirmed,
by directly inspecting the source's Iceberg snapshot, that its original
single 30-row file was gone from the current snapshot -- replaced by two
brand-new files (1 changed row + 29 unchanged rows = 30, still). Re-running
`register_consolidation.py` unchanged spliced both new files in as "never
seen before" without retiring the stale original, producing 90 rows instead
of 60: all 30 of `cow_test_01`'s rows duplicated (the COW rewrite touched
the whole file, not just the one changed row), with the specifically-updated
row present as both `(green, active, v1)` and `(orange, UPDATED, v2)`
simultaneously.

**The fix:** `already_registered_by_source()` returns the actual `DataFile`
objects (not just path strings), grouped by source via the partition value
already recorded on every spliced entry (`f.partition[0]`). Each source's
current file listing is now diffed both directions every run: new paths
still splice in exactly as step 2 always did; any previously-registered
path no longer present in that source's current snapshot is collected as
orphaned. If any orphans exist, the commit swaps from the plain
`_append_snapshot_producer` (fast-append, used when there's nothing to
retire) to `transaction.update_snapshot().overwrite()` -- a **public**
pyiceberg 0.11.1 API, not a private hack like the reserved-field-id
mechanism in step 1 -- calling `delete_data_file()` on each orphan and
`append_data_file()` on its replacement(s) in one atomic commit. This
mirrors Fivetran's own `OverwriteFiles.deleteFile()` + `addFile()` pattern
exactly, one layer up the stack.

Confirmed via the target's own snapshot history after the fix: the two
prior splice commits recorded as `Operation.APPEND` (2 files added each);
the retirement commit recorded as `Operation.OVERWRITE` (1 file deleted,
target manifest count 2 -> 3). Re-running the exact broken scenario, no
config changes: retired the 1 orphaned file, removed the 30 stale rows,
table back to the correct 60, zero duplicates, the previously-updated
widget showing only its current value. A second run immediately after was
a clean no-op (0 spliced, 0 rewritten, 0 orphaned) -- idempotency holds
under the new logic exactly as it did before.

**Overhead, measured, not estimated:** a pure no-op run (nothing new,
nothing orphaned) took 5.6s; the original splice-only baseline took 7.2s;
the orphan-retirement run took 7.3s -- retiring an orphan costs about the
same as a normal splice commit, not meaningfully more, at this test's
scale. Under the hood this is because `_OverwriteFiles._existing_manifests()`
only rewrites a manifest file if it actually contains a deleted entry;
manifests untouched by the retirement are reused by reference, unchanged --
so this stays a metadata-only operation, no Parquet data read or rewritten,
consistent with every other step in this mechanism.

**Real scaling caveat, not just a footnote:** a manifest batches many
`DataFile` entries together (potentially thousands, at production scale
with manifest merging enabled), and the rewrite-if-touched rule means
retiring even one orphaned file forces a full rewrite of *every other
entry* sharing that manifest, not just the orphan. The cost of a retirement
therefore scales with the size of the manifest(s) the orphan happens to
share, not with the number of orphaned files itself. This was invisible at
this test's scale (one file, one small manifest) and has not been
re-measured against a production-sized, frequently-updated source with
large, merged manifests -- don't assume the 7.3s number above holds at
that scale.

**Still not separately exercised:** a genuine DELETE with no replacement
file at all (as opposed to an UPDATE, which always produces a replacement).
The retirement logic should handle a pure delete identically -- it doesn't
require the deleted file to have a same-run replacement, only that it's
gone from the source's current listing -- but this was confirmed by
mechanism, not by running a dedicated delete-only reproduction the way the
update case above was.

### Idempotency

Re-running `register_consolidation.py` against sources with nothing new
must be a safe no-op. Three separate mechanisms make that true for the
three different code paths:

- **No-rewrite path**: `already_registered_by_source()` reads the target's
  existing manifest before deciding what to splice, and skips any file
  whose path is already registered. An early, non-idempotent version of
  this technique had no such check and silently doubled every row on a
  second run -- this was caught empirically (a rerun came back with every
  source's row count exactly 2x) before this check existed.
- **Rewrite path**: physical appends produce new file paths every time, so
  path-based deduplication doesn't apply. Instead, before rewriting,
  `register_table()` checks whether the target already has at least as many
  non-null rows for that (source, field) pair as the drifted file set would
  produce, and skips if so.
- **Retirement path** (see step 5 above): an orphan is, by construction,
  only detected once -- the moment it's retired via `delete_data_file()`, it
  stops existing in the target's manifest, so a second run simply won't
  find it in `already_registered_by_source()` anymore and has nothing left
  to retire for that file. Confirmed live: a run immediately following a
  real retirement came back with 0 spliced, 0 rewritten, 0 orphaned -- a
  clean no-op, same as the other two paths.

### Partition pruning

The target table's partition spec has an identity partition on
`source_id_column`. Filtering a query to one source (`WHERE
source_connection_id = 'x'`, or whatever you named the column) lets the
query engine prune to just that source's files at the manifest level,
without opening files belonging to other sources. This was independently
confirmed via Snowflake and DuckDB, not just inferred from the partition
spec being present: a filtered query planned and read fewer files than an
unfiltered one, verified through the query engines' own operator-level
stats (not just "the result was correct," which a full scan would also
produce). `scripts/verify_consolidation.py` includes a lightweight version
of this same check.

## Credentials: vended, not static

`register_consolidation.py` never asks for or constructs an AWS access
key/secret. `catalog_properties()` builds only Polaris OAuth
client-credentials properties (`uri`, `warehouse`, `credential`, `scope`,
optionally `oauth2-server-uri`) for pyiceberg's `RestCatalog`.

Confirmed by reading pyiceberg 0.11.1's own source
(`pyiceberg/catalog/rest/__init__.py`): `RestCatalog` sends the
`X-Iceberg-Access-Delegation: vended-credentials` header **by default**, with
no extra client-side configuration required to opt in
(`session.headers.setdefault("X-Iceberg-Access-Delegation",
ACCESS_DELEGATION_DEFAULT)`, where `ACCESS_DELEGATION_DEFAULT =
"vended-credentials"`). When the catalog server (Polaris, here) honors that
header and returns short-lived, scoped storage credentials alongside a
table's metadata, pyiceberg merges those into the table's config
(`_load_file_io`) and constructs the table's `FileIO` from them
automatically -- `PyArrowFileIO` reads `s3.access-key-id`,
`s3.secret-access-key`, and `s3.session-token` straight out of that merged
property set (confirmed in `pyiceberg/io/pyarrow.py`). In other words: as
long as your Polaris catalog integration is actually configured to vend
credentials (this is a Fivetran/Polaris-side setup detail, not something
this script controls), `cat.load_table(...)` returns a `Table` whose IO is
already correctly, narrowly scoped -- there's no separate "vended mode" flag
to flip in this codebase.

Two sharp edges worth knowing about, found by reading the same source
rather than assumed:

- **`oauth2-server-uri` fallback is deprecated.** If `POLARIS_TOKEN_URI`
  (mapped to pyiceberg's `oauth2-server-uri` property) is left unset,
  pyiceberg 0.11.1 falls back to deriving a token endpoint from the catalog
  URI, but its own source explicitly logs this as deprecated behavior
  scheduled for removal in a future release (see the `_warn_oauth_tokens_deprecation`
  method and its message referencing
  [apache/iceberg#10537](https://github.com/apache/iceberg/issues/10537)).
  Set `POLARIS_TOKEN_URI` explicitly; don't rely on the fallback.
- **No built-in refresh of vended storage credentials on expiry.** pyiceberg's
  OAuth *bearer token* (used to talk to the catalog itself) refreshes
  proactively with a margin before expiry (see `pyiceberg/catalog/rest/auth.py`).
  The *vended AWS credentials* attached to an already-loaded `Table`'s `FileIO`
  have no equivalent automatic refresh mechanism for S3 in this pyiceberg
  version (GCS has explicit expiry-aware handling via
  `GCS_TOKEN_EXPIRES_AT_MS`; S3 does not). If a `Table` object is held for
  longer than its vended credentials' lifetime (commonly on the order of an
  hour for STS-style credentials, but this is set by Polaris/your cloud
  provider, not by this tool), subsequent reads through that stale `Table`
  could start failing with an S3 auth error. `register_consolidation.py`
  already reloads the target table via `cat.load_table()` after every schema
  change and every append, which incidentally refreshes its credentials
  frequently; source tables are loaded once per registration pass. If you
  run this against a source large enough that a single table's registration
  pass takes longer than your vended credentials' lifetime, re-loading the
  source table partway through (or splitting the run) is the workaround
  until/unless this needs a more general fix.

This was validated by reading the installed pyiceberg 0.11.1 source directly
(not just its documentation, which is thin on this specific mechanism) --
it was not re-validated end-to-end against a live Polaris catalog with real
vended AWS credentials as part of building this repo. See the README's
"Vended credentials: what's actually confirmed" section for the precise
line between what was read in source code versus what was previously proven
live against a real Fivetran-managed Polaris catalog during the original
internal validation (which used static AWS keys, not vended credentials, as
documented in this repo's CHANGELOG).

## What's proven versus what isn't

See the README's "What this does NOT yet handle" section for the customer-facing
version. In short: "a source adds a new nullable column" (steps 1-4) and "a
source performs a row-level UPDATE, retiring the old file via copy-on-write"
(step 5) were both validated against real infrastructure. Renamed columns,
dropped columns, and changed data types are different Iceberg mechanisms
entirely and remain out of scope for this tool as written. A pure DELETE
(no replacement file) is expected to work via the same step 5 mechanism but
wasn't separately exercised as its own reproduction -- see step 5 above.
