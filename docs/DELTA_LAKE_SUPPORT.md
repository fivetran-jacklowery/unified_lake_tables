# Delta Lake support (experimental)

**Status: validated end to end, not productionised.** Iceberg is this
project's primary path and everything else in this repo assumes it. This
document covers a secondary capability: emitting a Delta Lake `_delta_log`
over the *same* consolidated data files, so Databricks customers can read
the consolidated table too.

Read this before using `scripts/generate_delta_log.py` for anything real.
The mechanism works and was measured; the surrounding operational story is
thinner than the Iceberg side.

---

## The idea

A consolidated Iceberg table's manifest points at every source's Parquet
files by absolute path. Delta's `add.path` may *also* be an absolute URI —
that is exactly the mechanism behind **Delta shallow clone**, where a new
table's log references another table's files without copying them.

So a `_delta_log` can point at the same files the Iceberg manifest does.
The result is **two independent metadata layers over one shared set of data
files**: Iceberg for Snowflake/Trino, Delta for Databricks. No bytes copied,
no second consolidation run, and the Iceberg side is untouched.

This mirrors, one level up, what Fivetran MDLS already does per source
table — it can emit both Iceberg and Delta metadata over the same Parquet.

## Why Delta is structurally *easier* here than Iceberg

This is the part worth internalising, because it inverts the usual
expectation.

**Delta resolves columns by name. Iceberg resolves by field id.** Most of
`register_consolidation.py`'s complexity — the reserve / widen-for-free /
rewrite-on-collision machinery, plus the base-schema divergence guard added
after a customer hit it — exists because Iceberg matches columns by an
integer baked into each Parquet file, so two sources can disagree about what
an id means and Iceberg will silently return the wrong column. With column
mapping off, Delta reads column names straight out of the Parquet footer.
That entire class of silent corruption is not reachable.

**Partition values live in the log, not the files.** The consolidated table
needs a bookkeeping column identifying each row's source. In Iceberg that
required a reserved out-of-band field id and a two-step create-then-widen
dance, because the catalog renumbers field ids at create time. In Delta, a
partition column is simply declared in the schema and supplied per-file via
`partitionValues`; it is *expected* to be absent from the data files. Which
is precisely the situation here.

**The log is JSON.** No Avro manifest splicing, no dependence on private
pyiceberg internals like `_last_column_id`. `generate_delta_log.py` is
materially smaller than the Iceberg path and has no comparable fragility.

## What was validated

Measured 2026-09-23 against the live lake, on the consolidated `shipments`
table (8 sources, 156 files, ~498k rows).

Generated a single `_delta_log/00000000000000000000.json` (53,714 bytes) at
`s3://lowery-fmdl/two/delta_unified/shipments/`, with 156 `add` actions
pointing at the sources' existing Parquet by absolute URI. Read it with
**Spark 3.5.3 + Delta 3.2.1** — the engine Databricks runs:

```
TOTAL ROWS: 497,650

source_connection_id     count
synth_src_changing_01    99501
synth_src_changing_02    61464
synth_src_changing_03    70699
synth_src_changing_04    58554
synth_src_changing_05    31519
synth_src_changing_06    65418
synth_src_changing_07    64110
synth_src_changing_08    46385
```

- Filtering to one source returned **70,699** rows, matching that source
  exactly — partition pruning works off `partitionValues`.
- Cross-checked against the Iceberg side of the same table: both return
  **497,650 rows** and a total `freight_cost` of **3,736,170,232.47**.
  Identical.
- The physical Parquet has **31** columns and does not contain
  `source_connection_id`; the Delta schema declares **32** and supplies the
  last from the log. Confirmed by reading a spliced file's footer directly.

## Reading it from Databricks

Quickest check, no table creation:

```sql
SELECT * FROM delta.`s3://<bucket>/<prefix>/shipments` LIMIT 10;
```

Registering it:

```sql
CREATE TABLE my_catalog.my_schema.unified_shipments
USING DELTA LOCATION 's3://<bucket>/<prefix>/shipments';
```

**Put the Delta root inside the same storage prefix as the source data.**
A single Unity Catalog external location then covers both the `_delta_log`
and every source's Parquet. If the log sits outside that prefix you need
either two external locations or one spanning the whole bucket, and the
symptom of getting it wrong is confusing: the log reads fine and the data
files 403.

## Protocol choice

```json
{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
```

The permissive floor, chosen deliberately — not inherited from what MDLS
emits. Any Delta reader ever shipped can read it.

Reader v1 means **no column mapping**, which is the point: bumping to
reader v2 for column mapping in `id` mode would resolve columns by Parquet
field id and drag the entire Iceberg field-id hazard into the Delta path.
Writer v2 is what `partitionColumns` and table properties need.

Nothing above 1/2 earns its keep, and the reason is consistent:

| Feature | Needs | Why not |
|---|---|---|
| Column mapping | reader 2 | Reintroduces field-id resolution — the hazard being avoided |
| Deletion vectors | reader 3 | Source files are plain Parquet; nothing to honour |
| CHECK constraints | writer 3 | Nothing writes to this table |
| Generated columns | writer 4 | The source column comes from `partitionValues` already |
| Change data feed | writer 4 | Needs real commit history, not a regenerated snapshot |
| Row tracking / liquid clustering | reader 3 / writer 7 | Assume Delta owns the data layout; Fivetran does |

The pattern: everything above the floor assumes Delta manages the files.
Here Fivetran's writer does, so those features are meaningless or harmful.

## Caveats and limits

**The table is read-only by construction.** Not a protocol restriction — an
`INSERT` or `OPTIMIZE` from Databricks would write new Parquet under the
Delta root that the Iceberg manifest knows nothing about, desynchronising
the two views. `delta.appendOnly` is available at writer v2 but only blocks
deletes and updates, not appends, so it is a partial guard at best. Treat
the Delta table as a projection, not a table you own.

**delta-rs cannot read it.** Version 1.6.5 treats an absolute `add.path` as
relative, joins it to the table root and mangles the URI scheme. The log is
protocol-correct — Spark proves it — but any delta-rs-based client (Polars,
several Python Delta tools) will fail with a 404. Spark and Databricks are
fine. Verify before pointing a non-Spark reader at this.

**It is a point-in-time snapshot.** Re-running `register_consolidation.py`
updates the Iceberg manifest and does *nothing* to the Delta log, which
then silently serves a stale file list. Regeneration is currently manual and
unlinked. Anything operational needs the two regenerated together.

**Stats are `numRecords` only.** Valid and enough for row-count shortcuts,
but Databricks gets no min/max data skipping. Iceberg's bounds are
field-id-keyed binary blobs; translating them is possible and is the single
highest-value improvement here.

**Source-side file maintenance breaks it, same as Iceberg.** The log
references source Parquet by absolute path, so compaction, snapshot expiry
or `VACUUM` at a source deletes files out from under it. Identical race to
the Iceberg gap, and worth noting Delta's own `VACUUM` semantics make this
a familiar failure for Databricks users.

**MDLS's own Delta output is optional and may be off.** Fivetran can emit
Delta metadata per connection, but it is a setting — several connections in
the validation lake had Iceberg metadata only. Irrelevant to this script
(it generates the consolidated log itself from the Iceberg side) but
relevant to any assumption that source tables already have `_delta_log`.

## Not done yet

In rough order of value:

1. **Min/max stats**, translated from Iceberg bounds. Unlocks data skipping.
2. **Regeneration tied to consolidation** — either a flag on
   `register_consolidation.py` or a documented paired run, so the Delta log
   cannot silently go stale.
3. **Incremental commits** instead of rewriting commit 0, so the Delta table
   has real version history and time travel means something.
4. **A `verify` equivalent** — the Iceberg side has
   `verify_consolidation.py` re-deriving row counts independently; the
   Delta side has no such check.
5. **Unity Catalog registration**, if customers want a managed table entry
   rather than a path-based read.

## Reference: what MDLS itself emits

From a source table's MDLS-generated `_delta_log` in the validation lake,
for comparison rather than as a constraint:

```json
{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
{"metaData": {"configuration": {"delta.checkpointInterval": "1"}, "partitionColumns": []}}
```

Same protocol floor, no column mapping, no deletion vectors, plain lowercase
column names with `primaryKey` metadata on key fields. Worth knowing because
it confirms the physical files carry unmangled names — which is what makes
name-based resolution across sources safe. It does **not** constrain what
the consolidated log declares; that is an independent table we author.
