# unified-lake-tables

> A note on the name: "unified-lake-tables" is just what this capability is
> called in this repo right now -- it isn't an officially locked-in Fivetran
> product name. Rename the directory, the package, whatever you like; nothing
> here depends on this specific name.

No-rewrite consolidation of many structurally-identical Iceberg tables (one
per tenant, one per regional database, one per customer -- however your
Fivetran connections are split up) into a single queryable table per table
type, on a Fivetran-managed Polaris catalog and Managed Data Lake Service
(MDLS) destination, without rewriting any of the underlying data.

## What this does, and why

If you run N connections of the same connector (say, one SQL Server
connection per tenant database, all landing the same table shapes), you
end up with N copies of every table instead of one. The usual fixes to
that both cost more the more data you have: a `UNION ALL` view re-scans
every source's full data on every single query, and a materialized merge
job physically rewrites everything into one table, every time it runs,
whether or not anything actually changed since the last run.

Here's a useful way to think about what this tool does instead. Imagine N
libraries, each with its own card catalog and its own shelves of books. A
`UNION ALL` view is like re-reading every relevant book from every library
every time someone asks a question. A merge job is like photocopying every
relevant book from every library onto one new shelf on a schedule, whether
or not anything changed. This tool builds one new card catalog whose cards
point directly at books still sitting on the original libraries' shelves.
Building that catalog is fast and cheap, because it never touches a single
book. Looking things up through it is still fast, because a query engine
following those pointers goes straight to the right shelf -- it never has
to reconstruct anything.

Concretely: Apache Iceberg tables are mostly metadata. The "table" is a
manifest listing which physical Parquet files exist and what's in them; the
actual data lives in ordinary files in object storage. This tool builds a
new target table whose manifest points at your existing sources' physical
files directly -- no data copied, no data rewritten -- and adds a
bookkeeping column identifying which source each row came from, which also
happens to be a real partition key, so filtering to one source at query
time prunes to just that source's files.

The harder, more interesting part -- and the actual reason this needs more
than an afternoon of manifest-splicing code -- is a real correctness risk
this technique introduces: Iceberg resolves columns by an internal number
(a "field id") baked into each file, not by column name. Two sources that
evolve their schemas independently can land a new column on the exact same
field id with two completely different meanings, and Iceberg won't error --
it'll silently return the wrong column's data. This was reproduced against
real infrastructure, not just reasoned about, and this tool includes the
validated three-layer fix for it (reserve / widen-for-free / rewrite-only-
on-collision). See [docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md) for the full
mechanism.

## Prerequisites

- A Fivetran destination on **Managed Data Lake Service (MDLS)**, backed by
  Fivetran's **Polaris** Iceberg REST catalog.
- OAuth **client-credentials** for that catalog integration: a catalog URI,
  a token endpoint, a client id/secret pair, a scope, and a warehouse name.
  Find these in the Fivetran dashboard on your MDLS destination's setup
  page (look for "catalog integration," "Polaris," or "Iceberg REST
  catalog" -- exact labeling varies by dashboard version). If you can't
  find them, ask your Fivetran account team for the Polaris catalog
  connection details for that destination.
- Two or more source namespaces sharing the same table names and a
  structurally compatible schema (same connector, different tenants/
  accounts/databases is the common case).
- Python 3.10+.
- **No AWS credentials of any kind.** This tool authenticates to Polaris
  with OAuth only; Polaris vends short-lived, scoped AWS storage
  credentials automatically per table load. See "Vended credentials" below.

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# edit .env with your Polaris OAuth client-credentials

cp config.example.yaml config.yaml
# edit config.yaml: list your source namespaces, name the target namespace
# and the bookkeeping/partition column

python scripts/register_consolidation.py
# then, any time you want a sanity check:
python scripts/verify_consolidation.py
```

`register_consolidation.py` auto-discovers which table names exist in
*every* listed source namespace and registers one consolidated table per
name into `target_namespace`. Re-running it is a safe no-op for sources
with nothing new (see "Idempotency" below) -- schedule it however you'd
schedule any batch job, e.g. after each source connection's sync completes.

## How it works, briefly

Four steps, run per table, per registration pass:

1. **Reserve.** The target table's own bookkeeping column gets a field id
   from a permanently out-of-band range, so it can never collide with a
   source's own future schema changes.
2. **Splice (no-rewrite).** Every source file with no schema drift, or
   drift that resolves cleanly, gets appended to the target's manifest by
   reference -- zero data movement.
3. **Detect collisions.** If two sources independently land different new
   columns on the same internal field id, that's flagged rather than
   silently resolved by luck.
4. **Rewrite (rare).** Only a genuine collision triggers an actual data
   rewrite, and only for the small number of newly-drifted rows involved,
   never a source's full history.

Full technical walkthrough, including the exact private pyiceberg mechanism
this depends on and why it took three attempts to get right: see
[docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md).

## What this does NOT yet handle

Read this before running this against anything you care about.

This tool was validated against **one specific kind of schema change: a
source adding a new nullable column.** That's it. The following are
explicitly **not** handled, are a different Iceberg mechanism entirely, and
running this against a source that has done any of them could produce
**incorrect results** with no error raised:

- **Renamed columns.** Iceberg tracks renames by field id under the hood,
  but this tool's drift detection has no way to distinguish "this is a
  rename of an existing field" from "this is a new field that happens to
  reuse an id" -- it was never tested against this case.
- **Dropped columns.** Not tested. Behavior against a target table that
  already has files referencing a since-dropped source field is unknown.
- **Changed data types**, even ostensibly-compatible promotions (e.g. `int`
  to `long`). Not tested against this pattern.
- **Merge-on-read sources / delete files.** This tool only ever splices
  `DataFileContent.DATA` entries. It has no logic for carrying forward
  position-delete or equality-delete files. If a source's Iceberg writer
  ever produces delete files (a common, cheap way to implement upserts),
  splicing that source in **silently drops the delete and resurrects a
  logically-deleted row** in the consolidated table, with zero errors
  anywhere. Whether Fivetran's actual managed-lake writer represents
  upserts this way for the connectors you're using was not checked as part
  of building this tool -- confirm this for your own connectors before
  running this against a source with row-level updates or deletes, not
  just appends.

This is explicitly not a general-purpose ETL replacement, either: it's
additive-column-safe, not transform-safe. If you need row-level
transformation, deduplication across sources, or a canonical schema that
differs in shape from every source's native schema, keep using dbt or a
real merge job.

## Vended credentials: what's actually confirmed

This was validated two different ways, and it's worth being precise about
which is which:

- **Confirmed by reading pyiceberg 0.11.1's own source** (not just its
  docs, which don't spell this out clearly): `RestCatalog` requests vended
  credentials by default (no config flag needed), and `PyArrowFileIO`
  consumes the `s3.access-key-id` / `s3.secret-access-key` / `s3.session-token`
  properties a catalog server returns for this automatically. See
  [docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md#credentials-vended-not-static)
  for the exact code paths.
- **Not re-validated live against a real Polaris catalog with real vended
  AWS credentials as part of building this repo.** The original internal
  validation that proved the underlying manifest-splicing technique works
  end-to-end (see `CHANGELOG.md`) used static AWS keys, because that
  internal sandbox had direct broad AWS access to its own test bucket --
  it did not exercise Polaris's credential-vending path at all. Swapping to
  vended credentials is a deliberate, source-code-grounded adaptation for
  this repo, not a re-run of the original validation. If you hit a
  credentials-related error on your first run, that's the part of this
  tool most likely to have a sharp edge nobody's filed off yet -- see
  Troubleshooting below and please compare notes with whoever owns this
  internally.

## Troubleshooting

**"Field id X on my target table doesn't match what I expected" / a
reserved field id didn't stick.** This tool creates target tables in two
steps on purpose: create with the source's original schema, then reserve
the bookkeeping column's field id in a *separate* schema-evolution call
made after the table exists. This is required, not stylistic -- Polaris (and
Iceberg REST catalogs generally) renumber field ids on `CREATE TABLE`
regardless of what's requested, and only honor an explicit id sent via a
later schema-update call. If you're extending this tool and see field ids
not landing where expected, you likely tried to set them at creation time.

**A rerun added rows I didn't expect, or added zero rows when I expected
some.** Re-running `register_consolidation.py` against a source with
nothing new should be a safe no-op -- it tracks already-registered file
paths in the target manifest, and separately checks row content before
re-appending on the rewrite/collision path. Run
`scripts/verify_consolidation.py` to check for duplicate files or row-count
mismatches. If you see duplicates, please report it -- an earlier,
unfixed version of this exact technique had precisely this bug (a naive
version silently doubled every row on a second run) before the
already-registered-paths check existed.

**A run logs a "COLLISION" warning.** This means two of your sources
independently added a different new column that landed on the same
internal Iceberg field id -- expected to happen eventually at scale, since
every source numbers its own schema independently from the same starting
point. This is handled automatically (see "How it works" above): the
tool picks a deterministic owner for the id within that run and rewrites
just the colliding source's drifted rows onto a fresh id, at a real but
small cost. Nothing is lost, but if you're seeing this repeatedly for the
same source across runs, that source's schema is drifting more than this
tool's cost model assumes -- worth a closer look, and the run's summary
output flags this explicitly.

**Everything works except reading actual data (`SELECT COUNT(*)` succeeds
but a real query fails on a file path).** This is a known catalog-
integration gotcha, not specific to this tool: a vended-credential catalog
integration can fail to cover a table whose manifest points at files
**outside its own default storage location** -- which is exactly what this
tool does by design (the whole point is pointing a target table's manifest
at a different table's files). If you hit this from a downstream engine
(e.g. Snowflake) reading the consolidated table, check whether that
engine's catalog integration needs a broader storage-location allowlist
than a single table's default location.

**A downstream Snowflake-side Iceberg table on a consolidated table starts
erroring after a rebuild.** If a consolidated table ever gets dropped and
recreated at the catalog level, any Snowflake-side `ICEBERG TABLE` pointed
at it will fail a plain refresh with a UUID mismatch, not just show stale
data. The fix is a full `CREATE OR REPLACE ICEBERG TABLE` on the Snowflake
side, not a refresh. This tool doesn't automate that recreation; treat it
as a manual runbook step if you're using Snowflake against these tables.

## License

Apache License 2.0. See [LICENSE](LICENSE).
