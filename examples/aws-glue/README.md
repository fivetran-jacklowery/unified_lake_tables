# AWS Glue Data Catalog variant

Runs the same consolidation against a customer-managed **AWS Glue Data
Catalog** instead of a Fivetran-managed Polaris catalog. If your Iceberg
tables' catalog is Glue -- a common setup when you're running your own AWS
Glue / Athena / EMR / S3 Iceberg stack -- start here.

## How this works now (changed — read this if you used the old version)

Glue is a **backend switch inside the one validated implementation**, not a
separate script:

```
CATALOG_BACKEND=glue   ->  scripts/register_consolidation.py talks to GlueCatalog
CATALOG_BACKEND=polaris ->  (default) same file talks to Polaris via RestCatalog
```

`register_consolidation_glue.py` in this directory is now a ~90-line adapter
that sets that variable and calls the main script. It contains no
consolidation logic of its own.

**This used to be a full fork** — an 839-line copy of the main script with
only the catalog construction changed. Its header promised everything else
was "copied verbatim, unmodified, from the validated Polaris version." That
was true the day it was written and false within weeks: the main script went
on to receive a series of field-id correctness fixes, and the fork received
none of them, because a copy doesn't announce that its original has moved.

A customer ran that fork against a six-tenant SQL Server replication in Glue,
where the same column carries a different Iceberg field id in each tenant's
table. The fork spliced files by reference whose embedded field ids
disagreed with the target's schema, producing a consolidated table that
returned the **wrong column's values** — and reported success with zero
rewrites. The fixes that prevent exactly this had been in the main script for
some time. See the repo root `CHANGELOG.md`.

If you have a local copy of the old `register_consolidation_glue.py`, replace
it. You can check what you have in one command:

```bash
grep -c 'src_field.name != target_name' register_consolidation_glue.py
```

Anything other than `0` on the *new* adapter means you're looking at the old
fork (the new adapter holds no such logic — the main script does).

## The one real difference: credentials

Polaris **vends** short-lived, per-table-scoped AWS storage credentials
automatically on every table load, and the Polaris path deliberately strips
ambient AWS credentials from the environment to force that vended-only path.
Glue has no equivalent mechanism and vends nothing. The IAM principal (role,
user, or instance profile) running this needs its own **standing** AWS
credentials with:

- **Glue permissions** across every source database this tool reads
  (`glue:GetDatabase`, `glue:GetTable`, `glue:GetTables`,
  `glue:GetPartitions`) and create/alter permissions on the target database
  and tables (`glue:CreateDatabase`, `glue:CreateTable`, `glue:UpdateTable`).
- **S3 permissions** across every source table's data location (read) and
  the target table's data location (read + write).

Because there's no per-table credential vending under Glue, this is
necessarily a **broader IAM footprint** than the Polaris path needs. Size the
IAM policy accordingly, and consider scoping by S3 prefix or bucket policy if
your sources span more locations than you want one role to reach. This is a
real trade-off, not a footnote -- worth discussing with whoever owns IAM
policy on your side before running this broadly.

### Why the adapter sets `CATALOG_BACKEND` in code

The main module decides **at import time** whether to purge ambient AWS
credentials, and on Glue those credentials are the only ones there are.
Setting `CATALOG_BACKEND` in `.env` is too late — `load_dotenv()` doesn't run
until `main()`. That single ordering constraint is the entire reason this
wrapper exists instead of a README line telling you to export a variable.
The main script detects the too-late case and fails with an explicit message
rather than letting it surface as an opaque S3/Glue auth error.

Either of these works:

```bash
python examples/aws-glue/register_consolidation_glue.py     # adapter sets it for you
export CATALOG_BACKEND=glue && python scripts/register_consolidation.py
```

## What's proven on Glue

The core mechanism was validated against a live Fivetran-managed Polaris
catalog -- see the repo root's `CHANGELOG.md` and `docs/HOW_IT_WORKS.md`.
Collapsing the fork means Glue inherits all of that, and every future fix,
automatically.

**The Glue path itself has now also been run end to end against a real AWS
Glue Data Catalog + S3**, which it had not been before. `tests/test_glue_live.py`
creates per-tenant Iceberg tables with divergent field ids, consolidates
them, and checks the result — 9/9 passing. The two assumptions this section
used to carry as unverified are now findings:

1. **The reserved-field-id trick holds on Glue.** `create_target_table()`
   forces `source_connection_id` to field id 100000 by assigning an
   `itertools.count` to a private pyiceberg `UpdateSchema` attribute. On a
   live Glue catalog it lands at exactly 100000.
2. **Glue's `CreateTable` does not renumber field ids.** Asked for ids 1-4,
   got ids 1-4. The two-step create-then-widen dance is still correct, and
   nothing Glue-specific is needed around it.

Also confirmed live on Glue, not just against the local SQLite catalog the
rest of `tests/` uses: no row loss, no rerun duplication, NULLs preserved,
no field-id crossover, manifest bounds matching declared types (the Snowflake
symptom), and a clean `verify_consolidation.py`.

That test is opt-in, because it creates real AWS resources:

```bash
ULT_LIVE_GLUE=1 python tests/test_glue_live.py
```

It needs Glue create/delete on its own timestamped databases and S3
create/delete on its own bucket, and removes everything in a `finally` block.
Re-run it after any pyiceberg upgrade — both findings above depend on library
internals that could change without a deprecation warning.

**Still unexercised:** decimal precision/scale divergence across sources.
pyiceberg has a known decimal-promotion bug (see `CHANGELOG.md`), and real
per-tenant replicas often differ here, so validate that case against your own
data before trusting a large run.

## Before you run this against many tenants

If your sources are per-tenant replicas of one logical schema, check whether
they actually agree on field ids **before** consolidating anything. Tables
that land the same column on different field ids are correct but expensive
here: each divergent source gets 0 files spliced and 100% of its rows
physically rewritten, which forfeits the no-rewrite premise that is the point
of this tool.

The run tells you up front — `find_field_id_disagreements()` is a pre-flight
that executes before any target table is created, and prints a per-source
table of how many files will be rewritten. Read that output before letting a
large first run proceed. See the repo root `CHANGELOG.md` for the measured
difference between base-schema divergence (~100% of files, loud warning) and
ordinary drift (recent files only, quiet INFO).

### Fixed in this version: NULL rows were dropped on the rewrite path

If you ran an **earlier** build, consolidated tables produced by it may be
missing rows. A customer hit this on 2026-10-13: rows whose drifted column
was NULL never reached the target (80 missing across six tenants, exactly
matching the 80 NULLs), and reruns duplicated the rows that did land. Both
are fixed, with regression tests at `tests/test_collision_rewrite.py`. Full
detail in the repo root `CHANGELOG.md`.

It bit on Glue specifically because base-schema field-id divergence is what
routes a source onto the rewrite path, and that is the normal case for
per-tenant replicas.

**If you consolidated with an earlier build, re-verify.** The target is not
self-healing for the duplicate half — rerunning an affected table against an
existing target can leave the extra rows in place. Safest is to drop the
affected target tables and re-register from scratch.

Either way, always run `scripts/verify_consolidation.py` after a
consolidation that reported any rewrites. Its per-source row-count check is
what caught this, and a mismatch means do not trust the target table.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env      # fill in GLUE_WAREHOUSE_PATH at minimum
cp config.example.yaml config.yaml   # list your source databases
python register_consolidation_glue.py
```

If you're running this somewhere that already has an IAM role attached (EC2,
ECS, Lambda, CodeBuild, etc. -- recommended), you can leave every `AWS_*`
variable in `.env` blank; boto3's default credential chain picks up the
attached role automatically. `GLUE_WAREHOUSE_PATH` is the one value you
always need to set.

Scheduling this on Lambda works too — see `examples/aws-lambda/`, which sets
`CATALOG_BACKEND` as an ordinary Lambda environment variable.

## Everything else

Config shape, the known limitations (renamed/dropped columns, changed data
types, and which rename scenario is fixed versus which one silently goes
stale), and the overall "what this does NOT yet handle" list are identical to
the Polaris original -- see the repo root `README.md`. None of that is
catalog-specific.
