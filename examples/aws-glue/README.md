# AWS Glue Data Catalog variant

A direct adaptation of `register_consolidation.py` (the Fivetran-managed
Polaris/MDLS original in this repo's `scripts/` directory) for a
customer-managed **AWS Glue Data Catalog** instead of a Fivetran-managed
Polaris catalog. If your Iceberg tables' catalog is Glue -- a common setup
when you're running your own AWS Glue / Athena / EMR / S3 Iceberg stack --
this is the file to start from.

## What actually changed

Exactly one thing: how the catalog connection is constructed
(`catalog_properties()` and `_make_catalog()` in `register_consolidation_glue.py`).
Everything else -- the reserve/splice/detect-collisions/rewrite/retire-orphaned-files
mechanism, `register_table()`, `file_has_drift_beyond()`,
`already_registered_by_source()` -- is copied verbatim from the validated
Polaris version. That logic operates entirely on pyiceberg
Table/Schema/Transaction objects once a table is loaded, which is
catalog-agnostic: `GlueCatalog` and `RestCatalog` both implement the same
abstract pyiceberg `Catalog` interface (confirmed directly against the
installed pyiceberg 0.11.1 library, not assumed from documentation).

## The one real difference: credentials

Polaris **vends** short-lived, per-table-scoped AWS storage credentials
automatically on every table load. AWS Glue Data Catalog has no equivalent
mechanism. The IAM principal (role, user, or instance profile) running this
script needs its own **standing** AWS credentials with:

- **Glue permissions** across every source database this tool reads
  (`glue:GetDatabase`, `glue:GetTable`, `glue:GetTables`,
  `glue:GetPartitions`) and create/alter permissions on the target database
  and tables (`glue:CreateDatabase`, `glue:CreateTable`, `glue:UpdateTable`).
- **S3 permissions** across every source table's data location (read) and
  the target table's data location (read + write).

Because there's no per-table credential vending under Glue, this is
necessarily a **broader IAM footprint** than the Polaris version needs.
Size the IAM policy accordingly, and consider scoping by S3 prefix or
bucket policy if your sources span more locations than you want one role
to reach. This is a real trade-off, not a footnote -- worth discussing
with whoever owns IAM policy on your side before running this broadly.

## What's proven versus what's adapted-but-unverified

The core mechanism was validated against a live Fivetran-managed Polaris
catalog -- see the repo root's `CHANGELOG.md` and `docs/HOW_IT_WORKS.md`
for exactly what was tested, including real reproductions of the
copy-on-write duplication bug and its fix, and the full history of what
schema-drift scenarios have and haven't been exercised.

**The catalog swap in this example has not yet been run against a live AWS
Glue Data Catalog with real source tables.** It's a direct, mechanical
adaptation based on pyiceberg's shared `Catalog` interface, not something
independently re-tested end to end the way the Polaris version was before
this repo's 1.0.0 release. Two specific things flagged in the code
comments as carried-over assumptions rather than re-confirmed facts:

1. The reserved-field-id trick in `create_target_table()` (an
   `itertools.count` assigned to a private pyiceberg `UpdateSchema`
   attribute) is pyiceberg-library-internal and *should* behave identically
   regardless of catalog backend -- but that's an expectation carried over
   from Polaris testing, not independently confirmed against Glue.
2. Whether Glue's own `CreateTable` handling renumbers field ids on create
   the same way Polaris's REST catalog does (the reason the two-step
   create-then-widen dance exists at all) hasn't been separately verified.

Treat this as a starting point to validate in a sandbox/dev Glue catalog
first, the same way the Polaris original was validated before being
trusted against anything real.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env      # fill in GLUE_WAREHOUSE_PATH at minimum
cp config.example.yaml config.yaml   # list your source databases
python register_consolidation_glue.py
```

If you're running this somewhere that already has an IAM role attached
(EC2, ECS, Lambda, CodeBuild, etc. -- recommended), you can leave every
`AWS_*` variable in `.env` blank; boto3's default credential chain picks up
the attached role automatically. `GLUE_WAREHOUSE_PATH` is the one value
you always need to set.

## Everything else

Config shape, the known limitations (renamed/dropped columns, changed data
types, and which rename scenario is fixed versus which one silently goes
stale), and the overall "what this does NOT yet handle" list are identical
to the Polaris original -- see the repo root `README.md`. None of that is
catalog-specific.
