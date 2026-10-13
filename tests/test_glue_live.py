#!/usr/bin/env python3
"""LIVE end-to-end test against a real AWS Glue Data Catalog + S3.

OPT-IN. This is the only test here that touches a cloud account, creates
real resources, and costs real (trivial) money, so it refuses to run unless
you ask for it explicitly:

    ULT_LIVE_GLUE=1 python tests/test_glue_live.py

Everything else in tests/ runs against a local SQLite catalog. This exists
because that can never answer two questions the Glue README carried as
unverified assumptions from the day the Glue path was written:

  A. Does the reserved-field-id trick survive Glue? create_target_table()
     assigns an itertools.count to a PRIVATE pyiceberg UpdateSchema attribute
     to force source_connection_id to field id 100000. That is library
     internals, and "it should behave the same on any catalog" was an
     expectation, not a finding.
  B. Does Glue's own CreateTable renumber field ids? The two-step
     create-then-widen dance exists because of how Polaris behaves; nobody
     had checked Glue.

Both now answered -- see CHANGELOG.md. Keep this runnable so they stay
answered, because a pyiceberg or Glue upgrade can change either.

Fivetran is not writing into the catalog this was developed against, so the
sources here are built to mimic what it produces: per-tenant tables with
identical column names and types but DIFFERENT field ids, and NULLs in the
drifted column. Field-id layout is the thing under test, and that is pure
metadata -- the data's provenance is irrelevant to it.

Everything is created under a unique timestamped prefix and removed in a
finally block, so a failure mid-run still cleans up. Requires Glue
create/delete on its own databases and S3 create/delete on its own bucket.
"""
import os
import sys
import time
import traceback

REGION = os.environ.get("ULT_LIVE_GLUE_REGION", "us-east-1")
BUCKET_PREFIX = os.environ.get("ULT_LIVE_GLUE_BUCKET_PREFIX", "ult-glue-test")

if os.environ.get("ULT_LIVE_GLUE") != "1":
    print(__doc__)
    print("SKIPPED: set ULT_LIVE_GLUE=1 to run this against real AWS.")
    sys.exit(0)

STAMP = time.strftime("%Y%m%d%H%M%S")
BUCKET = f"{BUCKET_PREFIX}-{STAMP}"
DB_A, DB_B, DB_T = (f"ult_test_{STAMP}_src_a", f"ult_test_{STAMP}_src_b", f"ult_test_{STAMP}_tgt")

# MUST precede importing register_consolidation -- it decides at import time
# whether to purge ambient AWS credentials, and on Glue those are the only
# credentials there are. See the Glue adapter's module docstring.
os.environ["CATALOG_BACKEND"] = "glue"
os.environ["AWS_REGION"] = REGION
os.environ["GLUE_WAREHOUSE_PATH"] = f"s3://{BUCKET}/warehouse/"
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import boto3  # noqa: E402
import pyarrow as pa  # noqa: E402
from pyiceberg.schema import Schema  # noqa: E402
from pyiceberg.types import BooleanType, LongType, NestedField, StringType  # noqa: E402

s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)

N = 10
SCHEMAS = {
    DB_A: Schema(  # baseline
        NestedField(1, "accountid", StringType(), required=False),
        NestedField(2, "custodiancash", LongType(), required=False),
        NestedField(3, "excludeall", BooleanType(), required=False),
        NestedField(4, "reconciliationstatus", StringType(), required=False),
    ),
    DB_B: Schema(  # same names and types, SHUFFLED ids -- the customer's shape
        NestedField(1, "reconciliationstatus", StringType(), required=False),
        NestedField(2, "excludeall", BooleanType(), required=False),
        NestedField(3, "accountid", StringType(), required=False),
        NestedField(4, "custodiancash", LongType(), required=False),
    ),
}
DATA = {
    DB_A: {"accountid": [f"a{i}" for i in range(N)],
           "custodiancash": list(range(N)),
           "excludeall": [i % 2 == 0 for i in range(N)],
           "reconciliationstatus": [f"ra{i}" for i in range(N)]},
    # NULLs in the drifted column -- the exact condition that lost 80 rows
    DB_B: {"reconciliationstatus": [None if i % 3 == 0 else f"rb{i}" for i in range(N)],
           "excludeall": [i % 3 == 0 for i in range(N)],
           "accountid": [f"b{i}" for i in range(N)],
           "custodiancash": [1000 + i for i in range(N)]},
}
EXPECTED_NULLS = sum(1 for v in DATA[DB_B]["reconciliationstatus"] if v is None)

results = []


def check(name, ok, detail=""):
    results.append((name, bool(ok), detail))
    print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f"  -- {detail}" if detail else ""))


def cleanup():
    print("\n=== cleanup ===")
    for db in (DB_A, DB_B, DB_T):
        try:
            for t in glue.get_tables(DatabaseName=db).get("TableList", []):
                glue.delete_table(DatabaseName=db, Name=t["Name"])
            glue.delete_database(Name=db)
            print(f"  dropped glue db {db}")
        except Exception as e:  # noqa: BLE001
            print(f"  (glue {db}: {type(e).__name__})")
    try:
        pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET)
        keys = [{"Key": o["Key"]} for p in pages for o in p.get("Contents", [])]
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys[i:i + 1000]})
        s3.delete_bucket(Bucket=BUCKET)
        print(f"  deleted s3://{BUCKET} ({len(keys)} objects)")
    except Exception as e:  # noqa: BLE001
        print(f"  (s3 {BUCKET}: {type(e).__name__}: {e})")


try:
    print(f"=== setup: s3://{BUCKET} in {REGION} ===")
    kw = {} if REGION == "us-east-1" else {"CreateBucketConfiguration": {"LocationConstraint": REGION}}
    s3.create_bucket(Bucket=BUCKET, **kw)
    for db in (DB_A, DB_B, DB_T):
        glue.create_database(DatabaseInput={"Name": db})

    import register_consolidation as rc

    cat = rc.get_catalog()
    check("backend resolves to GlueCatalog", type(cat).__name__ == "GlueCatalog",
          type(cat).__name__)

    print("\n=== create sources (mimicking Fivetran's metadata shape) ===")
    for db, schema in SCHEMAS.items():
        t = cat.create_table(f"{db}.account", schema=schema)
        cols = [f.name for f in schema.fields]
        t.append(pa.table({c: DATA[db][c] for c in cols}).cast(schema.as_arrow()))
        print(f"  {db}: {({f.name: f.field_id for f in cat.load_table(f'{db}.account').schema().fields})}")

    got_ids = {f.name: f.field_id for f in cat.load_table(f"{DB_A}.account").schema().fields}
    want_ids = {f.name: f.field_id for f in SCHEMAS[DB_A].fields}
    check("ASSUMPTION B: Glue CreateTable preserves field ids", got_ids == want_ids,
          f"asked {want_ids}, got {got_ids}")

    print("\n=== consolidate (CATALOG_BACKEND=glue) ===")
    cfg = {"source_namespaces": [DB_A, DB_B], "target_namespace": DB_T,
           "source_id_column": "source_connection_id",
           "table_workers": 1, "source_workers": 1}
    r1 = rc.run(cfg, tables=["account"])
    r2 = rc.run(cfg, tables=["account"])
    print(f"  run 1: spliced={r1['total_spliced']} rewritten={r1['total_rewritten']}")
    print(f"  run 2: spliced={r2['total_spliced']} rewritten={r2['total_rewritten']}")

    tgt = cat.load_table(f"{DB_T}.account")
    tgt_ids = {f.name: f.field_id for f in tgt.schema().fields}
    check("ASSUMPTION A: reserved field id 100000 holds on Glue",
          tgt_ids.get("source_connection_id") == rc.RESERVED_FIELD_ID_BASE,
          f"source_connection_id = {tgt_ids.get('source_connection_id')}")

    got = tgt.scan().to_arrow()
    rows = got.to_pylist()
    per_src = {db: sum(1 for r in rows if r["source_connection_id"] == db) for db in (DB_A, DB_B)}
    print(f"  rows={got.num_rows} per-source={per_src}")

    check("no row loss", got.num_rows == 2 * N, f"{got.num_rows} != {2 * N}")
    check("no rerun duplication", r2["total_spliced"] == 0 and r2["total_rewritten"] == 0)
    check("NULLs preserved", got.column("reconciliationstatus").null_count == EXPECTED_NULLS,
          f"{got.column('reconciliationstatus').null_count} != {EXPECTED_NULLS}")

    crossover = []
    for db in (DB_A, DB_B):
        srows = [r for r in rows if r["source_connection_id"] == db]
        for col, exp in DATA[db].items():
            key = lambda v: (v is None, str(v))  # noqa: E731
            if sorted((r[col] for r in srows), key=key) != sorted(exp, key=key):
                crossover.append(f"{db}.{col}")
    check("no field-id crossover", not crossover, ", ".join(crossover))

    widths = {"boolean": 1, "int": 4, "float": 4, "long": 8, "double": 8}
    by_id = {f.field_id: f for f in tgt.schema().fields}
    bad = []
    for task in tgt.scan().plan_files():
        for fid, raw in (task.file.lower_bounds or {}).items():
            fld = by_id.get(fid)
            if fld is None:
                bad.append(f"unknown field id {fid}")
            elif widths.get(str(fld.field_type)) not in (None, len(raw)):
                bad.append(f"{fld.name}({fld.field_type}) {len(raw)}B")
    check("manifest bounds match declared types (Snowflake symptom)", not bad, ", ".join(bad))

    print("\n=== verify_consolidation.py against Glue ===")
    import verify_consolidation as vc

    check("verify_consolidation passes",
          vc.verify_table(cat, "account", [DB_A, DB_B], DB_T, "source_connection_id"))

except Exception:  # noqa: BLE001
    traceback.print_exc()
    results.append(("harness did not raise", False, "see traceback"))
finally:
    cleanup()

print("\n" + "=" * 60)
failed = [n for n, ok, _ in results if not ok]
print(f"{len(results) - len(failed)}/{len(results)} passed"
      + (f"   FAILED: {failed}" if failed else "   ALL PASS"))
sys.exit(1 if failed else 0)
