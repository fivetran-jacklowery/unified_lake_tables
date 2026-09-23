#!/usr/bin/env python3
"""generate_delta_log.py -- emit a Delta Lake `_delta_log` over a consolidated
Iceberg table's data files, so Databricks can read the same consolidated
table Snowflake/Trino read through Iceberg.

EXPERIMENTAL. Iceberg is this project's primary path; this is a secondary
projection for Databricks customers. See docs/DELTA_LAKE_SUPPORT.md for the
full rationale, what was validated live, and the caveats -- read that before
using this for anything that matters.

THE IDEA, in one paragraph: a consolidated Iceberg table's manifest already
points at every source's Parquet files by absolute path. Delta's `add.path`
may also be an absolute URI -- that is the mechanism behind Delta shallow
clone -- so a `_delta_log` can point at exactly the same files. The result
is two independent metadata layers over one shared set of data files. No
bytes are copied, and the Iceberg side is untouched.

THREE DELIBERATE CHOICES, each explained at length in the doc:

  1. Column mapping OFF (reader protocol 1). Delta then resolves columns by
     NAME out of the Parquet footer. This is the whole reason the Delta
     projection is structurally safer than the Iceberg one: `id` mode would
     resolve by Parquet field id and reimport the field-id collision hazard
     that most of register_consolidation.py exists to defend against.
  2. `source_id_column` is a Delta PARTITION column. Spliced files are the
     sources' own Parquet and do not physically contain it; Delta supplies
     partition values from the log. Iceberg needed a reserved-field-id trick
     for the same effect.
  3. Protocol 1/2 -- the permissive floor. Everything above it assumes Delta
     owns the data layout, which it does not here: Fivetran's writer does.

READ-ONLY BY CONSTRUCTION. Do not write to the resulting table. An INSERT or
OPTIMIZE from Databricks would land new Parquet under the Delta root that
the Iceberg manifest knows nothing about, desynchronising the two views.
"""
import argparse
import json
import logging
import os
import sys
import time
from urllib.parse import quote

from dotenv import load_dotenv

# Reuse register_consolidation.py's config/credential plumbing rather than
# re-implementing it, exactly as verify_consolidation.py and
# bench_vs_union.py do -- so this can never drift from how the rest of the
# toolkit authenticates or resolves sources.
from register_consolidation import catalog_properties, load_config
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.types import (
    BinaryType, BooleanType, DateType, DecimalType, DoubleType, FixedType,
    FloatType, IntegerType, LongType, StringType, TimestampType,
    TimestamptzType, TimeType, UUIDType,
)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("generate_delta_log")


def delta_type(t):
    """Iceberg type -> Delta type string.

    Deliberately explicit rather than a best-effort fallback: an unmapped
    type must fail loudly here, not silently produce a log that mis-declares
    a column and returns wrong values at read time.
    """
    if isinstance(t, BooleanType):
        return "boolean"
    if isinstance(t, IntegerType):
        return "integer"
    if isinstance(t, LongType):
        return "long"
    if isinstance(t, FloatType):
        return "float"
    if isinstance(t, DoubleType):
        return "double"
    if isinstance(t, DateType):
        return "date"
    if isinstance(t, (TimestampType, TimestamptzType)):
        return "timestamp"
    if isinstance(t, (StringType, UUIDType, TimeType)):
        return "string"
    if isinstance(t, (BinaryType, FixedType)):
        return "binary"
    if isinstance(t, DecimalType):
        return f"decimal({t.precision},{t.scale})"
    raise SystemExit(
        f"Unmapped Iceberg type {t!r}. Add it to delta_type() rather than "
        "guessing -- a wrong mapping here returns wrong values at read time."
    )


def build_commit(tbl, source_id_column: str, table_name: str) -> tuple:
    """Build the list of Delta actions for commit 0. Returns (actions, stats)."""
    schema = tbl.schema()

    # Partition columns belong in the schema AND in partitionColumns; they
    # are simply absent from the data files.
    fields = [
        {
            "name": f.name,
            "type": delta_type(f.field_type),
            "nullable": not f.required,
            "metadata": {},
        }
        for f in schema.fields
    ]
    if not any(f["name"] == source_id_column for f in fields):
        raise SystemExit(
            f"Table has no column {source_id_column!r} -- is this actually a "
            "consolidated table produced by register_consolidation.py?"
        )

    adds, total_rows, per_source = [], 0, {}
    now_ms = int(time.time() * 1000)
    for task in tbl.scan().plan_files():
        f = task.file
        part_val = f.partition[0]
        per_source[part_val] = per_source.get(part_val, 0) + 1
        total_rows += f.record_count
        adds.append({
            "add": {
                # Absolute URI -- the same mechanism Delta shallow clone uses.
                # safe=":/" keeps the scheme separator intact.
                "path": quote(f.file_path, safe=":/"),
                "partitionValues": {source_id_column: part_val},
                "size": f.file_size_in_bytes,
                "modificationTime": now_ms,
                "dataChange": True,
                # numRecords only. Iceberg's min/max bounds are field-id-keyed
                # binary blobs; translating them is possible and would buy
                # extra file skipping, but it is not required for correctness.
                # See docs/DELTA_LAKE_SUPPORT.md, "Not done yet".
                "stats": json.dumps({"numRecords": f.record_count}),
            }
        })

    if not adds:
        raise SystemExit("Table planned no files -- nothing to write.")

    actions = [
        {"commitInfo": {
            "timestamp": now_ms,
            "operation": "CREATE TABLE",
            "operationParameters": {"partitionBy": f'["{source_id_column}"]'},
            "isBlindAppend": True,
            "engineInfo": "unified-lake-tables generate_delta_log.py",
        }},
        # Protocol floor on purpose -- see module docstring, choice 3.
        {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
        {"metaData": {
            "id": f"unified-lake-tables-{table_name}",
            "name": table_name,
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json.dumps({"type": "struct", "fields": fields}),
            "partitionColumns": [source_id_column],
            "configuration": {},
            "createdTime": now_ms,
        }},
    ] + adds

    return actions, {"files": len(adds), "rows": total_rows, "per_source": per_source,
                     "columns": len(fields)}


def main():
    ap = argparse.ArgumentParser(
        description="Generate a Delta _delta_log over a consolidated Iceberg table."
    )
    ap.add_argument("table", help="Table name within the target namespace, e.g. shipments")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument(
        "--out",
        help="Local path to write the commit JSON. Default: ./_delta_log/00000000000000000000.json",
        default="_delta_log/00000000000000000000.json",
    )
    args = ap.parse_args()

    load_dotenv()
    cfg = load_config(args.config)
    cat = RestCatalog("fivetran_mdls_delta", **catalog_properties())

    target = f"{cfg['target_namespace']}.{args.table}"
    if not cat.table_exists(target):
        raise SystemExit(f"{target} does not exist -- run register_consolidation.py first.")

    tbl = cat.load_table(target)
    actions, stats = build_commit(tbl, cfg["source_id_column"], args.table)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as fh:
        for action in actions:
            fh.write(json.dumps(action) + "\n")

    logger.info("table        %s", target)
    logger.info("columns      %d (partition: %s)", stats["columns"], cfg["source_id_column"])
    logger.info("add actions  %d file(s), %s row(s)", stats["files"], f"{stats['rows']:,}")
    logger.info("sources      %d -> %s", len(stats["per_source"]),
                ", ".join(f"{k}:{v}" for k, v in sorted(stats["per_source"].items())))
    logger.info("wrote        %s (%s bytes)", args.out, f"{os.path.getsize(args.out):,}")
    logger.info("")
    logger.info("Upload it to the Delta table root you want Databricks to read, e.g.:")
    logger.info("  aws s3 cp %s s3://<bucket>/<prefix>/%s/_delta_log/00000000000000000000.json",
                args.out, args.table)
    logger.info("")
    logger.info("Put that root INSIDE the same storage prefix as your source data if you")
    logger.info("can -- one external location then covers both the log and the Parquet.")
    logger.info("See docs/DELTA_LAKE_SUPPORT.md before using this for anything real.")


if __name__ == "__main__":
    main()
