#!/usr/bin/env python3
"""realign_source_field_ids.py -- EXPERIMENTAL. THIS MODIFIES SOURCE TABLES.

    ************************************************************************
    *  Everything else in this repository only READS source tables. This   *
    *  script DROPS a Fivetran-managed source table from the catalog and   *
    *  RE-CREATES it. Try it in a dev environment first. Not supported or  *
    *  endorsed by Fivetran.                                               *
    ************************************************************************

WHY IT EXISTS
-------------
Fivetran assigns Iceberg field ids in column declaration order and does not
guarantee that order across connections, so the same table can carry a
different field-id layout in every tenant. register_consolidation.py handles
that by RELABELING: every file of a divergent source is copied with a
corrected Parquet footer -- and copied again every time the writer replaces
it (copy-on-write). On a busy table that is a copy that never ends.

This script fixes the layout AT THE SOURCE, once, without a resync: it
copies the source's current files with the target's field ids in the footer,
then re-creates the source table with a schema that uses those ids. The
writer carries on from its cursor and -- in our tests -- writes every new
file with the new ids. From then on the source agrees with the target and is
spliced by reference: no more relabel copies.

WHAT WE TESTED (2026-11-06), AND WHAT WE DID NOT
-------------------------------------------------
Tested, Polaris (Fivetran MDLS), Connector SDK source, 100k-row table:
  * swap, then three copy-on-write update syncs with no resync: the writer
    kept the new ids, values matched row for row, no errors in the platform
    log, the writer never re-created its own table;
  * reads from pyiceberg, DuckDB and Snowflake matched afterwards;
  * consolidation then spliced the source with zero relabel copies.
Tested, AWS Glue (tests/test_glue_live.py): apply, value check, rollback,
re-apply and consolidation, on tables written by pyiceberg.
NOT tested:
  * the Fivetran writer continuing into a re-created table on GLUE (only on
    Polaris);
  * database connectors (e.g. SQL Server) -- only the Connector SDK writer;
  * a source column added AFTER the swap;
  * large tables (many files): the connector stays paused for the copy;
  * concurrent readers during the swap window.

WHAT IT DOES, PER SOURCE TABLE
------------------------------
  plan (default) -- read-only. Shows the old -> new field-id mapping and the
         number of files that would be copied. Changes nothing.
  apply  -- requires --execute AND --i-understand-this-modifies-source-tables.
     1. Refuses unless the source's columns are all in the reference schema
        (by name), it is unpartitioned and unsorted, and every column is a
        primitive type.
     2. Writes a RECORD FILE (JSON) with the table's current metadata
        location -- the rollback point -- BEFORE changing anything.
     3. Copies each current data file to a new path in the same table
        location with the new field ids in its footer. Data pages are
        copied byte for byte (S3 server-side copy on Glue).
     4. Checks the source snapshot has not moved since step 2 (i.e. the
        connector really is paused). If it has, deletes the copies and stops
        without touching the table.
     5. Drops the table from the catalog (registration only -- no files are
        deleted) and re-creates it at the same location with the same
        properties, the new ids, and the copied files.
     6. Verifies: field ids read back as intended, row counts match, and a
        sample of files reads value-for-value identical to the originals.
        Any failure rolls back automatically.
     The ORIGINAL files are kept; they are what makes rollback possible.
  rollback --record FILE -- re-registers the original metadata. Only safe
         BEFORE the connector writes to the re-created table; refuses if it
         has, unless --force.
  finalize --record FILE -- deletes the original files. After this there is
         no rollback.

BEFORE YOU RUN IT
-----------------
  * PAUSE THE CONNECTOR for the table's connection, and wait for any running
    sync to finish. Step 4 catches a sync that commits mid-swap; it cannot
    stop one.
  * Run register_consolidation.py first, so the target table exists: by
    default its field ids are the reference. (--reference-namespace uses one
    source's layout instead.)
  * Plan for readers: anything reading the table during the swap (seconds)
    fails. A Snowflake ICEBERG TABLE pointing at it STOPS auto-refreshing
    after the swap -- the table UUID changes -- and keeps serving the old
    snapshot from cache: re-create it (CREATE OR REPLACE ICEBERG TABLE). A
    catalog-linked database re-links by itself within minutes.
  * Polaris blocks schema changes to Fivetran-managed tables, which is why
    this drops and re-creates instead of evolving the schema in place.

AFTER
-----
  * Resume the connector. Run a sync and check it succeeds.
  * Run register_consolidation.py: the realigned source should be spliced,
    and its old relabeled copies retired.
  * When you are satisfied, finalize to delete the original files.

Usage:
  python scripts/realign_source_field_ids.py --config config.yaml --table accounts --sources ns1,ns2
  python scripts/realign_source_field_ids.py apply --config config.yaml --table accounts --sources ns1 \\
      --execute --i-understand-this-modifies-source-tables
  python scripts/realign_source_field_ids.py rollback --record realign-records/<file>.json --execute \\
      --i-understand-this-modifies-source-tables
"""
import argparse
import itertools
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from pyiceberg.io.pyarrow import _to_requested_schema, pyarrow_to_schema
from pyiceberg.manifest import DataFile, DataFileContent
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import NestedField, PrimitiveType

import parquet_footer
import register_consolidation as rc

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("realign_source_field_ids")

ACK_FLAG = "--i-understand-this-modifies-source-tables"
SAMPLE_FILES = 3


class Refused(Exception):
    """A precondition failed; nothing was changed."""


# ---------------------------------------------------------------------------
# Planning (read-only)
# ---------------------------------------------------------------------------


def reference_schema(cat, cfg: dict, table: str, reference_namespace: str = None) -> Schema:
    ident = f"{reference_namespace}.{table}" if reference_namespace else f"{cfg['target_namespace']}.{table}"
    if not cat.table_exists(ident):
        raise Refused(
            f"reference table {ident} does not exist -- run register_consolidation.py first, "
            "or pass --reference-namespace"
        )
    return cat.load_table(ident).schema()


def realigned_schema(src: Schema, ref: Schema) -> tuple:
    """(new schema, {old id: new id}). Each source column keeps its name,
    type, required-ness and doc, and takes the reference's id for that name."""
    ref_ids = {f.name.lower(): f.field_id for f in ref.fields}
    missing = [f.name for f in src.fields if f.name.lower() not in ref_ids]
    if missing:
        raise Refused(
            f"source columns not in the reference schema: {missing}. Run register_consolidation.py "
            "first so the target knows every column."
        )
    nested = [f.name for f in src.fields if not isinstance(f.field_type, PrimitiveType)]
    if nested:
        raise Refused(f"nested columns are not supported: {nested}")
    id_map = {f.field_id: ref_ids[f.name.lower()] for f in src.fields}
    fields = sorted(
        (NestedField(id_map[f.field_id], f.name, f.field_type, required=f.required, doc=f.doc) for f in src.fields),
        key=lambda f: f.field_id,
    )
    identifiers = [id_map[i] for i in src.identifier_field_ids]
    return Schema(*fields, identifier_field_ids=identifiers), id_map


def plan_one(cat, cfg, ns: str, table: str, ref: Schema) -> dict:
    t = cat.load_table(f"{ns}.{table}")
    if not t.spec().is_unpartitioned():
        raise Refused(f"{ns}.{table} is partitioned; not supported")
    if t.sort_order().fields:
        raise Refused(f"{ns}.{table} has a sort order; not supported")
    new_schema, id_map = realigned_schema(t.schema(), ref)
    snap = t.current_snapshot()
    tasks = list(t.scan().plan_files()) if snap else []
    files = [task.file for task in tasks]
    if any(task.delete_files for task in tasks):
        raise Refused(f"{ns}.{table} has delete files (merge-on-read); not supported")
    return {
        "table": t, "namespace": ns, "new_schema": new_schema, "id_map": id_map,
        "aligned": all(k == v for k, v in id_map.items()), "files": files,
        "snapshot_id": snap.snapshot_id if snap else None,
    }


def describe(p: dict) -> str:
    moved = {k: v for k, v in p["id_map"].items() if k != v}
    if p["aligned"]:
        return f"{p['namespace']}: already aligned with the reference, nothing to do"
    names = {f.field_id: f.name for f in p["table"].schema().fields}
    lines = [f"{p['namespace']}: {len(moved)} column(s) change field id; "
             f"{len(p['files'])} data file(s), {sum(f.record_count for f in p['files'])} rows to copy"]
    lines += [f"    {names[o]:<32} {o:>4} -> {n}" for o, n in sorted(moved.items())]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


def _copy_files(p: dict, cfg: dict) -> list:
    """Relabeled copies of the current files in the same table location.
    Returns [(original DataFile, new DataFile)]."""
    t, id_map = p["table"], p["id_map"]
    workers = cfg.get("relabel_workers", rc.DEFAULT_RELABEL_WORKERS)
    s3 = rc._server_side_s3_client(workers)
    location = t.metadata.location.rstrip("/")

    def remap(d):
        return None if d is None else {id_map[k]: v for k, v in d.items() if k in id_map}

    def one(df):
        dst = f"{location}/data/realigned-{uuid.uuid4()}.parquet"
        if s3 is not None and df.file_path.startswith("s3://"):
            size = parquet_footer.relabel_s3_object(s3, df.file_path, dst, id_map)
        else:
            size = parquet_footer.relabel_via_fileio(t.io, t.io, df.file_path, dst, id_map)
        new = DataFile.from_args(
            content=DataFileContent.DATA, file_path=dst, file_format=df.file_format, partition=Record(),
            record_count=df.record_count, file_size_in_bytes=size,
            column_sizes=remap(df.column_sizes), value_counts=remap(df.value_counts),
            null_value_counts=remap(df.null_value_counts), nan_value_counts=remap(df.nan_value_counts),
            lower_bounds=remap(df.lower_bounds), upper_bounds=remap(df.upper_bounds),
            split_offsets=df.split_offsets,
        )
        new.spec_id = df.spec_id
        return df, new

    return list(rc._relabel_pool(workers).map(one, p["files"]))


def _create_with_ids(cat, ident: str, schema: Schema, location: str, props: dict, files: list):
    """Create the table so its field ids come out EXACTLY as in `schema`.

    Catalogs (Polaris, Glue) and pyiceberg itself renumber ids 1..n in
    declaration order on create. So: declare the leading run of ids 1..k at
    create (renumbering then reproduces them), and add any ids after a gap by
    schema evolution with an explicit next id -- the same technique
    register_consolidation.py uses. When there is no gap, table and files are
    created in ONE atomic commit; otherwise the table briefly exists empty."""
    fields = sorted(schema.fields, key=lambda f: f.field_id)
    if not fields or fields[0].field_id != 1:
        raise Refused("the realigned schema has no field id 1; cannot be reproduced on create")
    k = 0
    while k < len(fields) and fields[k].field_id == k + 1:
        k += 1
    head, tail = fields[:k], fields[k:]
    if any(f.required for f in tail):
        raise Refused(f"required column(s) {[f.name for f in tail if f.required]} sit after an id gap; not supported")
    head_ids = [f.field_id for f in head if f.field_id in schema.identifier_field_ids]
    base = Schema(*head, identifier_field_ids=head_ids)

    if not tail:
        try:
            txn = cat.create_table_transaction(ident, schema=base, location=location, properties=props)
            if files:
                with txn.update_snapshot().fast_append() as fa:
                    for df in files:
                        fa.append_data_file(df)
            txn.commit_transaction()
            return cat.load_table(ident)
        except Exception as e:  # noqa: BLE001 -- fall back to the two-step path below
            logger.warning("atomic create not available (%s: %s); creating in two steps", type(e).__name__, str(e)[:200])
            if cat.table_exists(ident):
                cat.drop_table(ident)

    t = cat.create_table(ident, schema=base, location=location, properties=props)
    for f in tail:
        with t.update_schema() as us:
            us._last_column_id = itertools.count(f.field_id)
            us.add_column(f.name, f.field_type, doc=f.doc)
        t = cat.load_table(ident)
    if files:
        with t.transaction() as tx:
            with tx.update_snapshot().fast_append() as fa:
                for df in files:
                    fa.append_data_file(df)
    return cat.load_table(ident)


def _verify(cat, ident: str, p: dict, pairs: list) -> None:
    new = cat.load_table(ident)
    want = {f.name: f.field_id for f in p["new_schema"].fields}
    got = {f.name: f.field_id for f in new.schema().fields}
    if got != want:
        raise RuntimeError(f"field ids did not come out as intended: wanted {want}, got {got}")
    old_rows = sum(df.record_count for df, _ in pairs)
    new_rows = sum(t.file.record_count for t in new.scan().plan_files()) if new.current_snapshot() else 0
    if old_rows != new_rows:
        raise RuntimeError(f"row count changed: {old_rows} -> {new_rows}")
    # Value-for-value on a sample: each original read through the OLD schema
    # must equal its copy read through the NEW one.
    old_schema = p["table"].schema()
    for old_df, new_df in pairs[:SAMPLE_FILES]:
        a = _read(p["table"].io, old_df.file_path, old_schema)
        b = _read(new.io, new_df.file_path, new.schema())
        if a.to_pylist() != b.select(a.column_names).to_pylist():
            raise RuntimeError(f"copy of {old_df.file_path} does not read back identically")


def _read(io, path: str, schema: Schema):
    """Read one data file projected by FIELD ID onto `schema` -- the way any
    Iceberg reader does -- so a wrong id shows up as wrong values."""
    with io.new_input(path).open() as f:
        tbl = pq.read_table(f)
    file_schema = pyarrow_to_schema(tbl.schema)
    batches = [_to_requested_schema(schema, file_schema, b) for b in tbl.to_batches()]
    # No forced Arrow schema: the projected batches can differ from
    # schema.as_arrow() in string width or field metadata, and the caller
    # compares VALUES, not Arrow types.
    return pa.Table.from_batches(batches) if batches else schema.as_arrow().empty_table()


def apply_one(cat, cfg: dict, p: dict, table: str, record_dir: str) -> str:
    ns, t = p["namespace"], p["table"]
    ident = f"{ns}.{table}"
    record = {
        "warning": "realign_source_field_ids.py modified this SOURCE table",
        "table": ident,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "original_metadata_location": t.metadata_location,
        "original_table_uuid": str(t.metadata.table_uuid),
        "original_snapshot_id": p["snapshot_id"],
        "id_map": {str(k): v for k, v in p["id_map"].items()},
        "original_files": [df.file_path for df in p["files"]],
        "status": "started",
    }
    os.makedirs(record_dir, exist_ok=True)
    record_path = os.path.join(record_dir, f"{ns}.{table}.{int(time.time())}.json")
    _write(record_path, record)
    logger.info("[%s] record written (rollback point): %s", ident, record_path)

    pairs = _copy_files(p, cfg)
    record["new_files"] = [new.file_path for _, new in pairs]
    _write(record_path, record)
    logger.info("[%s] %d file(s) copied with the new field ids", ident, len(pairs))

    now = cat.load_table(ident)
    now_sid = now.current_snapshot().snapshot_id if now.current_snapshot() else None
    if now_sid != p["snapshot_id"]:
        for _, new in pairs:
            t.io.delete(new.file_path)
        record["status"] = "aborted: source changed during copy -- is the connector paused?"
        _write(record_path, record)
        raise Refused(f"{ident} committed a new snapshot during the copy; nothing changed. Pause the connector.")

    props = {k: v for k, v in t.properties.items() if k != "created-at"}
    location = t.metadata.location
    cat.drop_table(ident)  # registration only; files stay
    record["status"] = "dropped"
    _write(record_path, record)
    try:
        new = _create_with_ids(cat, ident, p["new_schema"], location, props, [n for _, n in pairs])
        _verify(cat, ident, p, pairs)
    except Exception:
        logger.exception("[%s] re-create or verification failed -- rolling back", ident)
        _restore(cat, ident, record)
        for _, new_df in pairs:  # the copies are referenced by nothing now
            try:
                t.io.delete(new_df.file_path)
            except Exception:  # noqa: BLE001
                logger.warning("could not delete copy %s", new_df.file_path)
        record["status"] = "rolled back automatically after a failure"
        _write(record_path, record)
        raise
    record.update(status="applied", new_table_uuid=str(new.metadata.table_uuid),
                  new_snapshot_id=new.current_snapshot().snapshot_id if new.current_snapshot() else None,
                  applied_at=datetime.now(timezone.utc).isoformat())
    _write(record_path, record)
    logger.info("[%s] REALIGNED. New table uuid %s. Originals kept for rollback.", ident, record["new_table_uuid"])
    return record_path


def _restore(cat, ident: str, record: dict) -> None:
    if cat.table_exists(ident):
        cat.drop_table(ident)
    cat.register_table(ident, record["original_metadata_location"])


def _write(path: str, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Rollback / finalize
# ---------------------------------------------------------------------------


def rollback(cat, record_path: str, force: bool = False) -> None:
    record = json.load(open(record_path))
    ident = record["table"]
    if cat.table_exists(ident):
        cur = cat.load_table(ident)
        sid = cur.current_snapshot().snapshot_id if cur.current_snapshot() else None
        if record.get("new_snapshot_id") is not None and sid != record["new_snapshot_id"] and not force:
            raise Refused(
                f"{ident} has been written since the realign (snapshot {sid}); rolling back would lose "
                "those commits. Pass --force to roll back anyway."
            )
        new_files = record.get("new_files", [])
    else:
        new_files = record.get("new_files", [])
    _restore(cat, ident, record)
    t = cat.load_table(ident)
    for path in new_files:
        try:
            t.io.delete(path)
        except Exception:  # noqa: BLE001
            logger.warning("could not delete copy %s", path)
    record["status"] = "rolled back"
    _write(record_path, record)
    logger.info("[%s] rolled back to %s", ident, record["original_metadata_location"])


def finalize(cat, record_path: str) -> None:
    record = json.load(open(record_path))
    if record.get("status") != "applied":
        raise Refused(f"record status is {record.get('status')!r}, not 'applied'")
    t = cat.load_table(record["table"])
    live = {task.file.file_path for task in t.scan().plan_files()} if t.current_snapshot() else set()
    still_used = [p for p in record["original_files"] if p in live]
    if still_used:
        raise Refused(f"{len(still_used)} original file(s) are still referenced by the table; not deleting")
    for path in record["original_files"]:
        t.io.delete(path)
    record["status"] = "finalized: originals deleted, no rollback possible"
    _write(record_path, record)
    logger.info("[%s] %d original file(s) deleted", record["table"], len(record["original_files"]))


# ---------------------------------------------------------------------------


BANNER = """
==========================================================================
 realign_source_field_ids.py -- EXPERIMENTAL -- MODIFIES SOURCE TABLES
 Drops and re-creates Fivetran-managed source tables. Pause the connector
 first. Try it in dev. Read the docstring at the top of this file.
==========================================================================
"""


def main():
    parser = argparse.ArgumentParser(description="EXPERIMENTAL: realign SOURCE tables' field ids (modifies sources).")
    parser.add_argument("command", nargs="?", default="plan", choices=["plan", "apply", "rollback", "finalize"])
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--table", help="table name (plan/apply)")
    parser.add_argument("--sources", help="comma-separated source namespaces (plan/apply)")
    parser.add_argument("--reference-namespace", help="use this source's layout instead of the target's")
    parser.add_argument("--record", help="record file (rollback/finalize)")
    parser.add_argument("--record-dir", default="realign-records")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(ACK_FLAG, dest="ack", action="store_true")
    parser.add_argument("--force", action="store_true", help="rollback even if the table was written since")
    args = parser.parse_args()
    print(BANNER, file=sys.stderr)

    load_dotenv()
    cat = rc.get_catalog()
    writes = args.command in ("apply", "rollback", "finalize")
    if writes and not (args.execute and args.ack):
        raise SystemExit(f"'{args.command}' changes source tables: pass --execute and {ACK_FLAG}")

    try:
        if args.command in ("rollback", "finalize"):
            if not args.record:
                raise SystemExit("--record is required")
            (rollback(cat, args.record, args.force) if args.command == "rollback" else finalize(cat, args.record))
            return
        cfg = rc.load_config(args.config)
        if not (args.table and args.sources):
            raise SystemExit("--table and --sources are required")
        ref = reference_schema(cat, cfg, args.table, args.reference_namespace)
        plans = [plan_one(cat, cfg, ns.strip(), args.table, ref) for ns in args.sources.split(",") if ns.strip()]
        for p in plans:
            print(describe(p))
        if args.command == "plan":
            print("\nplan only -- nothing changed. Use 'apply --execute " + ACK_FLAG + "' to realign.")
            return
        for p in plans:
            if not p["aligned"]:
                apply_one(cat, cfg, p, args.table, args.record_dir)
    except Refused as e:
        raise SystemExit(f"REFUSED: {e}")


if __name__ == "__main__":
    main()
