#!/usr/bin/env python3
"""verify_consolidation.py -- post-run sanity checks for tables produced by
register_consolidation.py.

Three checks, each re-derived independently rather than trusting the
registration script's own bookkeeping, so a bug in register_consolidation.py
wouldn't hide itself here:

  1. Row counts. For every configured source namespace, the rows readable
     in the target for that source's identity-partition value must equal
     (a) the row counts the target's own manifest records for it -- else the
     data is unreadable or mis-registered -- and (b) the source's row count,
     judged at the source snapshot the target recorded at registration: if
     the source has moved on since, the difference is reported as LAG, not
     as a failure. Counting streams one column batch by batch, so memory
     stays flat however large the table is. This is the same class of
     check that caught a real bug during internal validation: an early,
     non-idempotent version of this technique silently doubled every row on
     a second run, and it was caught by exactly this kind of count
     comparison.
  2. Duplicate files. No single physical data file should be registered
     into the target table's manifest more than once. A duplicated
     file_path is the manifest-level symptom of the same bug #1 checks for
     at the row level -- checking both is cheap and catches the bug two
     independent ways.
  3. Partition pruning sanity check. Scanning the target table filtered to
     one source's identity-partition value should plan strictly fewer files
     than scanning the whole table, whenever more than one source has data.
     This is a rough, engine-independent signal that pruning is wired up
     correctly (an identity partition on source_id_column actually exists
     and is being used), without needing access to Snowflake or DuckDB
     EXPLAIN output the way the original internal validation did.

None of these checks are a substitute for the deeper validation described in
docs/HOW_IT_WORKS.md and the README's "What this does NOT yet handle"
section -- they check that a run of register_consolidation.py did what it
was supposed to do, not that your source data itself is drift-free in ways
this tool was never designed to detect (renames, type changes, deletes).

Usage:
    python scripts/verify_consolidation.py [--config config.yaml] [--workers 4] [tables...]
"""
import argparse
import logging
import os
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv

# Reuses the exact same catalog/config plumbing as register_consolidation.py
# rather than re-implementing it, so this can never silently drift out of
# sync with how the registration script actually authenticates or resolves
# source tables.
from register_consolidation import (
    catalog_backend,
    discover_tables,
    get_catalog,
    load_config,
    recorded_source_snapshots,
    resolve_source_namespaces,
    thread_catalog,
)
from pyiceberg.catalog import Catalog  # noqa: F401  (re-exported for callers' type hints)

from multi_source_io import MultiSourceFileIO

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("verify_consolidation")

# get_catalog is imported from register_consolidation rather than rebuilt
# here. It used to be a local two-line copy that hard-coded
# RestCatalog(**catalog_properties()) -- which meant that the moment the
# registration script gained a Glue backend, this verifier would have gone
# on building a Polaris catalog no matter what CATALOG_BACKEND said, and a
# Glue user's verification run would have failed (or worse, silently checked
# the wrong catalog). That is the same copy-drifts-from-original failure
# that put a stale fork of the whole registration script in
# examples/aws-glue/ and corrupted a customer's data; see CHANGELOG.md.
# Don't reintroduce a local catalog constructor here.


COUNT_COLUMN_PREFERENCE = ("_fivetran_synced",)


def _count_column(src_schema, target_schema, source_id_column: str) -> str:
    """One real column present in both the source and the target: reading it
    forces every data file open (so an unreadable file still fails), while
    holding only one column of one batch in memory at a time."""
    target_names = {f.name for f in target_schema.fields} - {source_id_column}
    for name in COUNT_COLUMN_PREFERENCE:
        if name in target_names and src_schema.find_field(name, case_sensitive=False) is not None:
            return name
    for f in src_schema.fields:
        if f.name in target_names:
            return f.name
    raise ValueError("source and target share no column to count by")


def streamed_row_count(table, column: str, row_filter=None, filter_column: str = None) -> int:
    """Row count by streaming ONE column batch by batch. The old check called
    scan().to_arrow().num_rows, materializing every column of the whole table
    -- 7 minutes for one 5.3M-row tenant, and an out-of-memory kill (exit 137)
    on a customer's larger tables.

    filter_column MUST be passed when row_filter references a column other
    than `column`: pyiceberg 0.10 silently returns ZERO rows when a scan
    filters on an identity-partition column that is not in selected_fields.
    It costs nothing here -- the partition value is a constant filled in from
    metadata, not read from the files."""
    if row_filter is None:
        scan = table.scan(selected_fields=(column,))
    else:
        fields = (column,) if filter_column in (None, column) else (column, filter_column)
        scan = table.scan(row_filter=row_filter, selected_fields=fields)
    return sum(batch.num_rows for batch in scan.to_arrow_batch_reader())


def _manifest_rows(tasks) -> tuple:
    """(rows per the manifests, whether any task carries delete files). With
    delete files, record counts overstate live rows and can't be trusted."""
    rows, has_deletes = 0, False
    for t in tasks:
        rows += t.file.record_count
        has_deletes = has_deletes or bool(t.delete_files)
    return rows, has_deletes


def verify_table(cat, table_name: str, source_namespaces: list, target_namespace: str, source_id_column: str) -> bool:
    target_id = f"{target_namespace}.{table_name}"
    ok = True

    if not cat.table_exists(target_id):
        logger.error("[%s] target table %s does not exist -- run register_consolidation.py first", table_name, target_id)
        return False

    target_table = cat.load_table(target_id)
    recorded = recorded_source_snapshots(target_table)

    # --- Check 1: row counts, per source ---
    # Two comparisons, so "behind" and "wrong" are told apart:
    #   a. target vs ITSELF: rows actually readable for a source must equal the
    #      row counts the target's manifest claims for it. A mismatch means
    #      unreadable or mis-registered files -- always an error.
    #   b. target vs SOURCE: only meaningful at the source snapshot the target
    #      recorded at registration. If the source has moved on since, the
    #      difference is expected LAG (re-run registration), not an error. A
    #      target written before snapshots were recorded falls back to
    #      comparing against the source's current state.
    total_target_rows = 0
    sources_checked = 0
    for ns in source_namespaces:
        src_id = f"{ns}.{table_name}"
        if not cat.table_exists(src_id):
            logger.warning("[%s] source table %s does not exist, skipping", table_name, src_id)
            continue
        sources_checked += 1
        src_table = cat.load_table(src_id)
        column = _count_column(src_table.schema(), target_table.schema(), source_id_column)
        row_filter = f"{source_id_column} = '{ns}'"

        # Same cross-namespace credential gap register_consolidation.py's
        # idempotency check has (see multi_source_io.py's docstring): these
        # rows were spliced in by reference and still physically live under
        # NS's own storage location, which the target table's own vended
        # credential doesn't cover.
        original_io = target_table.io
        target_table.io = MultiSourceFileIO([target_table.io, src_table.io])
        try:
            registered_rows, _ = _manifest_rows(target_table.scan(row_filter=row_filter).plan_files())
            target_row_count = streamed_row_count(target_table, column, row_filter, source_id_column)
        finally:
            target_table.io = original_io
        total_target_rows += target_row_count

        if target_row_count != registered_rows:
            logger.error(
                "[%s] UNREADABLE/MIS-REGISTERED DATA for source '%s': target manifest records %d row(s), "
                "but its files hold %d",
                table_name, ns, registered_rows, target_row_count,
            )
            ok = False
            continue

        snap = src_table.current_snapshot()
        current_sid = snap.snapshot_id if snap is not None else None
        recorded_sid = recorded.get(ns)
        src_tasks = list(src_table.scan().plan_files()) if current_sid is not None else []
        source_row_count, has_deletes = _manifest_rows(src_tasks)
        if has_deletes:  # merge-on-read source: count what is actually live
            source_row_count = streamed_row_count(src_table, column)

        if source_row_count == target_row_count:
            logger.info("[%s] %s: %d row(s), matches source", table_name, ns, target_row_count)
        elif recorded_sid is not None and recorded_sid != current_sid:
            logger.warning(
                "[%s] LAG for source '%s': target reflects source snapshot %d (%d row(s)); the source has "
                "moved on to snapshot %s (%d row(s)). Expected between runs -- re-run registration to catch up.",
                table_name, ns, recorded_sid, target_row_count, current_sid, source_row_count,
            )
        else:
            logger.error(
                "[%s] ROW COUNT MISMATCH for source '%s': source has %d row(s), target has %d row(s) for that "
                "source%s",
                table_name, ns, source_row_count, target_row_count,
                "" if recorded_sid is not None else
                " (target predates snapshot tracking, so this may also be lag -- re-run registration, then verify)",
            )
            ok = False

    if sources_checked == 0:
        logger.error("[%s] none of the configured source namespaces have this table -- nothing to verify", table_name)
        return False

    # --- Check 2: duplicate physical files in the target manifest ---
    file_paths = [task.file.file_path for task in target_table.scan().plan_files()]
    dupes = {p: n for p, n in Counter(file_paths).items() if n > 1}
    if dupes:
        logger.error("[%s] DUPLICATE FILE(S) registered in target manifest: %s", table_name, dupes)
        ok = False
    else:
        logger.info("[%s] no duplicate files in target manifest (%d distinct file(s))", table_name, len(file_paths))

    # --- Check 3: basic partition-pruning sanity check ---
    total_files = len(file_paths)
    if len(source_namespaces) > 1 and total_files > 0:
        first_ns = source_namespaces[0]
        pruned_files = len(list(target_table.scan(row_filter=f"{source_id_column} = '{first_ns}'").plan_files()))
        if pruned_files < total_files:
            logger.info(
                "[%s] partition pruning looks correct: filtering to one source plans %d/%d file(s)",
                table_name,
                pruned_files,
                total_files,
            )
        else:
            logger.warning(
                "[%s] partition pruning sanity check inconclusive: filtering to one source still planned "
                "%d/%d file(s). Check that the target table's partition spec has an identity partition "
                "on '%s' (it should, if it was created by register_consolidation.py).",
                table_name,
                pruned_files,
                total_files,
                source_id_column,
            )

    logger.info("[%s] TOTAL target rows across all checked sources: %d", table_name, total_target_rows)
    return ok


def main():
    parser = argparse.ArgumentParser(description="Sanity-check tables produced by register_consolidation.py.")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml (default: ./config.yaml)")
    parser.add_argument("--workers", type=int, default=4, help="Tables verified concurrently (default 4)")
    parser.add_argument("tables", nargs="*", help="Optional: restrict to these table names")
    args = parser.parse_args()

    load_dotenv()
    cfg = load_config(args.config)
    cat = get_catalog()
    # Same resolution register_consolidation.py's run() does -- cfg may have
    # either an explicit source_namespaces list or a source_namespace_pattern
    # glob (see config.example.yaml); this normalizes to a concrete list
    # either way, so everything below can stay unaware of which one was set.
    cfg["source_namespaces"] = resolve_source_namespaces(cat, cfg)
    by_table = discover_tables(cat, cfg["source_namespaces"])
    tables = args.tables or sorted(by_table)
    if not tables:
        logger.error("No tables to verify.")
        sys.exit(1)

    def _one(t):
        logger.info("=== verifying %s ===", t)
        sources = [ns for ns in cfg["source_namespaces"] if ns in by_table.get(t, ())]
        return verify_table(thread_catalog(), t, sources, cfg["target_namespace"], cfg["source_id_column"])

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        all_ok = all(list(ex.map(_one, tables)))

    if not all_ok:
        logger.error("One or more checks FAILED. See above.")
        sys.exit(1)
    logger.info("All checks passed for %d table(s).", len(tables))


if __name__ == "__main__":
    main()
