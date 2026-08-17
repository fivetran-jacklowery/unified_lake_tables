#!/usr/bin/env python3
"""verify_consolidation.py -- post-run sanity checks for tables produced by
register_consolidation.py.

Three checks, each re-derived independently rather than trusting the
registration script's own bookkeeping, so a bug in register_consolidation.py
wouldn't hide itself here:

  1. Row counts. For every configured source namespace, the number of
     materialized rows readable in the target table for that source's
     identity-partition value must equal the number of rows an independent
     scan of the source table itself returns. This is the same class of
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
    python scripts/verify_consolidation.py [--config config.yaml] [tables...]
"""
import argparse
import logging
import os
import sys
from collections import Counter

from dotenv import load_dotenv

# Reuses the exact same catalog/config plumbing as register_consolidation.py
# rather than re-implementing it, so this can never silently drift out of
# sync with how the registration script actually authenticates or resolves
# source tables.
from register_consolidation import (
    catalog_properties,
    discover_common_tables,
    load_config,
)
from pyiceberg.catalog.rest import RestCatalog

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("verify_consolidation")


def get_catalog() -> RestCatalog:
    return RestCatalog("fivetran_mdls_verify", **catalog_properties())


def verify_table(cat, table_name: str, source_namespaces: list, target_namespace: str, source_id_column: str) -> bool:
    target_id = f"{target_namespace}.{table_name}"
    ok = True

    if not cat.table_exists(target_id):
        logger.error("[%s] target table %s does not exist -- run register_consolidation.py first", table_name, target_id)
        return False

    target_table = cat.load_table(target_id)

    # --- Check 1: row counts, per source, independently re-derived ---
    total_target_rows = 0
    sources_checked = 0
    for ns in source_namespaces:
        src_id = f"{ns}.{table_name}"
        if not cat.table_exists(src_id):
            logger.warning("[%s] source table %s does not exist, skipping", table_name, src_id)
            continue
        sources_checked += 1
        src_table = cat.load_table(src_id)
        source_row_count = src_table.scan().to_arrow().num_rows
        target_row_count = target_table.scan(row_filter=f"{source_id_column} = '{ns}'").to_arrow().num_rows
        total_target_rows += target_row_count
        if source_row_count != target_row_count:
            logger.error(
                "[%s] ROW COUNT MISMATCH for source '%s': source has %d row(s), target has %d row(s) for that source",
                table_name,
                ns,
                source_row_count,
                target_row_count,
            )
            ok = False
        else:
            logger.info("[%s] %s: %d row(s), matches source", table_name, ns, target_row_count)

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
    parser.add_argument("tables", nargs="*", help="Optional: restrict to these table names")
    args = parser.parse_args()

    load_dotenv()
    cfg = load_config(args.config)
    cat = get_catalog()
    tables = args.tables or discover_common_tables(cat, cfg["source_namespaces"])
    if not tables:
        logger.error("No tables to verify.")
        sys.exit(1)

    all_ok = True
    for t in tables:
        logger.info("=== verifying %s ===", t)
        if not verify_table(cat, t, cfg["source_namespaces"], cfg["target_namespace"], cfg["source_id_column"]):
            all_ok = False

    if not all_ok:
        logger.error("One or more checks FAILED. See above.")
        sys.exit(1)
    logger.info("All checks passed for %d table(s).", len(tables))


if __name__ == "__main__":
    main()
