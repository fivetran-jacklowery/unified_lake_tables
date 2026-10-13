#!/usr/bin/env python3
"""diagnose_sources.py -- READ-ONLY survey of your source tables.

Creates nothing, writes nothing, commits nothing. Safe against production.

    python scripts/diagnose_sources.py [--config config.yaml] [tables...]
    CATALOG_BACKEND=glue python scripts/diagnose_sources.py        # or via the
                                                                   # Glue adapter

WHY RUN THIS BEFORE CONSOLIDATING ANYTHING
-------------------------------------------
register_consolidation.py splices Parquet files by reference, which is only
safe when the sources agree about what their metadata means. Three of its
assumptions are properties of whoever WROTE your tables, not of this tool,
and a synthetic test cannot check them -- a test that creates its sources
with pyiceberg and reads them with pyiceberg will always agree with itself.

  1. PER-FILE STATISTICS. Drift detection opens with
         if not f.null_value_counts: return None
     so a writer that omits null_value_counts makes every file look
     drift-free. Divergent files would then be spliced in by reference and
     the target would silently return the wrong column's values -- the exact
     corruption the collision logic exists to prevent, bypassed entirely.

  2. NO DELETE FILES. The splice copies DataFileContent.DATA and never looks
     at a scan task's delete_files. A merge-on-read source would have its
     data files spliced and its deletes dropped, so rows deleted at the
     source come back to life in the target.

     For Fivetran-managed sources this is expected to hold, and it does not
     depend on which catalog the tables are registered in -- registering a
     table in a catalog only records where its metadata lives; it does not
     change how the data was written. See docs/HOW_IT_WORKS.md.

     The check runs anyway: it costs nothing, it covers sources this tool
     was never aimed at, and it would catch that expectation changing.

  3. FIELD-ID AGREEMENT ACROSS SOURCES. Where sources disagree, each
     divergent source is rewritten in full rather than spliced -- correct,
     but it forfeits the no-rewrite premise that is the point of the tool.
     Better to know the fraction up front than to discover it from a bill.

(3) also answers the question worth settling before any large rollout: of N
tables, how many actually have agreeing ids and can be consolidated cheaply?
"""
import argparse
import logging
import os
import sys
from collections import Counter

from dotenv import load_dotenv

from register_consolidation import (
    catalog_backend,
    discover_common_tables,
    find_field_id_disagreements,
    get_catalog,
    load_config,
    resolve_source_namespaces,
)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(message)s")
logger = logging.getLogger("diagnose_sources")


def survey_table(cat, table_name, source_namespaces):
    """Returns a dict of findings for one table name across every source."""
    out = {"table": table_name, "sources": {}, "disagreements": [], "errors": []}
    schemas = {}
    for ns in source_namespaces:
        try:
            t = cat.load_table(f"{ns}.{table_name}")
        except Exception as e:  # noqa: BLE001
            out["errors"].append(f"{ns}: cannot load ({type(e).__name__}: {e})")
            continue
        schemas[ns] = t.schema()

        files = no_stats = no_bounds = delete_files = rows = 0
        fmt = t.metadata.format_version
        try:
            for task in t.scan().plan_files():
                files += 1
                f = task.file
                rows += f.record_count
                if not f.null_value_counts:
                    no_stats += 1
                if not f.lower_bounds:
                    no_bounds += 1
                # The property the splice silently depends on. Present on
                # FileScanTask in pyiceberg; guarded in case that changes.
                delete_files += len(getattr(task, "delete_files", ()) or ())
        except Exception as e:  # noqa: BLE001
            out["errors"].append(f"{ns}.{table_name}: scan planning failed ({e})")

        out["sources"][ns] = {
            "format_version": fmt, "files": files, "rows": rows,
            "files_without_null_counts": no_stats,
            "files_without_bounds": no_bounds,
            "delete_files": delete_files,
            "columns": len(t.schema().fields),
            "field_ids": {f.name: f.field_id for f in t.schema().fields},
        }

    if len(schemas) > 1:
        out["disagreements"] = find_field_id_disagreements(schemas)

    # How many DISTINCT field-id layouts are there across these sources?
    #
    # This is the number that decides how much of the no-rewrite premise is
    # salvageable when sources disagree. Iceberg resolves a file's columns by
    # field id against the table's single schema, and a manifest entry has
    # nowhere to record "this file's id 14 is the table's id 13" -- it carries
    # file_path, partition, counts and bounds, all keyed by field id, and no
    # per-file schema. So one target table can only splice sources that
    # already agree.
    #
    # But sources that agree with EACH OTHER can share a target even if they
    # disagree with the rest. Group the sources by id layout, give each group
    # its own consolidated table, and every group splices at zero rewrite
    # cost. You then union G tables instead of N sources, where G is the count
    # below -- all of the correctness, none of the rewriting, and the planning
    # cost falls by N/G.
    #
    # G == 1 means the whole table consolidates for free. G == N means every
    # source has its own layout and grouping buys nothing.
    layouts = {}
    for ns, schema in schemas.items():
        sig = tuple(sorted((f.name, f.field_id) for f in schema.fields))
        layouts.setdefault(sig, []).append(ns)
    out["layout_groups"] = sorted(layouts.values(), key=lambda g: (-len(g), g[0]))
    return out


def main():
    ap = argparse.ArgumentParser(description="Read-only survey of source tables. Writes nothing.")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--limit", type=int, default=0, help="survey at most N tables (0 = all)")
    ap.add_argument("--verbose", action="store_true", help="print per-table detail")
    ap.add_argument("tables", nargs="*")
    args = ap.parse_args()

    load_dotenv()
    cfg = load_config(args.config)
    cat = get_catalog()
    sources = resolve_source_namespaces(cat, cfg)
    tables = args.tables or discover_common_tables(cat, sources)
    if args.limit:
        tables = tables[: args.limit]

    print(f"\nbackend={catalog_backend()}  sources={len(sources)}  tables={len(tables)}\n")

    tot_files = tot_rows = tot_no_stats = tot_deletes = 0
    clean_tables, divergent_tables, failed_tables = [], [], []
    group_hist = Counter()
    groups_total = 0

    for i, tname in enumerate(tables, 1):
        r = survey_table(cat, tname, sources)
        if r["errors"]:
            failed_tables.append((tname, r["errors"]))
        for ns, s in r["sources"].items():
            tot_files += s["files"]
            tot_rows += s["rows"]
            tot_no_stats += s["files_without_null_counts"]
            tot_deletes += s["delete_files"]
        n_dis = len(r["disagreements"])
        n_groups = len(r.get("layout_groups") or [[]])
        group_hist[n_groups] += 1
        groups_total += n_groups
        (clean_tables if n_dis == 0 else divergent_tables).append((tname, n_dis))
        if args.verbose or n_dis:
            print(f"[{i}/{len(tables)}] {tname}: {n_dis} contested field id(s)")
            for fid, entries in r["disagreements"][:5]:
                print(f"    id {fid}: " + "; ".join(f"{ns}={n}({t})" for ns, n, t in entries))
            if n_dis > 5:
                print(f"    ... and {n_dis - 5} more")
        for tname_, errs in ([(tname, r["errors"])] if r["errors"] else []):
            for e in errs:
                print(f"    ERROR {e}")

    print("\n" + "=" * 70)
    print("WRITER ASSUMPTIONS (these decide whether splicing is safe at all)")
    print("=" * 70)
    ok_stats = tot_no_stats == 0
    ok_deletes = tot_deletes == 0
    print(f"  per-file null_value_counts present : {'YES' if ok_stats else 'NO'}"
          f"   ({tot_no_stats}/{tot_files} files missing them)")
    if not ok_stats:
        print("    ^^ DO NOT CONSOLIDATE. Drift detection returns None for a file with no")
        print("       null_value_counts, so divergent files get spliced in by reference and")
        print("       the target silently returns the wrong column's values.")
    print(f"  delete files (merge-on-read)       : {'NONE' if ok_deletes else tot_deletes}")
    if not ok_deletes:
        print("    ^^ DO NOT CONSOLIDATE. The splice copies data files and ignores deletes,")
        print("       so rows deleted at the source would come back to life in the target.")
        print("       Unexpected for a Fivetran-written source -- that writer is")
        print("       copy-on-write only, whichever catalog it registers into. Worth")
        print("       reporting before going further; see docs/HOW_IT_WORKS.md.")

    print("\n" + "=" * 70)
    print("FIELD-ID AGREEMENT (this decides whether consolidation is CHEAP)")
    print("=" * 70)
    n = len(tables)
    print(f"  tables whose sources fully agree   : {len(clean_tables)}/{n}"
          f" ({100*len(clean_tables)/n:.0f}%)  <- spliceable, no rewrite")
    print(f"  tables with any disagreement       : {len(divergent_tables)}/{n}"
          f" ({100*len(divergent_tables)/n:.0f}%)  <- divergent sources rewritten in full")
    if divergent_tables:
        worst = sorted(divergent_tables, key=lambda x: -x[1])[:5]
        print("  worst: " + ", ".join(f"{t} ({c} ids)" for t, c in worst))
    print("\n" + "=" * 70)
    print("DISTINCT FIELD-ID LAYOUTS (this decides how much is salvageable)")
    print("=" * 70)
    print("  A target table can only splice sources that already agree on field ids --")
    print("  Iceberg resolves by id and a manifest entry cannot record a per-file id")
    print("  mapping. But sources that agree with EACH OTHER can share a target. Group")
    print("  by layout, consolidate within each group at ZERO rewrite cost, and union")
    print("  the groups: G tables instead of N sources, no data rewritten.\n")
    n_src = len(sources)
    for g in sorted(group_hist):
        verdict = ("1 target, nothing rewritten" if g == 1
                   else "grouping buys nothing" if g >= n_src
                   else f"{g} targets, nothing rewritten")
        print(f"  {group_hist[g]:5} table(s) need {g} target(s)  -- {verdict}")
    if tables:
        avg = groups_total / len(tables)
        print(f"\n  average union width: {n_src} sources -> {avg:.1f} grouped targets"
              f"  ({'no improvement' if avg >= n_src else f'{n_src/avg:.1f}x fewer branches'})")
        print("  (vs. consolidating all sources into one target, which would rewrite")
        print("   every divergent source in full)")

    print(f"\n  total: {tot_files} files, {tot_rows:,} rows across {len(sources)} sources")
    if failed_tables:
        print(f"\n  {len(failed_tables)} table(s) could not be surveyed -- see ERROR lines above")

    print("\nNothing was created, written or committed by this script.")
    return 0 if (ok_stats and ok_deletes) else 2


if __name__ == "__main__":
    sys.exit(main())
