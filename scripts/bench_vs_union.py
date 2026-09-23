#!/usr/bin/env python3
"""bench_vs_union.py -- measure what consolidation actually buys you, versus
the `UNION ALL` view it replaces.

This exists because the README used to assert, without measurement, that a
`UNION ALL` view "re-scans every source's full data on every single query."
Measured against a real Polaris catalog and a real query optimizer, that
claim is **false** in the case people care about most, and the actual
advantage of this tool turns out to live somewhere else entirely. See
docs/BENCHMARKS.md for the numbers and the reasoning.

Three things get measured, in increasing order of how much setup they need:

  1. Planning cost (always runs). How long it takes to go from "I want this
     data" to "here is the list of files I must read," for the consolidated
     table versus loading and planning all N source tables. This is pure
     catalog + manifest work -- no data is read -- so it needs nothing
     beyond the same credentials register_consolidation.py already uses.
     This is where consolidation genuinely wins, and the margin grows with
     the number of sources.

  2. Staleness (always runs). Per source, rows currently in the consolidated
     table versus rows live at the source. A non-zero delta means the
     consolidated table is behind and wants a register_consolidation.py run.
     Uses manifest record_count sums, so it is cheap -- and approximate,
     since record_count does not account for position deletes.

  3. Optimizer behavior (--with-duckdb). The decisive question: does a query
     engine, given a UNION ALL whose branches each tag rows with a literal
     source id, constant-fold that literal and eliminate the branches that
     cannot match a filter on it? If yes, the union prunes just as well as
     this tool's identity partition does, and the "avoids re-scanning" pitch
     collapses. Materializes each source locally and asks DuckDB directly.
     Requires `pip install duckdb`.

Usage:
    python scripts/bench_vs_union.py [--config config.yaml] [--table shipments]
    python scripts/bench_vs_union.py --with-duckdb   # adds the optimizer test
"""
import argparse
import logging
import os
import shutil
import sys
import tempfile
import time

from dotenv import load_dotenv

# Same plumbing as verify_consolidation.py -- never re-implement auth or
# namespace resolution here, or this can silently drift from what the
# registration script actually does.
from register_consolidation import (
    catalog_properties,
    discover_common_tables,
    load_config,
    resolve_source_namespaces,
)
from pyiceberg.catalog.rest import RestCatalog

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("bench_vs_union")


def _summarize(tasks):
    """(file count, bytes, rows) for a scan's planned files."""
    files = list(tasks)
    return (
        len(files),
        sum(t.file.file_size_in_bytes for t in files),
        sum(t.file.record_count for t in files),
    )


def _timed(fn):
    t0 = time.perf_counter()
    result = fn()
    return result, (time.perf_counter() - t0) * 1000


def bench_planning(cat, table_name, source_namespaces, target_namespace, source_id_column):
    """Planning cost + bytes/rows touched, consolidated vs union, filtered and not."""
    cons = cat.load_table(f"{target_namespace}.{table_name}")
    pick = source_namespaces[0]

    (c_all, t_c_all) = _timed(lambda: _summarize(cons.scan().plan_files()))
    (c_one, t_c_one) = _timed(
        lambda: _summarize(cons.scan(row_filter=f"{source_id_column} = '{pick}'").plan_files())
    )

    def union_all():
        f = b = r = 0
        for ns in source_namespaces:
            t = cat.load_table(f"{ns}.{table_name}")
            df, db, dr = _summarize(t.scan().plan_files())
            f, b, r = f + df, b + db, r + dr
        return f, b, r

    (u_all, t_u_all) = _timed(union_all)
    # Best case for the union: the optimizer folds the literal and only one
    # branch survives, so only that one source table is loaded and planned.
    (u_one, t_u_one) = _timed(
        lambda: _summarize(cat.load_table(f"{pick}.{table_name}").scan().plan_files())
    )

    print(f"\n=== planning cost: {table_name} ({len(source_namespaces)} sources) ===")
    print(f"single-source filter target: {pick}\n")
    print(f"{'approach':<38} {'files':>7} {'MB':>9} {'rows':>12} {'plan ms':>9}")
    print("-" * 79)
    for label, (f, b, r), ms in [
        ("consolidated, no filter", c_all, t_c_all),
        ("consolidated, filter 1 source", c_one, t_c_one),
        ("UNION ALL, no filter", u_all, t_u_all),
        ("UNION ALL, filter (branch-pruned)", u_one, t_u_one),
    ]:
        print(f"{label:<38} {f:>7} {b / 1e6:>9.1f} {r:>12,} {ms:>9.0f}")

    if c_all[0]:
        print(
            f"\npruning, consolidated: {c_one[0]}/{c_all[0]} files "
            f"({100 * c_one[0] / c_all[0]:.1f}%), {100 * c_one[2] / c_all[2]:.1f}% of rows"
        )
    print(
        f"planning, 1 table vs {len(source_namespaces)}: "
        f"{t_c_all:.0f} ms vs {t_u_all:.0f} ms ({t_u_all / max(t_c_all, 1e-9):.1f}x)"
    )
    print(
        "\nNote: consolidated and union plan nearly identical file sets -- by design,\n"
        "they are the SAME physical Parquet files. Data-scan cost cannot differ.\n"
        "Any gap in the file counts above is staleness, not layout (see below)."
    )


def bench_staleness(cat, table_name, source_namespaces, target_namespace):
    """Per-source drift between the consolidated table and its live sources."""
    cons = cat.load_table(f"{target_namespace}.{table_name}")
    by_src = {}
    for t in cons.scan().plan_files():
        ns = t.file.partition[0]
        f, r = by_src.get(ns, (0, 0))
        by_src[ns] = (f + 1, r + t.file.record_count)

    print(f"\n=== staleness: {table_name} ===")
    print(f"{'source':<28} {'cons rows':>11} {'src rows':>11} {'delta':>10}")
    print("-" * 62)
    total = 0
    for ns in source_namespaces:
        _, cr = by_src.get(ns, (0, 0))
        sr = sum(t.file.record_count for t in cat.load_table(f"{ns}.{table_name}").scan().plan_files())
        d = sr - cr
        total += d
        print(f"{ns:<28} {cr:>11,} {sr:>11,} {d:>+10,}{'   STALE' if d else ''}")
    print("-" * 62)
    print(f"{'TOTAL':<28} {'':>11} {'':>11} {total:>+10,}")
    if total:
        print("\nConsolidated table is behind its sources. Re-run register_consolidation.py.")


def bench_optimizer(cat, table_name, source_namespaces, source_id_column):
    """Does a real optimizer prune UNION ALL branches on a literal source id?"""
    try:
        import duckdb
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        logger.error("--with-duckdb needs duckdb and pyarrow: pip install duckdb pyarrow")
        return

    workdir = tempfile.mkdtemp(prefix="bench_vs_union_")
    try:
        logger.info("materializing %d sources to %s", len(source_namespaces), workdir)
        for ns in source_namespaces:
            tab = cat.load_table(f"{ns}.{table_name}").scan().to_arrow()
            pq.write_table(tab, os.path.join(workdir, f"{ns}.parquet"))
            d = os.path.join(workdir, "consolidated", f"{source_id_column}={ns}")
            os.makedirs(d, exist_ok=True)
            pq.write_table(
                tab.append_column(source_id_column, pa.array([ns] * tab.num_rows, type=pa.string())),
                os.path.join(d, "data.parquet"),
            )

        con = duckdb.connect()
        union_sql = "\nUNION ALL\n".join(
            f"SELECT *, '{ns}' AS {source_id_column} FROM "
            f"read_parquet('{os.path.join(workdir, ns)}.parquet')"
            for ns in source_namespaces
        )
        con.execute(f"CREATE VIEW v_union AS {union_sql}")
        con.execute(
            "CREATE VIEW v_consolidated AS SELECT * FROM read_parquet("
            f"'{os.path.join(workdir, 'consolidated')}/**/*.parquet', hive_partitioning=true)"
        )

        def best_ms(sql, runs=5):
            con.execute(sql)  # warm caches; discard
            times = []
            for _ in range(runs):
                t0 = time.perf_counter()
                con.execute(sql).fetchall()
                times.append((time.perf_counter() - t0) * 1000)
            return min(times)

        pick = source_namespaces[0]
        print(f"\n=== optimizer: does UNION ALL prune on {source_id_column}? ===")
        # The contrast case is deliberately "no filter" rather than a filter on
        # some other column: it makes the same point (the union must touch every
        # source) without hardcoding a column name that only exists on one table.
        for label, where in [
            (f"filter ON {source_id_column}", f" WHERE {source_id_column} = '{pick}'"),
            ("no filter", ""),
        ]:
            print(f"\n  {label}:")
            for view in ("v_union", "v_consolidated"):
                sql = f"SELECT count(*) FROM {view}{where}"
                ms = best_ms(sql)
                plan = con.execute(f"EXPLAIN ANALYZE {sql}").fetchall()[0][1]
                touched = len({n for n in source_namespaces if n in plan})
                # v_consolidated is a single hive glob scan, which does not name
                # individual files in the plan -- it always reports 0, and that is
                # an artifact of plan formatting, not evidence about pruning.
                shown = f"{touched}/{len(source_namespaces)}" if view == "v_union" else "n/a (glob scan)"
                print(f"    {view:<16} {ms:>7.1f} ms   sources in plan: {shown}")
        print(
            "\n  If the union touches 1/N on a source-id filter, its optimizer folds the\n"
            "  literal and eliminates branches -- it prunes as well as the identity\n"
            "  partition does, and consolidation's advantage is planning cost alone."
        )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser(description="Benchmark consolidation against a UNION ALL view.")
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--table", help="Table to benchmark (default: first discovered common table)")
    ap.add_argument("--with-duckdb", action="store_true", help="Also run the optimizer test")
    args = ap.parse_args()

    load_dotenv()
    cfg = load_config(args.config)
    cat = RestCatalog("fivetran_mdls_bench", **catalog_properties())
    sources = resolve_source_namespaces(cat, cfg)

    table = args.table
    if not table:
        common = discover_common_tables(cat, sources)
        if not common:
            logger.error("No common tables found across sources.")
            sys.exit(1)
        table = sorted(common)[0]
        logger.info("no --table given, benchmarking '%s'", table)

    bench_planning(cat, table, sources, cfg["target_namespace"], cfg["source_id_column"])
    bench_staleness(cat, table, sources, cfg["target_namespace"])
    if args.with_duckdb:
        bench_optimizer(cat, table, sources, cfg["source_id_column"])


if __name__ == "__main__":
    main()
