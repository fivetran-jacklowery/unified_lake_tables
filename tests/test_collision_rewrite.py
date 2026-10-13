#!/usr/bin/env python3
"""Regression tests for the field-id collision/rewrite path.

Runs entirely against a local SQLite catalog with file:// warehouse -- no
Polaris, no Glue, no AWS, no network. Run it directly or under pytest:

    python tests/test_collision_rewrite.py
    pytest tests/test_collision_rewrite.py

Needs `sqlalchemy` on top of requirements.txt (pyiceberg's SqlCatalog).

WHY THESE EXIST
---------------
A customer reported on 2026-10-13 that rows whose drifted column was NULL
never reached the target: 80 rows missing across six tenants, exactly equal
to the 80 NULLs in those tenants' drifted columns, while the run reported
success. The three tenants with no NULLs lost nothing.

The rewrite read the source with a `{name} is not null` row filter, and for
base-schema divergence that append is the ONLY path those rows have into the
target -- every one of the source's files is excluded from splicing. So any
row with a NULL there was filtered straight back out. The idempotency guard
then compared a NON-NULL row count against a TOTAL record count, so with any
NULLs present it could never fire and every rerun appended again.

The existing two-source validation used columns that were non-null in every
row, which hides both failures completely -- it is exactly the case where the
buggy code looks correct. Hence these two tests, which differ from it only in
having NULLs where it matters.

The NULLs must sit in the column the COLLISION NAMES for the losing source,
not merely in some nullable column. A first draft of this test put them
elsewhere and passed against the known-broken code.

test_base_schema_divergence   -- every file drifts (the customer's shape)
test_ordinary_drift           -- only SOME files drift

The second one is not redundant. The obvious fix for the first -- drop the
filter, read the whole source -- is correct when every file drifts and WRONG
here, because step 3 splices the non-drifted files in by reference and
re-reading the whole table appends those rows a second time. That trades row
loss for row duplication, and Tamarac-shaped data would never reveal it.
"""
import os
import shutil
import sys
import tempfile

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))


def _consolidate(cat, table_name, sources, runs=2):
    """Run registration `runs` times against an injected catalog, returning
    (summary, target_table) after the final run. Running twice is the point:
    the second run is where duplication shows up."""
    import register_consolidation as rc

    rc.get_catalog = lambda: cat
    rc.thread_catalog = lambda: cat
    cfg = {
        "source_namespaces": list(sources),
        "target_namespace": "consolidated",
        "source_id_column": "source_connection_id",
        "table_workers": 1,
        "source_workers": 1,
    }
    results = []
    for _ in range(runs):
        results.append(rc.run(cfg, tables=[table_name]))
    return results, cat.load_table(f"consolidated.{table_name}")


def _counts(tgt, sources):
    got = tgt.scan().to_arrow()
    per_src = {
        ns: tgt.scan(row_filter=f"source_connection_id = '{ns}'").to_arrow().num_rows
        for ns in sources
    }
    return got, per_src


def test_base_schema_divergence():
    """Same column, different field id per source, NULLs in the drifted one.

    This is the customer's shape: identical names and types, divergent ids,
    so 100% of the losing source's files take the rewrite path.
    """
    work = tempfile.mkdtemp(prefix="ult_base_")
    try:
        cat = SqlCatalog("t", uri=f"sqlite:///{work}/c.db", warehouse=f"file://{work}")
        # src_a: flag=1, label=2.  src_b: SWAPPED -> label=1, flag=2.
        schemas = {
            "src_a": Schema(
                NestedField(1, "flag", StringType(), required=False),
                NestedField(2, "label", StringType(), required=False),
                NestedField(3, "amount", LongType(), required=False),
            ),
            "src_b": Schema(
                NestedField(1, "label", StringType(), required=False),
                NestedField(2, "flag", StringType(), required=False),
                NestedField(3, "amount", LongType(), required=False),
            ),
        }
        # The collision fires on field id 1, where src_a says 'flag' and src_b
        # says 'label' -- so 'label' is src_b's drifted column and the NULLs
        # belong THERE. Put them anywhere else and this test passes against
        # the broken code.
        data = {
            "src_a": pa.table({
                "flag": [f"a{i}" for i in range(10)],
                "label": [f"LA{i}" for i in range(10)],
                "amount": list(range(10)),
            }),
            "src_b": pa.table({
                "label": [None if i % 3 == 0 else f"LB{i}" for i in range(10)],  # 4 NULLs
                "flag": [f"b{i}" for i in range(10)],
                "amount": list(range(100, 110)),
            }),
        }
        for ns, schema in schemas.items():
            cat.create_namespace(ns)
            t = cat.create_table(f"{ns}.widgets", schema=schema)
            cols = [f.name for f in schema.fields]
            t.append(data[ns].select(cols).cast(schema.as_arrow()))

        expected_nulls = data["src_b"]["label"].null_count
        _, tgt = _consolidate(cat, "widgets", ("src_a", "src_b"))
        got, per_src = _counts(tgt, ("src_a", "src_b"))

        assert got.num_rows == 20, f"expected 20 rows, got {got.num_rows}"
        assert per_src["src_a"] == 10, f"src_a: {per_src['src_a']}"
        assert per_src["src_b"] == 10, f"src_b lost rows: {per_src['src_b']}/10"
        assert got.column("label").null_count == expected_nulls, (
            f"NULLs dropped: {got.column('label').null_count} != {expected_nulls}"
        )
        # Values must land under the right NAMES despite the swapped ids.
        b_rows = [r for r in got.to_pylist() if r["source_connection_id"] == "src_b"]
        assert all(r["label"] is None or r["label"].startswith("LB") for r in b_rows), \
            "field-id crossover: src_b 'label' holds another source's values"
        assert all(r["flag"] and r["flag"].startswith("b") for r in b_rows), \
            "field-id crossover: src_b 'flag' holds another source's values"
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_ordinary_drift():
    """Only SOME files drift: the non-drifted ones are spliced by reference.

    Guards against 'fixing' the NULL bug by reading the whole source table,
    which would append the already-spliced rows a second time.
    """
    work = tempfile.mkdtemp(prefix="ult_partial_")
    try:
        cat = SqlCatalog("t", uri=f"sqlite:///{work}/c.db", warehouse=f"file://{work}")
        base = Schema(
            NestedField(1, "sku", StringType(), required=False),
            NestedField(2, "qty", LongType(), required=False),
        )
        newcol = {"src_a": "alpha", "src_b": "beta"}
        for ns in ("src_a", "src_b"):
            cat.create_namespace(ns)
            t = cat.create_table(f"{ns}.orders", schema=base)
            # File 1: base schema -> no drift, spliced by reference.
            t.append(pa.table({
                "sku": [f"{ns}-{i}" for i in range(5)],
                "qty": list(range(5)),
            }).cast(base.as_arrow()))
            # Each source independently adds its own column at field id 3.
            with t.update_schema() as us:
                us.add_column(newcol[ns], StringType())
            t = cat.load_table(f"{ns}.orders")
            # File 2: carries it -> drifts for the losing source. NULLs here
            # too, so this covers nulls on the ordinary-drift path as well.
            t.append(pa.table({
                "sku": [f"{ns}-{i}" for i in range(5, 10)],
                "qty": list(range(5, 10)),
                newcol[ns]: [None if i % 2 == 0 else f"v{i}" for i in range(5)],
            }).cast(t.schema().as_arrow()))

        _, tgt = _consolidate(cat, "orders", ("src_a", "src_b"))
        got, per_src = _counts(tgt, ("src_a", "src_b"))

        assert got.num_rows == 20, f"expected 20 rows, got {got.num_rows}"
        for ns in ("src_a", "src_b"):
            assert per_src[ns] == 10, f"{ns}: {per_src[ns]}/10"
        skus = [r["sku"] for r in got.to_pylist()]
        dupes = sorted({s for s in skus if skus.count(s) > 1})
        assert not dupes, f"spliced rows duplicated by the rewrite: {dupes}"
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    failed = 0
    for fn in (test_base_schema_divergence, test_ordinary_drift):
        try:
            fn()
            print(f"PASS  {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {fn.__name__}: {e}")
    sys.exit(1 if failed else 0)
