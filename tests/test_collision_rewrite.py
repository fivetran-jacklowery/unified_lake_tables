#!/usr/bin/env python3
"""Regression tests for the field-id collision/rewrite path.

Runs entirely against a local SQLite catalog with file:// warehouse -- no
Polaris, no Glue, no AWS, no network. Run it directly or under pytest:

    python tests/test_collision_rewrite.py
    pytest tests/test_collision_rewrite.py

Needs `sqlalchemy` on top of requirements.txt (pyiceberg's SqlCatalog).

WHY THESE EXIST
---------------
Rows whose drifted column was NULL never reached the target: a source came
up short by exactly the number of NULLs in its drifted column, while the run
reported success. Sources with no NULLs lost nothing.

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

test_base_schema_divergence   -- every file drifts
test_ordinary_drift           -- only SOME files drift

The second one is not redundant. The obvious fix for the first -- drop the
filter, read the whole source -- is correct when every file drifts and WRONG
here, because step 3 splices the non-drifted files in by reference and
re-reading the whole table appends those rows a second time. That trades row
loss for row duplication, and data where every file drifts would never
reveal it.
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

    Identical names and types, divergent ids, so 100% of the losing source's
    files take the rewrite path.
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


def test_heterogeneous_sources():
    """Three sources that differ in every way at once.

    src_a (baseline): w,x,y,z        ids 1-4
    src_b: all ids shuffled, PLUS a column the baseline does not have
    src_c: ids shuffled, MISSING a column the baseline has

    Covers three things the simpler tests do not:

      * a source carrying a column the baseline lacks. The rewrite appends
        that source's WHOLE column set, so every column must be registered
        first or pyiceberg rejects the append outright --
        "PyArrow table contains more columns: extra" -- and the run dies with
        nothing committed. This is not exotic: discover_common_tables()
        intersects table NAMES, never column names.
      * a source MISSING a baseline column (must come back NULL, not shift).
      * manifest bound consistency, which is the Snowflake symptom. A file
        whose bounds describe a different column than the schema says is
        what makes a warehouse reject the table on read, so
        assert bound widths against the target's declared types directly.
    """
    work = tempfile.mkdtemp(prefix="ult_hetero_")
    try:
        from pyiceberg.types import BooleanType

        cat = SqlCatalog("t", uri=f"sqlite:///{work}/c.db", warehouse=f"file://{work}")
        n = 8
        schemas = {
            "src_a": Schema(
                NestedField(1, "w", StringType(), required=False),
                NestedField(2, "x", BooleanType(), required=False),
                NestedField(3, "y", LongType(), required=False),
                NestedField(4, "z", StringType(), required=False),
            ),
            "src_b": Schema(
                NestedField(1, "z", StringType(), required=False),
                NestedField(2, "y", LongType(), required=False),
                NestedField(3, "x", BooleanType(), required=False),
                NestedField(4, "w", StringType(), required=False),
                NestedField(5, "extra", StringType(), required=False),
            ),
            "src_c": Schema(
                NestedField(1, "y", LongType(), required=False),
                NestedField(2, "w", StringType(), required=False),
                NestedField(3, "x", BooleanType(), required=False),
            ),
        }
        data = {
            "src_a": {"w": [f"aw{i}" for i in range(n)], "x": [i % 2 == 0 for i in range(n)],
                      "y": list(range(n)), "z": [f"az{i}" for i in range(n)]},
            "src_b": {"z": [None if i % 4 == 0 else f"bz{i}" for i in range(n)],
                      "y": [100 + i for i in range(n)], "x": [i % 3 == 0 for i in range(n)],
                      "w": [f"bw{i}" for i in range(n)],
                      "extra": [None if i % 2 else f"bx{i}" for i in range(n)]},
            "src_c": {"y": [200 + i for i in range(n)], "w": [f"cw{i}" for i in range(n)],
                      "x": [None if i % 5 == 0 else (i % 2 == 1) for i in range(n)]},
        }
        for ns, schema in schemas.items():
            cat.create_namespace(ns)
            t = cat.create_table(f"{ns}.t", schema=schema)
            cols = [f.name for f in schema.fields]
            t.append(pa.table({c: data[ns][c] for c in cols}).cast(schema.as_arrow()))

        srcs = tuple(schemas)
        _, tgt = _consolidate(cat, "t", srcs)
        got, per_src = _counts(tgt, srcs)
        rows = got.to_pylist()

        assert got.num_rows == 3 * n, f"expected {3*n} rows, got {got.num_rows}"
        for ns in srcs:
            assert per_src[ns] == n, f"{ns}: {per_src[ns]}/{n}"
        assert "extra" in got.column_names, "src_b's extra column was lost from the target"

        for ns in srcs:
            srows = [r for r in rows if r["source_connection_id"] == ns]
            for col, expected in data[ns].items():
                key = lambda v: (v is None, str(v))
                actual = sorted((r[col] for r in srows), key=key)
                assert actual == sorted(expected, key=key), \
                    f"{ns}.{col} wrong: {actual[:4]} != {sorted(expected, key=key)[:4]}"
        # src_c has no 'z' -- it must read back NULL, not another column's values.
        assert all(r["z"] is None for r in rows if r["source_connection_id"] == "src_c"), \
            "src_c lacks 'z' but the target returned values for it"

        # Manifest bounds must match the target's declared types (Snowflake symptom).
        widths = {"boolean": 1, "int": 4, "float": 4, "long": 8, "double": 8}
        by_id = {f.field_id: f for f in tgt.schema().fields}
        for task in tgt.scan().plan_files():
            for fid, raw in (task.file.lower_bounds or {}).items():
                fld = by_id.get(fid)
                assert fld is not None, f"file carries a bound for unknown field id {fid}"
                want = widths.get(str(fld.field_type))
                assert want is None or len(raw) == want, (
                    f"field id {fid} ('{fld.name}' {fld.field_type}) has a {len(raw)}-byte "
                    f"bound, expected {want} -- this is the Snowflake error"
                )
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_type_divergence_widens_target():
    """Same column name and id, different TYPE per source.

    Drift detection compares a field id's NAME against the target's, never
    its type -- so before promote_target_types_to_widest() existed, a source
    holding `long` where the baseline held `int` was spliced in silently. The
    run reported success with zero collisions and the target was then
    unreadable: "ResolveError: Cannot promote long to int".

    The repair is to widen the TARGET, not to rewrite: Iceberg reads a narrow
    file through a wider schema fine, so this must cost ZERO rewrites.
    """
    work = tempfile.mkdtemp(prefix="ult_types_")
    try:
        from pyiceberg.types import IntegerType

        cat = SqlCatalog("t", uri=f"sqlite:///{work}/c.db", warehouse=f"file://{work}")
        n = 6
        schemas = {
            "src_a": Schema(NestedField(1, "k", StringType(), required=False),
                            NestedField(2, "amount", IntegerType(), required=False)),
            "src_b": Schema(NestedField(1, "k", StringType(), required=False),
                            NestedField(2, "amount", LongType(), required=False)),
        }
        # Values that only fit in a long -- truncation would be visible.
        data = {"src_a": {"k": [f"a{i}" for i in range(n)], "amount": list(range(n))},
                "src_b": {"k": [f"b{i}" for i in range(n)], "amount": [2**40 + i for i in range(n)]}}
        for ns, schema in schemas.items():
            cat.create_namespace(ns)
            t = cat.create_table(f"{ns}.t", schema=schema)
            t.append(pa.table({c: data[ns][c] for c in [f.name for f in schema.fields]})
                     .cast(schema.as_arrow()))

        results, tgt = _consolidate(cat, "t", tuple(schemas))
        got, per_src = _counts(tgt, tuple(schemas))

        tgt_types = {f.name: str(f.field_type) for f in tgt.schema().fields}
        assert tgt_types["amount"] == "long", \
            f"target kept the narrow type: amount is {tgt_types['amount']}"
        assert got.num_rows == 2 * n, f"expected {2*n} rows, got {got.num_rows}"
        assert results[0]["total_relabeled"] == 0, (
            "widening a type must not rewrite anything -- Iceberg reads a narrow file "
            f"through a wider schema; got {results[0]['total_relabeled']} relabeled rows"
        )
        for ns in schemas:
            vals = sorted(r["amount"] for r in got.to_pylist()
                          if r["source_connection_id"] == ns)
            assert vals == sorted(data[ns]["amount"]), f"{ns}.amount truncated or wrong"
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_incompatible_types_abort():
    """Types no single Iceberg column can hold must stop the run, not guess.

    string vs int is not a widening -- picking either silently corrupts one
    source. Must raise SystemExit with an actionable message.
    """
    work = tempfile.mkdtemp(prefix="ult_incompat_")
    try:
        from pyiceberg.types import IntegerType

        cat = SqlCatalog("t", uri=f"sqlite:///{work}/c.db", warehouse=f"file://{work}")
        schemas = {
            "src_a": Schema(NestedField(1, "k", StringType(), required=False),
                            NestedField(2, "amount", IntegerType(), required=False)),
            "src_b": Schema(NestedField(1, "k", StringType(), required=False),
                            NestedField(2, "amount", StringType(), required=False)),
        }
        data = {"src_a": {"k": ["a"], "amount": [1]},
                "src_b": {"k": ["b"], "amount": ["x"]}}
        for ns, schema in schemas.items():
            cat.create_namespace(ns)
            t = cat.create_table(f"{ns}.t", schema=schema)
            t.append(pa.table({c: data[ns][c] for c in [f.name for f in schema.fields]})
                     .cast(schema.as_arrow()))

        try:
            _consolidate(cat, "t", tuple(schemas), runs=1)
        except SystemExit as e:
            assert "irreconcilably" in str(e) and "amount" in str(e), \
                f"aborted, but the message is unhelpful: {e}"
            return
        except Exception as e:  # noqa: BLE001
            raise AssertionError(f"expected a clean SystemExit, got {type(e).__name__}: {e}")
        raise AssertionError("incompatible types were accepted instead of aborting")
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    failed = 0
    for fn in (test_base_schema_divergence, test_ordinary_drift,
               test_heterogeneous_sources, test_type_divergence_widens_target,
               test_incompatible_types_abort):
        try:
            fn()
            print(f"PASS  {fn.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL  {fn.__name__}: {e}")
        except BaseException as e:  # noqa: BLE001
            # Catch everything, including SystemExit. Several of these
            # regressions manifest as a raised exception rather than a failed
            # assertion -- an unregistered column aborts the append with
            # ValueError, incompatible types raise SystemExit. Letting those
            # propagate would kill the whole run at the first one and hide
            # every test after it, which is exactly what happened the first
            # time this suite was pointed at the pre-fix code.
            failed += 1
            print(f"FAIL  {fn.__name__}: raised {type(e).__name__}: {e}")
    sys.exit(1 if failed else 0)
