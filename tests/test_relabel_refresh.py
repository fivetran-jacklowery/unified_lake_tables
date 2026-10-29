#!/usr/bin/env python3
"""Regression tests for the relabel path across REFRESHES.

Runs entirely against a local SQLite catalog with file:// warehouse -- no
Polaris, no Glue, no AWS, no network. Run it directly or under pytest:

    python tests/test_relabel_refresh.py
    pytest tests/test_relabel_refresh.py

Needs `sqlalchemy` on top of requirements.txt (pyiceberg's SqlCatalog).

WHY THESE EXIST
---------------
test_collision_rewrite.py covers a first run and a rerun with nothing
changed. Neither changes a source BETWEEN runs, and that is exactly where the
physical rewrite path this tool used to have was wrong. Its output files had
no tie back to the source files they came from, so its idempotency rested on
comparing row counts, which cannot see copy-on-write:

  * an UPDATE leaves the row count unchanged -> skipped, target keeps stale values
  * a DELETE makes the target LARGER than the source -> skipped, deleted rows kept
  * growth makes the counts differ -> every drifted file appended again (duplicates)

The relabel path ties each copy to its source file by path, so all three are
a file-level set comparison. Each test below changes a divergent source with
pyiceberg's own copy-on-write operations and compares the target to the
source value for value.
"""
import os
import shutil
import sys
import tempfile

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo, In
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, LongType, NestedField, StringType

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
sys.path.insert(0, HERE)

from test_collision_rewrite import _consolidate  # noqa: E402

N = 12


def _setup(work, amount_type_b=LongType()):
    """Two sources with the same columns in a different order, so every
    field id disagrees and src_b's files all take the relabel path."""
    cat = SqlCatalog("t", uri=f"sqlite:///{work}/c.db", warehouse=f"file://{work}")
    schemas = {
        "src_a": Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "label", StringType(), required=False),
            NestedField(3, "amount", LongType(), required=False),
        ),
        "src_b": Schema(
            NestedField(1, "amount", amount_type_b, required=False),
            NestedField(2, "id", LongType(), required=False),
            NestedField(3, "label", StringType(), required=False),
        ),
    }
    for ns, schema in schemas.items():
        cat.create_namespace(ns)
        t = cat.create_table(f"{ns}.items", schema=schema)
        base = 0 if ns == "src_a" else 1000
        rows = {
            "id": list(range(N)),
            "label": [None if i % 4 == 0 else f"{ns}-{i}" for i in range(N)],
            "amount": [base + i for i in range(N)],
        }
        t.append(pa.table({f.name: rows[f.name] for f in schema.fields}).cast(schema.as_arrow()))
    return cat


def _assert_matches(cat, tgt, sources=("src_a", "src_b")):
    for ns in sources:
        src = cat.load_table(f"{ns}.items").scan().to_arrow()
        got = tgt.scan(row_filter=EqualTo("source_connection_id", ns)).to_arrow()
        key = lambda r: r["id"]  # noqa: E731
        want = sorted(src.select(["id", "label", "amount"]).to_pylist(), key=key)
        have = sorted(got.select(["id", "label", "amount"]).to_pylist(), key=key)
        assert have == want, f"{ns}: target does not match source\n  want {want[:4]}\n  have {have[:4]}"


def _with_catalog(fn):
    work = tempfile.mkdtemp(prefix="ult_relabel_")
    try:
        fn(work)
    finally:
        shutil.rmtree(work, ignore_errors=True)


def test_update_is_picked_up():
    def body(work):
        cat = _setup(work)
        _consolidate(cat, "items", ("src_a", "src_b"), runs=1)
        src_b = cat.load_table("src_b.items")
        src_b.overwrite(
            pa.table({"amount": [999_999], "id": [3], "label": ["updated"]}).cast(src_b.schema().as_arrow()),
            overwrite_filter=EqualTo("id", 3),
        )
        _, tgt = _consolidate(cat, "items", ("src_a", "src_b"), runs=2)
        _assert_matches(cat, tgt)
    _with_catalog(body)


def test_delete_is_picked_up():
    def body(work):
        cat = _setup(work)
        _consolidate(cat, "items", ("src_a", "src_b"), runs=1)
        cat.load_table("src_b.items").delete(In("id", [1, 2, 7]))
        _, tgt = _consolidate(cat, "items", ("src_a", "src_b"), runs=2)
        _assert_matches(cat, tgt)
        assert tgt.scan(row_filter=EqualTo("source_connection_id", "src_b")).to_arrow().num_rows == N - 3
    _with_catalog(body)


def test_growth_does_not_duplicate():
    def body(work):
        cat = _setup(work)
        _consolidate(cat, "items", ("src_a", "src_b"), runs=1)
        src_b = cat.load_table("src_b.items")
        src_b.append(pa.table({"amount": [5000, 5001], "id": [N, N + 1], "label": [None, "new"]})
                     .cast(src_b.schema().as_arrow()))
        results, tgt = _consolidate(cat, "items", ("src_a", "src_b"), runs=2)
        _assert_matches(cat, tgt)
        assert results[-1]["total_relabeled"] == 0, "a rerun with nothing new relabeled again"
    _with_catalog(body)


def test_promoted_type_is_relabeled_without_bad_bounds():
    """src_b holds `amount` as int, the target widens it to long, and src_b's
    ids disagree too -- so its files are relabeled AND read at a wider type.
    Bounds serialized as 4-byte ints must not be registered under a long
    column (the warehouse-rejects-the-bound failure)."""
    def body(work):
        cat = _setup(work, amount_type_b=IntegerType())
        _, tgt = _consolidate(cat, "items", ("src_a", "src_b"), runs=2)
        _assert_matches(cat, tgt)
        amount_id = tgt.schema().find_field("amount").field_id
        assert str(tgt.schema().find_field("amount").field_type) == "long"
        for task in tgt.scan().plan_files():
            raw = (task.file.lower_bounds or {}).get(amount_id)
            assert raw is None or len(raw) == 8, f"{len(raw)}-byte bound under a long column"
    _with_catalog(body)


def test_old_rewrite_files_are_migrated():
    """A target built by the old rewrite path holds plain appended files for
    the divergent source. They must be retired, not duplicated, when that
    source's files are relabeled."""
    def body(work):
        cat = _setup(work)
        _consolidate(cat, "items", ("src_a", "src_b"), runs=1)
        tgt = cat.load_table("consolidated.items")
        # Recreate the old state: drop src_b's relabeled copies and append its
        # rows the way the rewrite path did (Transaction.append of the data).
        relabeled = [t.file for t in tgt.scan(row_filter=EqualTo("source_connection_id", "src_b")).plan_files()]
        with tgt.transaction() as txn:
            with txn.update_snapshot({}).overwrite() as ov:
                for df in relabeled:
                    ov.delete_data_file(df)
        tgt = cat.load_table("consolidated.items")
        rows = cat.load_table("src_b.items").scan().to_arrow().select(["id", "label", "amount"])
        rows = rows.append_column("source_connection_id", pa.array(["src_b"] * rows.num_rows, pa.string()))
        tgt.append(rows.cast(pa.schema([tgt.schema().as_arrow().field(n) for n in rows.column_names])))

        _, tgt = _consolidate(cat, "items", ("src_a", "src_b"), runs=2)
        _assert_matches(cat, tgt)
        paths = [t.file.file_path for t in tgt.scan(row_filter=EqualTo("source_connection_id", "src_b")).plan_files()]
        assert paths and all("/relabeled/" in p for p in paths), f"old rewrite files left behind: {paths}"
    _with_catalog(body)


def test_self_contained_target():
    """self_contained_target relabels every source, including ones that could
    be spliced, so the target references nothing outside its own location.
    Switching it on for a target that already holds spliced references must
    replace them, not duplicate them, and later refreshes must still track
    copy-on-write."""
    import register_consolidation as rc

    def run(cat, runs):
        rc.get_catalog = lambda: cat
        rc.thread_catalog = lambda: cat
        cfg = {"source_namespaces": ["src_a", "src_b"], "target_namespace": "consolidated",
               "source_id_column": "source_connection_id", "table_workers": 1, "source_workers": 1,
               "self_contained_target": True}
        return [rc.run(dict(cfg), tables=["items"]) for _ in range(runs)]

    def body(work):
        cat = _setup(work)
        _consolidate(cat, "items", ("src_a", "src_b"), runs=1)  # src_a spliced by reference
        results = run(cat, runs=2)
        tgt = cat.load_table("consolidated.items")
        _assert_matches(cat, tgt)
        loc = tgt.metadata.location
        outside = [t.file.file_path for t in tgt.scan().plan_files() if not t.file.file_path.startswith(loc)]
        assert not outside, f"self-contained target still references source files: {outside}"
        assert results[-1]["total_relabeled"] == 0, "a rerun with nothing new relabeled again"

        cat.load_table("src_a.items").delete(EqualTo("id", 5))
        run(cat, runs=1)
        _assert_matches(cat, cat.load_table("consolidated.items"))
    _with_catalog(body)


if __name__ == "__main__":
    failed = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS  {name}")
            except BaseException as e:  # noqa: BLE001
                failed += 1
                print(f"FAIL  {name}: {type(e).__name__}: {e}")
    sys.exit(1 if failed else 0)
