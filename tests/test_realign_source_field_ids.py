#!/usr/bin/env python3
"""Tests for scripts/realign_source_field_ids.py -- the EXPERIMENTAL script
that MODIFIES SOURCE TABLES. Local SQLite catalog + file:// warehouse:

    python tests/test_realign_source_field_ids.py

Most of these pin down the safety rails, not just the happy path: a dry run
changes nothing, nothing runs without the acknowledgement flag, a source that
moves during the copy is left untouched, a failed verification rolls back,
and rollback refuses to discard commits the writer made after the swap.
"""
import os
import sys

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
sys.path.insert(0, HERE)

from test_relabel_refresh import _assert_matches, _setup, _with_catalog  # noqa: E402

import register_consolidation as rc  # noqa: E402
import realign_source_field_ids as rs  # noqa: E402
import verify_consolidation as vc  # noqa: E402

CFG = {"source_namespaces": ["src_a", "src_b"], "target_namespace": "consolidated",
       "source_id_column": "source_connection_id", "table_workers": 1, "source_workers": 1,
       "relabel_workers": 4}


def _ready(work):
    """Two sources with different layouts, consolidated once (src_b relabeled)."""
    cat = _setup(work)
    rc.get_catalog = lambda: cat
    rc.thread_catalog = lambda: cat
    rc.run(dict(CFG), tables=["items"])
    return cat


def _rows(cat, ident):
    return sorted(cat.load_table(ident).scan().to_arrow().select(["id", "label", "amount"]).to_pylist(),
                  key=lambda r: r["id"])


def _ids(cat, ident):
    return {f.name: f.field_id for f in cat.load_table(ident).schema().fields}


def _plan(cat, ns="src_b", table="items"):
    ref = rs.reference_schema(cat, CFG, table)
    return rs.plan_one(cat, CFG, ns, table, ref)


def test_plan_changes_nothing():
    def body(work):
        cat = _ready(work)
        before = cat.load_table("src_b.items").metadata_location
        p = _plan(cat)
        assert not p["aligned"] and "change field id" in rs.describe(p)
        assert cat.load_table("src_b.items").metadata_location == before
    _with_catalog(body)


def test_apply_requires_the_acknowledgement_flag():
    sys_argv = sys.argv
    for argv in (["x", "apply", "--table", "items", "--sources", "src_b"],
                 ["x", "apply", "--table", "items", "--sources", "src_b", "--execute"]):
        sys.argv = argv
        try:
            rs.main()
            raise AssertionError(f"apply ran without both flags: {argv}")
        except SystemExit as e:
            assert rs.ACK_FLAG in str(e), e
        finally:
            sys.argv = sys_argv


def test_apply_realigns_and_consolidation_then_splices():
    def body(work):
        cat = _ready(work)
        rows_before = _rows(cat, "src_b.items")
        uuid_before = cat.load_table("src_b.items").metadata.table_uuid
        rs.apply_one(cat, CFG, _plan(cat), "items", os.path.join(work, "records"))

        assert _ids(cat, "src_b.items") == {"id": 1, "label": 2, "amount": 3}, _ids(cat, "src_b.items")
        assert _rows(cat, "src_b.items") == rows_before, "realigned source reads different values"
        assert cat.load_table("src_b.items").metadata.table_uuid != uuid_before

        summary = rc.run(dict(CFG), tables=["items"])
        assert summary["total_relabeled"] == 0, "realigned source was relabeled again"
        tgt = cat.load_table("consolidated.items")
        src_b_paths = [t.file.file_path for t in tgt.scan(row_filter="source_connection_id = 'src_b'").plan_files()]
        assert src_b_paths and not any("/relabeled/" in x for x in src_b_paths), src_b_paths
        _assert_matches(cat, tgt)
        assert vc.verify_table(cat, "items", ["src_a", "src_b"], "consolidated", "source_connection_id")

        # The writer keeps going after the swap (here: a copy-on-write delete
        # through the new table); consolidation follows it.
        cat.load_table("src_b.items").delete("id < 3")
        rc.run(dict(CFG), tables=["items"])
        _assert_matches(cat, cat.load_table("consolidated.items"))
    _with_catalog(body)


def test_ids_after_a_gap_are_reproduced():
    def body(work):
        cat = _ready(work)
        schema = Schema(NestedField(1, "amount", LongType(), required=False),
                        NestedField(2, "id", LongType(), required=False))  # no 'label' -> gap at 2
        t = cat.create_table("src_b.narrow", schema=schema)
        t.append(pa.table({"amount": [10, 20], "id": [1, 2]}).cast(schema.as_arrow()))
        ref = rs.reference_schema(cat, CFG, "items")
        p = rs.plan_one(cat, CFG, "src_b", "narrow", ref)
        rs.apply_one(cat, CFG, p, "narrow", os.path.join(work, "records"))
        assert _ids(cat, "src_b.narrow") == {"id": 1, "amount": 3}, _ids(cat, "src_b.narrow")
        got = sorted(cat.load_table("src_b.narrow").scan().to_arrow().to_pylist(), key=lambda r: r["id"])
        assert got == [{"id": 1, "amount": 10}, {"id": 2, "amount": 20}], got
    _with_catalog(body)


def test_source_that_moves_during_the_copy_is_left_untouched():
    def body(work):
        cat = _ready(work)
        before = cat.load_table("src_b.items").metadata_location
        real = rs._copy_files

        def copy_then_writer_commits(p, cfg):
            pairs = real(p, cfg)
            t = cat.load_table("src_b.items")  # simulate a sync landing mid-swap
            t.append(pa.table({"amount": [1], "id": [999], "label": ["late"]}).cast(t.schema().as_arrow()))
            return pairs

        rs._copy_files = copy_then_writer_commits
        try:
            rs.apply_one(cat, CFG, _plan(cat), "items", os.path.join(work, "records"))
            raise AssertionError("apply went ahead although the source changed")
        except rs.Refused:
            pass
        finally:
            rs._copy_files = real
        t = cat.load_table("src_b.items")
        assert t.metadata_location != before  # the simulated sync's commit
        assert _ids(cat, "src_b.items") == {"amount": 1, "id": 2, "label": 3}, "table was modified"
        leftovers = [f for f in os.listdir(os.path.join(work, "src_b", "items", "data")) if f.startswith("realigned-")]
        assert not leftovers, f"copies not cleaned up: {leftovers}"
    _with_catalog(body)


def test_failed_verification_rolls_back_automatically():
    def body(work):
        cat = _ready(work)
        rows_before = _rows(cat, "src_b.items")
        real = rs._verify
        rs._verify = lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("simulated verification failure"))
        try:
            rs.apply_one(cat, CFG, _plan(cat), "items", os.path.join(work, "records"))
            raise AssertionError("expected the failure")
        except RuntimeError:
            pass
        finally:
            rs._verify = real
        assert _ids(cat, "src_b.items") == {"amount": 1, "id": 2, "label": 3}
        assert _rows(cat, "src_b.items") == rows_before
        leftovers = [f for f in os.listdir(os.path.join(work, "src_b", "items", "data")) if f.startswith("realigned-")]
        assert not leftovers, f"copies left behind after the automatic rollback: {leftovers}"
    _with_catalog(body)


def test_rollback_restores_the_original_table():
    def body(work):
        cat = _ready(work)
        orig = cat.load_table("src_b.items")
        rows_before = _rows(cat, "src_b.items")
        record = rs.apply_one(cat, CFG, _plan(cat), "items", os.path.join(work, "records"))
        rs.rollback(cat, record)
        t = cat.load_table("src_b.items")
        assert t.metadata.table_uuid == orig.metadata.table_uuid
        assert _ids(cat, "src_b.items") == {"amount": 1, "id": 2, "label": 3}
        assert _rows(cat, "src_b.items") == rows_before
    _with_catalog(body)


def test_rollback_refuses_after_the_writer_has_written():
    def body(work):
        cat = _ready(work)
        record = rs.apply_one(cat, CFG, _plan(cat), "items", os.path.join(work, "records"))
        cat.load_table("src_b.items").delete("id = 1")  # the connector resumed and synced
        try:
            rs.rollback(cat, record)
            raise AssertionError("rollback discarded commits made after the swap")
        except rs.Refused:
            pass
        rs.rollback(cat, record, force=True)
        assert _ids(cat, "src_b.items") == {"amount": 1, "id": 2, "label": 3}
    _with_catalog(body)


def test_finalize_deletes_originals_and_table_still_reads():
    def body(work):
        cat = _ready(work)
        originals = [t.file.file_path for t in cat.load_table("src_b.items").scan().plan_files()]
        rows_before = _rows(cat, "src_b.items")
        record = rs.apply_one(cat, CFG, _plan(cat), "items", os.path.join(work, "records"))
        rs.finalize(cat, record)
        assert not any(os.path.exists(p[len("file://"):]) for p in originals)
        assert _rows(cat, "src_b.items") == rows_before
    _with_catalog(body)


def test_refuses_a_column_the_reference_does_not_have():
    def body(work):
        cat = _ready(work)
        t = cat.load_table("src_b.items")
        with t.update_schema() as us:
            us.add_column("brand_new", StringType())
        try:
            _plan(cat)
            raise AssertionError("planned a column the reference lacks")
        except rs.Refused as e:
            assert "brand_new" in str(e)
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
