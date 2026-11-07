#!/usr/bin/env python3
"""Tests for the fixes driven by a customer's dev-run logs (2026-11-04/05).

Local SQLite catalog + file:// warehouse, same as test_relabel_refresh.py --
no Polaris, Glue, AWS or network:

    python tests/test_scale_fixes.py

What the logs showed, and what each test pins down:

  * verify_consolidation.py was killed for out-of-memory (exit 137): it
    counted rows with scan().to_arrow(), materializing whole tables.
  * verify reported a 1-row difference as an ERROR when the source had simply
    moved on since registration -- lag, not corruption.
  * already_registered_by_source() swallowed every exception, so a transient
    read failure on the target looked like "nothing registered" and the run
    would splice every file in again (duplicates).
  * relabel concurrency was tied to source_workers (6), so one table's 5,373
    relabels took 26 minutes.
"""
import logging
import os
import sys

import pyarrow as pa
from pyiceberg.table import DataScan

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))
sys.path.insert(0, HERE)

from test_collision_rewrite import _consolidate  # noqa: E402
from test_relabel_refresh import N, _assert_matches, _setup, _with_catalog  # noqa: E402

import register_consolidation as rc  # noqa: E402
import verify_consolidation as vc  # noqa: E402

SOURCES = ("src_a", "src_b")


def _verify(cat):
    return vc.verify_table(cat, "items", list(SOURCES), "consolidated", "source_connection_id")


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append((record.levelname, record.getMessage()))


def _captured(fn):
    h = _Capture()
    vc.logger.addHandler(h)
    try:
        result = fn()
    finally:
        vc.logger.removeHandler(h)
    return result, h.records


def _append_rows(cat, ns, start, n):
    t = cat.load_table(f"{ns}.items")
    rows = {"id": list(range(start, start + n)), "label": [f"new-{i}" for i in range(n)], "amount": list(range(n))}
    t.append(pa.table({f.name: rows[f.name] for f in t.schema().fields}).cast(t.schema().as_arrow()))


def test_source_snapshots_recorded_and_carried_forward():
    def body(work):
        cat = _setup(work)
        _consolidate(cat, "items", SOURCES, runs=1)
        tgt = cat.load_table("consolidated.items")
        want = {ns: cat.load_table(f"{ns}.items").current_snapshot().snapshot_id for ns in SOURCES}
        assert rc.recorded_source_snapshots(tgt) == want, rc.recorded_source_snapshots(tgt)

        # A run over ONE source must not erase the other's record.
        _append_rows(cat, "src_b", 200, 3)
        cfg = {"source_namespaces": ["src_b"], "target_namespace": "consolidated",
               "source_id_column": "source_connection_id", "table_workers": 1, "source_workers": 1}
        rc.register_table("items", cfg)
        tgt = cat.load_table("consolidated.items")
        rec = rc.recorded_source_snapshots(tgt)
        assert rec["src_a"] == want["src_a"], rec
        assert rec["src_b"] == cat.load_table("src_b.items").current_snapshot().snapshot_id, rec
    _with_catalog(body)


def test_verify_never_materializes_whole_tables():
    def body(work):
        cat = _setup(work)
        _consolidate(cat, "items", SOURCES, runs=1)
        original = DataScan.to_arrow

        def refuse(self, *a, **kw):
            raise AssertionError("verify called scan().to_arrow() -- whole-table materialization")

        DataScan.to_arrow = refuse
        try:
            assert _verify(cat) is True
        finally:
            DataScan.to_arrow = original
    _with_catalog(body)


def test_verify_reports_lag_not_error_when_source_moved_on():
    def body(work):
        cat = _setup(work)
        _consolidate(cat, "items", SOURCES, runs=1)
        _append_rows(cat, "src_a", 500, 4)  # source moves on; no re-registration
        ok, records = _captured(lambda: _verify(cat))
        assert ok is True, records
        assert any(lvl == "WARNING" and "LAG for source 'src_a'" in msg for lvl, msg in records), records
        assert not any(lvl == "ERROR" for lvl, _ in records), records

        # After catching up, it matches again.
        _consolidate(cat, "items", SOURCES, runs=1)
        ok, records = _captured(lambda: _verify(cat))
        assert ok is True and not any(lvl in ("WARNING", "ERROR") for lvl, _ in records), records
        _assert_matches(cat, cat.load_table("consolidated.items"))
    _with_catalog(body)


def test_verify_still_fails_on_a_real_mismatch():
    def body(work):
        cat = _setup(work)
        _consolidate(cat, "items", SOURCES, runs=1)
        # Simulate a target without snapshot tracking (written by an older
        # version) that is short of rows: drop src_a's files from the target.
        tgt = cat.load_table("consolidated.items")
        with tgt.transaction() as txn:
            with txn.update_snapshot().overwrite() as ov:
                for task in tgt.scan(row_filter="source_connection_id = 'src_a'").plan_files():
                    ov.delete_data_file(task.file)
        ok, records = _captured(lambda: _verify(cat))
        assert ok is False, records
        assert any(lvl == "ERROR" and "ROW COUNT MISMATCH for source 'src_a'" in msg for lvl, msg in records), records
    _with_catalog(body)


def test_target_read_failure_is_not_treated_as_empty():
    class _Boom:
        def plan_files(self):
            raise OSError("simulated transient S3 failure")

    class _Target:
        class metadata:
            location = "file:///nowhere"

        def current_snapshot(self):
            return object()

        def scan(self):
            return _Boom()

    try:
        rc.already_registered_by_source(_Target())
    except OSError:
        return
    raise AssertionError("a failed target read was swallowed -- the run would splice every file again")


def test_relabel_pool_is_shared_and_sized_by_relabel_workers():
    assert rc._relabel_pool(5) is rc._relabel_pool(5)
    assert rc._relabel_pool(5)._max_workers == 5

    def body(work):
        cat = _setup(work)
        rc.get_catalog = lambda: cat
        rc.thread_catalog = lambda: cat
        cfg = {"source_namespaces": list(SOURCES), "target_namespace": "consolidated",
               "source_id_column": "source_connection_id", "table_workers": 1, "source_workers": 1,
               "relabel_workers": 3}
        rc.run(cfg, tables=["items"])
        assert 3 in rc._relabel_pools, "relabel ran on a pool not sized by relabel_workers"
        _assert_matches(cat, cat.load_table("consolidated.items"))
        assert cat.load_table("consolidated.items").scan().to_arrow().num_rows == 2 * N
    _with_catalog(body)


def _cfg(**kw):
    cfg = {"source_namespaces": list(SOURCES), "target_namespace": "consolidated",
           "source_id_column": "source_connection_id", "table_workers": 1, "source_workers": 1}
    cfg.update(kw)
    return cfg


def test_large_initial_relabel_commits_in_batches_and_resumes_after_a_crash():
    import parquet_footer

    def body(work):
        cat = _setup(work)
        for k in range(4):  # src_b (the divergent source) now has 5 files to relabel
            _append_rows(cat, "src_b", 1000 + 10 * k, 2)
        rc.get_catalog = lambda: cat
        rc.thread_catalog = lambda: cat

        real = parquet_footer.relabel_via_fileio
        calls = {"n": 0}

        def flaky(*a, **kw):
            calls["n"] += 1
            if calls["n"] == 4:  # die inside the 2nd batch of 2
                raise OSError("simulated crash mid-load")
            return real(*a, **kw)

        parquet_footer.relabel_via_fileio = flaky
        try:
            rc.run(_cfg(relabel_commit_batch=2), tables=["items"])
            raise AssertionError("expected the simulated crash")
        except OSError:
            pass
        finally:
            parquet_footer.relabel_via_fileio = real

        tgt = cat.load_table("consolidated.items")
        committed = [t for t in tgt.scan().plan_files() if t.file.partition[0] == "src_b"]
        assert len(committed) == 2, f"first batch should be committed, got {len(committed)}"
        assert "src_b" not in rc.recorded_source_snapshots(tgt), "an interrupted load must not claim the new snapshot"

        calls["n"] = 0
        parquet_footer.relabel_via_fileio = lambda *a, **kw: (calls.__setitem__("n", calls["n"] + 1), real(*a, **kw))[1]
        try:
            rc.run(_cfg(relabel_commit_batch=2), tables=["items"])
        finally:
            parquet_footer.relabel_via_fileio = real
        assert calls["n"] == 3, f"rerun should copy only the 3 uncommitted files, copied {calls['n']}"
        tgt = cat.load_table("consolidated.items")
        _assert_matches(cat, tgt)
        assert _verify(cat) is True
    _with_catalog(body)


def test_refresh_with_retirements_stays_one_atomic_commit():
    def body(work):
        cat = _setup(work)
        for k in range(3):
            _append_rows(cat, "src_b", 1000 + 10 * k, 2)
        rc.get_catalog = lambda: cat
        rc.thread_catalog = lambda: cat
        rc.run(_cfg(relabel_commit_batch=1), tables=["items"])
        src_b = cat.load_table("src_b.items")
        src_b.delete("id < 3")  # copy-on-write: replaces a file -> retirement on refresh
        before = len(cat.load_table("consolidated.items").metadata.snapshots)
        rc.run(_cfg(relabel_commit_batch=1), tables=["items"])
        after = len(cat.load_table("consolidated.items").metadata.snapshots)
        assert after - before == 1, f"a refresh that retires files must be ONE commit, made {after - before}"
        _assert_matches(cat, cat.load_table("consolidated.items"))
    _with_catalog(body)


def _count_planning(fn):
    """Run fn while counting which sources get their files planned."""
    planned = []
    real = rc._plan_source_files

    def spy(ns, table):
        planned.append(ns)
        return real(ns, table)

    rc._plan_source_files = spy
    try:
        result = fn()
    finally:
        rc._plan_source_files = real
    return result, planned


def test_unchanged_sources_are_not_planned():
    def body(work):
        cat = _setup(work)
        rc.get_catalog = lambda: cat
        rc.thread_catalog = lambda: cat
        rc.run(_cfg(), tables=["items"])
        snaps = len(cat.load_table("consolidated.items").metadata.snapshots)

        summary, planned = _count_planning(lambda: rc.run(_cfg(), tables=["items"]))
        assert planned == [], f"unchanged sources were planned: {planned}"
        assert len(cat.load_table("consolidated.items").metadata.snapshots) == snaps, "a no-op run committed"

        _append_rows(cat, "src_a", 300, 2)
        summary, planned = _count_planning(lambda: rc.run(_cfg(), tables=["items"]))
        assert planned == ["src_a"], f"expected only the changed source planned, got {planned}"
        _assert_matches(cat, cat.load_table("consolidated.items"))

        summary, planned = _count_planning(lambda: rc.run(_cfg(full_refresh=True), tables=["items"]))
        assert sorted(planned) == ["src_a", "src_b"], planned
        assert summary["total_spliced"] == summary["total_relabeled"] == 0, "full refresh re-registered files"
    _with_catalog(body)


def test_sources_filter_leaves_other_sources_untouched():
    def body(work):
        cat = _setup(work)
        rc.get_catalog = lambda: cat
        rc.thread_catalog = lambda: cat
        rc.run(_cfg(), tables=["items"])
        cat.load_table("src_a.items").delete("id < 2")   # copy-on-write in both sources
        cat.load_table("src_b.items").delete("id < 2")
        before_b = sorted(t.file.file_path for t in cat.load_table("consolidated.items").scan(
            row_filter="source_connection_id = 'src_b'").plan_files())

        _, planned = _count_planning(lambda: rc.run(_cfg(only_sources={"src_a"}), tables=["items"]))
        assert planned == ["src_a"], planned
        tgt = cat.load_table("consolidated.items")
        after_b = sorted(t.file.file_path for t in tgt.scan(row_filter="source_connection_id = 'src_b'").plan_files())
        assert after_b == before_b, "a run limited to src_a changed src_b's registered files"
        _assert_matches(cat, tgt, sources=("src_a",))

        rc.run(_cfg(), tables=["items"])  # now catch src_b up
        _assert_matches(cat, cat.load_table("consolidated.items"))
        assert _verify(cat) is True
    _with_catalog(body)


def test_table_missing_from_one_source_is_still_registered():
    def body(work):
        cat = _setup(work)
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField
        schema = Schema(NestedField(1, "id", LongType(), required=False))
        extras = cat.create_table("src_a.extras", schema=schema)  # src_b has no 'extras'
        extras.append(pa.table({"id": [1, 2, 3]}).cast(schema.as_arrow()))
        rc.get_catalog = lambda: cat
        rc.thread_catalog = lambda: cat
        summary = rc.run(_cfg())  # auto-discover
        assert summary["tables"] == 2, summary
        tgt = cat.load_table("consolidated.extras")
        assert tgt.scan().to_arrow().num_rows == 3
        _assert_matches(cat, cat.load_table("consolidated.items"))
        # A later run with src_b still lacking the table retires nothing.
        rc.run(_cfg(full_refresh=True))
        assert cat.load_table("consolidated.extras").scan().to_arrow().num_rows == 3
        assert vc.verify_table(cat, "extras", ["src_a"], "consolidated", "source_connection_id") is True
    _with_catalog(body)


def _local(path):
    return path[len("file://"):] if path.startswith("file://") else path


def test_cleanup_deletes_retired_copies_but_never_source_files():
    import time as _time

    def body(work):
        cat = _setup(work)
        rc.get_catalog = lambda: cat
        rc.thread_catalog = lambda: cat
        rc.run(_cfg(), tables=["items"])
        tgt = cat.load_table("consolidated.items")
        old_copy = [t.file.file_path for t in tgt.scan().plan_files() if t.file.partition[0] == "src_b"]
        old_spliced = [t.file.file_path for t in tgt.scan().plan_files() if t.file.partition[0] == "src_a"]
        assert len(old_copy) == 1 and "/relabeled/" in old_copy[0] and len(old_spliced) == 1

        cat.load_table("src_a.items").delete("id < 2")   # retires the spliced reference
        cat.load_table("src_b.items").delete("id < 2")   # retires the relabeled copy
        rc.run(_cfg(), tables=["items"])                 # default 24 h grace: nothing deleted
        assert os.path.exists(_local(old_copy[0])), "default grace window deleted a file"

        _time.sleep(0.2)
        cat.load_table("src_a.items").delete("id = 5")
        rc.run(_cfg(cleanup_grace_minutes=0.001), tables=["items"])  # ~60 ms grace
        assert not os.path.exists(_local(old_copy[0])), "retired relabeled copy was not deleted"
        assert os.path.exists(_local(old_spliced[0])), "cleanup deleted a SOURCE file"
        tgt = cat.load_table("consolidated.items")
        assert len(tgt.metadata.snapshots) < 4, f"old snapshots not expired: {len(tgt.metadata.snapshots)}"
        _assert_matches(cat, tgt)
        assert _verify(cat) is True
    _with_catalog(body)


def test_cleanup_off_when_grace_is_zero():
    import time as _time

    def body(work):
        cat = _setup(work)
        rc.get_catalog = lambda: cat
        rc.thread_catalog = lambda: cat
        rc.run(_cfg(), tables=["items"])
        cat.load_table("src_b.items").delete("id < 2")
        _time.sleep(0.1)
        rc.run(_cfg(cleanup_grace_minutes=0), tables=["items"])
        assert rc.cleanup_target(cat, "consolidated.items", {"cleanup_grace_minutes": 0}) == (0, 0)
        assert len(cat.load_table("consolidated.items").metadata.snapshots) == 2
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
