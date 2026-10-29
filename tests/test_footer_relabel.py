"""Offline tests for scripts/parquet_footer.py -- no catalog, no S3, no network.

Run with plain `python tests/test_footer_relabel.py` or `pytest`.

The property that matters: a relabeled file is the SAME bytes up to the footer,
declares the target's field ids, and reads back -- through Iceberg's
by-field-id projection -- with every value under the right column name.
"""

import io
import os
import random
import sys

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import parquet_footer as rf  # noqa: E402

N = 50_000


def _tenant_file(order: list) -> tuple:
    """A Parquet file with columns in `order`, field ids assigned 1..n in that
    order (as Fivetran does), mixed types, NULLs, several row groups, page
    indexes and dictionary encoding."""
    r = random.Random(7)
    values = {
        "account_id": pa.array(range(N), pa.int64()),
        "client_name": pa.array([r.choice(["cedar", "atlas", None]) for _ in range(N)], pa.string()),
        "balance": pa.array([r.randint(-10**6, 10**8) / 100 for _ in range(N)]).cast(pa.decimal128(18, 2)),
        "risk_score": pa.array([r.random() for _ in range(N)], pa.float64()),
        "num_positions": pa.array([r.randint(0, 400) if r.random() > 0.1 else None for _ in range(N)], pa.int32()),
        "is_active": pa.array([r.random() < 0.8 for _ in range(N)], pa.bool_()),
    }
    fields = [pa.field(name, values[name].type, metadata={b"PARQUET:field_id": str(i).encode()})
              for i, name in enumerate(order, 1)]
    table = pa.table([values[n] for n in order], schema=pa.schema(fields))
    buf = io.BytesIO()
    pq.write_table(table, buf, row_group_size=12_000, write_page_index=True, compression="zstd")
    return buf.getvalue(), table


def _ids(data: bytes) -> dict:
    return {f.name: int(f.metadata[b"PARQUET:field_id"]) for f in pq.read_schema(io.BytesIO(data))}


def test_footer_round_trips_byte_for_byte():
    data, _ = _tenant_file(["account_id", "client_name", "balance", "risk_score", "num_positions", "is_active"])
    _, footer = rf.split_parquet(data)
    assert rf.encode_footer(rf.decode_footer(footer)) == footer


def test_relabel_changes_only_the_footer_and_reads_back_correctly():
    canonical = ["account_id", "client_name", "balance", "risk_score", "num_positions", "is_active"]
    shuffled = ["is_active", "balance", "account_id", "num_positions", "client_name", "risk_score"]
    target_ids = {name: i for i, name in enumerate(canonical, 1)}

    data, table = _tenant_file(shuffled)
    src_ids = _ids(data)
    id_map = {src_ids[name]: target_ids[name] for name in shuffled}

    out = rf.relabel_bytes(data, id_map)

    # Data pages untouched: identical bytes up to the old footer.
    prefix, _ = rf.split_parquet(data)
    assert out[: len(prefix)] == prefix

    # Footer now declares the target's ids.
    assert _ids(out) == target_ids

    # Every value intact, page indexes still resolvable.
    back = pq.read_table(io.BytesIO(out))
    for name in shuffled:
        assert back[name].equals(table[name]), name
    md = pq.ParquetFile(io.BytesIO(out)).metadata
    assert md.num_row_groups > 1
    assert all(md.row_group(0).column(i).has_offset_index for i in range(md.num_columns))


def test_iceberg_projection_reads_relabeled_file_under_target_schema():
    """The real test: read the file the way an Iceberg engine does -- by field
    id against the TARGET schema. Before relabeling, id 2 means `balance` in
    this file but `client_name` in the target, so values land under the wrong
    name (or fail to cast); after, every column matches."""
    from pyiceberg.io.pyarrow import pyarrow_to_schema
    from pyiceberg.io.pyarrow import _to_requested_schema  # noqa: PLC2701  (private, test-only)

    canonical = ["account_id", "client_name", "balance", "risk_score", "num_positions", "is_active"]
    shuffled = ["balance", "client_name", "account_id", "is_active", "risk_score", "num_positions"]

    canon_data, _ = _tenant_file(canonical)
    target_schema = pyarrow_to_schema(pq.read_schema(io.BytesIO(canon_data)))

    data, table = _tenant_file(shuffled)
    src_ids = _ids(data)
    id_map = {src_ids[n]: target_schema.find_field(n).field_id for n in shuffled}
    out = rf.relabel_bytes(data, id_map)

    arrow = pq.read_table(io.BytesIO(out))
    file_schema = pyarrow_to_schema(arrow.schema)
    projected = _to_requested_schema(target_schema, file_schema, arrow.to_batches()[0])
    first = table.slice(0, projected.num_rows)
    for name in canonical:
        assert projected.column(name).equals(first[name].combine_chunks()), name


def test_unmapped_column_is_refused():
    data, _ = _tenant_file(["account_id", "client_name", "balance", "risk_score", "num_positions", "is_active"])
    try:
        rf.relabel_bytes(data, {1: 1, 2: 2})
    except KeyError:
        return
    raise AssertionError("expected KeyError for columns with no target mapping")


def test_real_fivetran_footer_if_present():
    """Round-trip a footer written by Fivetran's own writer (parquet-mr), if a
    sample has been downloaded to RELABEL_SAMPLE."""
    path = os.environ.get("RELABEL_SAMPLE")
    if not path or not os.path.exists(path):
        return
    data = open(path, "rb").read()
    _, footer = rf.split_parquet(data)
    assert rf.encode_footer(rf.decode_footer(footer)) == footer
    ids = rf.footer_field_ids(footer)
    reversed_map = dict(zip(sorted(ids), sorted(ids, reverse=True)))
    out = rf.relabel_bytes(data, reversed_map)
    assert pq.read_table(io.BytesIO(out)).equals(pq.read_table(io.BytesIO(data)))
    assert {v: k for k, v in rf.footer_field_ids(rf.split_parquet(out)[1]).items()} == \
        {name: reversed_map[i] for i, name in ids.items()}


if __name__ == "__main__":
    failed = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            try:
                fn()
                print(f"PASS  {name}")
            except Exception as e:  # noqa: BLE001
                failed += 1
                print(f"FAIL  {name}: {type(e).__name__}: {e}")
    sys.exit(1 if failed else 0)
