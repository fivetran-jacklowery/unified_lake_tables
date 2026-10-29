"""Relabel a Parquet file's field ids without rewriting its data.

Iceberg resolves Parquet columns by the field id embedded in each file, not by
name. When consolidated sources disagree about what an id means, a source's
files cannot be spliced in by reference. They used to be rewritten in full
(decode every row, encode new Parquet). This module makes that unnecessary.

A Parquet file's field ids live ONLY in its footer -- the Thrift-encoded
FileMetaData at the end of the file. Column chunks, page indexes and row
groups are located by absolute byte offset, so a file whose bytes before the
footer are unchanged, and whose footer differs only in SchemaElement.field_id,
is a valid Parquet file with every offset still correct:

    relabeled file = source bytes [0, footer_start)   <- copied verbatim
                   + relabeled footer                 <- typically ~1-2 KB
                   + footer length + b"PAR1"

Nothing is decoded or re-encoded, so cost is a byte copy rather than compute,
and memory is one file at a time. Two ways to make the copy:

  relabel_via_fileio()  reads with the source table's FileIO and writes with
                        the target's -- each table's own (vended) credential,
                        any storage pyiceberg supports. The bytes pass through
                        the worker.
  relabel_s3_object()   S3 only, and needs ONE credential that can read the
                        source and write the target. The prefix is copied
                        server-side (UploadPartCopy) and only the new footer
                        is uploaded. Multipart parts other than the last must
                        be >= 5 MiB, so a smaller file is downloaded and
                        re-uploaded instead.

Scope: flat (non-nested) schemas and unencrypted Parquet. Anything else
raises rather than guessing.
"""

import struct
from urllib.parse import urlparse

MIN_PART_BYTES = 5 * 1024 * 1024
TAIL_PROBE_BYTES = 64 * 1024
READ_CHUNK_BYTES = 8 * 1024 * 1024

# ---------------------------------------------------------------------------
# Thrift compact protocol: a generic, order-preserving codec. Parquet's footer
# is a FileMetaData struct; we parse it into nested [field_id, type, value]
# lists, edit SchemaElement.field_id, and serialize it back. Fields we do not
# touch round-trip byte-for-byte (verified in tests/test_footer_relabel.py).
# ---------------------------------------------------------------------------

T_STOP, T_TRUE, T_FALSE, T_BYTE, T_I16, T_I32, T_I64, T_DOUBLE, T_BINARY, T_LIST, T_SET, T_MAP, T_STRUCT = range(13)


class _Reader:
    def __init__(self, data: bytes):
        self.data, self.pos = data, 0

    def byte(self) -> int:
        b = self.data[self.pos]
        self.pos += 1
        return b

    def varint(self) -> int:
        shift = result = 0
        while True:
            b = self.byte()
            result |= (b & 0x7F) << shift
            if not b & 0x80:
                return result
            shift += 7

    def zigzag(self) -> int:
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def struct(self) -> list:
        fields, last = [], 0
        while True:
            header = self.byte()
            if header == T_STOP:
                return fields
            ftype, delta = header & 0x0F, header >> 4
            fid = last + delta if delta else self.zigzag()
            last = fid
            fields.append([fid, ftype, None if ftype in (T_TRUE, T_FALSE) else self.value(ftype)])

    def value(self, t: int):
        if t == T_BYTE:
            return self.byte()
        if t in (T_I16, T_I32, T_I64):
            return self.zigzag()
        if t == T_DOUBLE:
            v = self.data[self.pos : self.pos + 8]
            self.pos += 8
            return v
        if t == T_BINARY:
            n = self.varint()
            v = self.data[self.pos : self.pos + n]
            self.pos += n
            return v
        if t in (T_LIST, T_SET):
            header = self.byte()
            size, etype = header >> 4, header & 0x0F
            if size == 15:
                size = self.varint()
            return (etype, [self.elem(etype) for _ in range(size)])
        if t == T_MAP:
            size = self.varint()
            if not size:
                return (0, 0, [])
            kv = self.byte()
            kt, vt = kv >> 4, kv & 0x0F
            return (kt, vt, [(self.elem(kt), self.elem(vt)) for _ in range(size)])
        if t == T_STRUCT:
            return self.struct()
        raise ValueError(f"unknown thrift compact type {t}")

    def elem(self, t: int):
        # Booleans inside collections are a whole byte, not folded into a header.
        return self.byte() if t in (T_TRUE, T_FALSE) else self.value(t)


class _Writer:
    def __init__(self):
        self.out = bytearray()

    def varint(self, n: int):
        while True:
            if n < 0x80:
                self.out.append(n)
                return
            self.out.append((n & 0x7F) | 0x80)
            n >>= 7

    def zigzag(self, n: int):
        self.varint((n << 1) ^ (n >> 63))

    def struct(self, fields: list):
        last = 0
        for fid, ftype, value in fields:
            delta = fid - last
            if 0 < delta <= 15:
                self.out.append((delta << 4) | ftype)
            else:
                self.out.append(ftype)
                self.zigzag(fid)
            last = fid
            if ftype not in (T_TRUE, T_FALSE):
                self.value(ftype, value)
        self.out.append(T_STOP)

    def value(self, t: int, v):
        if t == T_BYTE:
            self.out.append(v)
        elif t in (T_I16, T_I32, T_I64):
            self.zigzag(v)
        elif t == T_DOUBLE:
            self.out += v
        elif t == T_BINARY:
            self.varint(len(v))
            self.out += v
        elif t in (T_LIST, T_SET):
            etype, items = v
            if len(items) < 15:
                self.out.append((len(items) << 4) | etype)
            else:
                self.out.append(0xF0 | etype)
                self.varint(len(items))
            for item in items:
                self.elem(etype, item)
        elif t == T_MAP:
            kt, vt, pairs = v
            self.varint(len(pairs))
            if pairs:
                self.out.append((kt << 4) | vt)
                for k, val in pairs:
                    self.elem(kt, k)
                    self.elem(vt, val)
        elif t == T_STRUCT:
            self.struct(v)
        else:
            raise ValueError(f"unknown thrift compact type {t}")

    def elem(self, t: int, v):
        if t in (T_TRUE, T_FALSE):
            self.out.append(v)
        else:
            self.value(t, v)


def decode_footer(footer: bytes) -> list:
    r = _Reader(footer)
    fmd = r.struct()
    if r.pos != len(footer):
        raise ValueError(f"footer has {len(footer) - r.pos} trailing byte(s) after FileMetaData")
    return fmd


def encode_footer(fmd: list) -> bytes:
    w = _Writer()
    w.struct(fmd)
    return bytes(w.out)


# FileMetaData field 2 = schema: list<SchemaElement>.
# SchemaElement: 4 = name, 5 = num_children, 9 = field_id.
FMD_SCHEMA, SE_NAME, SE_NUM_CHILDREN, SE_FIELD_ID = 2, 4, 5, 9


def _get(fields: list, fid: int):
    return next((f for f in fields if f[0] == fid), None)


def footer_field_ids(footer: bytes) -> dict:
    """{field_id: column name} for the top-level columns a footer declares."""
    _, elements = _get(decode_footer(footer), FMD_SCHEMA)[2]
    out = {}
    for se in elements[1:]:
        fid = _get(se, SE_FIELD_ID)
        out[fid[2] if fid else None] = _get(se, SE_NAME)[2].decode()
    return out


def relabel_footer(footer: bytes, id_map: dict) -> bytes:
    """Return `footer` with every top-level column's field id replaced via
    id_map ({source field id: target field id}). Everything else is untouched.

    Raises on anything outside the spike's scope rather than guessing: nested
    columns (the leaf ids would need path-aware mapping) and columns with no
    id or an id the map does not cover.
    """
    fmd = decode_footer(footer)
    _, elements = _get(fmd, FMD_SCHEMA)[2]
    root_children = _get(elements[0], SE_NUM_CHILDREN)[2]
    if root_children != len(elements) - 1:
        raise NotImplementedError("nested Parquet schema; only flat schemas are supported")
    for se in elements[1:]:
        fid = _get(se, SE_FIELD_ID)
        name = _get(se, SE_NAME)[2].decode()
        if fid is None or fid[2] not in id_map:
            raise KeyError(f"column {name!r} has field id {fid and fid[2]} with no target mapping")
        fid[2] = id_map[fid[2]]
    return encode_footer(fmd)


def split_parquet(data: bytes) -> tuple:
    """(prefix, footer) for an in-memory Parquet file."""
    if data[:4] != b"PAR1" or data[-4:] != b"PAR1":
        raise ValueError("not a plaintext Parquet file (encrypted files end in PARE)")
    n = struct.unpack("<I", data[-8:-4])[0]
    return data[: len(data) - 8 - n], data[len(data) - 8 - n : -8]


def assemble_parquet(prefix: bytes, footer: bytes) -> bytes:
    return prefix + footer + struct.pack("<I", len(footer)) + b"PAR1"


def relabel_bytes(data: bytes, id_map: dict) -> bytes:
    prefix, footer = split_parquet(data)
    return assemble_parquet(prefix, relabel_footer(footer, id_map))


# ---------------------------------------------------------------------------
# S3: relabel one object into another without downloading its data pages.
# ---------------------------------------------------------------------------


def _bucket_key(uri: str) -> tuple:
    u = urlparse(uri)
    return u.netloc, u.path.lstrip("/")


def relabel_s3_object(s3, src_uri: str, dst_uri: str, id_map: dict) -> int:
    """Write a relabeled copy of src_uri to dst_uri. Returns the new size."""
    sb, sk = _bucket_key(src_uri)
    db, dk = _bucket_key(dst_uri)

    tail = s3.get_object(Bucket=sb, Key=sk, Range=f"bytes=-{TAIL_PROBE_BYTES}")
    size = int(tail["ContentRange"].rsplit("/", 1)[1])
    tail = tail["Body"].read()
    if tail[-4:] != b"PAR1":
        raise ValueError(f"{src_uri}: not a plaintext Parquet file")
    footer_len = struct.unpack("<I", tail[-8:-4])[0]
    if footer_len + 8 > len(tail):
        tail = s3.get_object(Bucket=sb, Key=sk, Range=f"bytes=-{footer_len + 8}")["Body"].read()
    footer = tail[-8 - footer_len : -8]
    footer_start = size - 8 - footer_len

    new_footer = relabel_footer(footer, id_map)
    new_tail = new_footer + struct.pack("<I", len(new_footer)) + b"PAR1"

    if footer_start < MIN_PART_BYTES:
        prefix = s3.get_object(Bucket=sb, Key=sk, Range=f"bytes=0-{footer_start - 1}")["Body"].read()
        s3.put_object(Bucket=db, Key=dk, Body=prefix + new_tail)
    else:
        mpu = s3.create_multipart_upload(Bucket=db, Key=dk)
        try:
            p1 = s3.upload_part_copy(
                Bucket=db, Key=dk, UploadId=mpu["UploadId"], PartNumber=1,
                CopySource={"Bucket": sb, "Key": sk}, CopySourceRange=f"bytes=0-{footer_start - 1}",
            )
            p2 = s3.upload_part(Bucket=db, Key=dk, UploadId=mpu["UploadId"], PartNumber=2, Body=new_tail)
            s3.complete_multipart_upload(
                Bucket=db, Key=dk, UploadId=mpu["UploadId"],
                MultipartUpload={"Parts": [
                    {"PartNumber": 1, "ETag": p1["CopyPartResult"]["ETag"]},
                    {"PartNumber": 2, "ETag": p2["ETag"]},
                ]},
            )
        except Exception:
            s3.abort_multipart_upload(Bucket=db, Key=dk, UploadId=mpu["UploadId"])
            raise
    return footer_start + len(new_tail)


def relabel_via_fileio(src_io, dst_io, src_path: str, dst_path: str, id_map: dict) -> int:
    """Write a relabeled copy of src_path to dst_path through two pyiceberg
    FileIOs, so each side uses its own table's credential. Returns the new
    size. Holds one file in memory.

    Reads in ranged chunks rather than one read(): against a real MDLS lake,
    a single whole-object read of a ~21 MiB writer-compacted file failed the
    AWS SDK's response-checksum validation ("Response checksums mismatch"),
    while ranged reads of the same object -- which is how Iceberg readers
    always read -- succeed."""
    chunks = []
    with src_io.new_input(src_path).open() as f:
        while chunk := f.read(READ_CHUNK_BYTES):
            chunks.append(chunk)
    out = relabel_bytes(b"".join(chunks), id_map)
    with dst_io.new_output(dst_path).create(overwrite=True) as f:
        f.write(out)
    return len(out)
