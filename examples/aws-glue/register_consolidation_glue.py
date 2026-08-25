#!/usr/bin/env python3
"""register_consolidation_glue.py -- no-rewrite Iceberg table consolidation
for a customer-managed AWS Glue Data Catalog, adapted from
register_consolidation.py (the Fivetran-managed Polaris/MDLS original in
this repo's scripts/ directory).

WHAT CHANGED FROM THE POLARIS ORIGINAL, AND WHY: exactly one thing --
how the catalog connection is constructed (this file's catalog_properties()
and _make_catalog(), below). Everything else -- the reserve/splice/detect
collisions/rewrite/retire-orphaned-files mechanism, the whole
register_table() function, file_has_drift_beyond(), already_registered_by_source(),
the collision-resolution logic -- is copied verbatim, unmodified, from the
validated Polaris version. That logic operates entirely on pyiceberg
Table/Schema/Transaction objects once a table is loaded, which is
catalog-agnostic: pyiceberg's GlueCatalog and RestCatalog both implement the
same abstract Catalog interface (confirmed directly against the installed
library: both subclass pyiceberg.catalog.MetastoreCatalog/Catalog, and
create_table/load_table/list_tables/list_namespaces/create_namespace are
all present on both). See docs/HOW_IT_WORKS.md and CHANGELOG.md in the
repo root for the full validation history of that shared logic -- this file
does not re-litigate any of it.

THE ONE REAL DIFFERENCE THAT MATTERS: credentials. Polaris VENDS short-lived,
per-table-scoped AWS storage credentials automatically on every table load
-- this script's Polaris counterpart deliberately clears any ambient AWS
credentials from the environment to force that vended-only path. AWS Glue
Data Catalog does not vend scoped credentials at all. The IAM principal
(role, user, or instance profile) running this script needs its OWN
standing AWS credentials with:
  - Glue permissions across every source database this tool will read
    (glue:GetDatabase, glue:GetTable, glue:GetTables, glue:GetPartitions)
    and create/alter permissions on the target database/tables
    (glue:CreateDatabase, glue:CreateTable, glue:UpdateTable).
  - S3 permissions across every source table's data location (read) and
    the target table's data location (read + write). Because there is no
    per-table credential vending under Glue, this is necessarily a BROADER
    IAM footprint than the Polaris version needs -- size the IAM policy
    accordingly, and consider a bucket-policy or prefix-scoped approach if
    your sources span more S3 prefixes than you want one role to reach.
This is a real trade-off worth being explicit about, not just a footnote:
Polaris's vended-credential model is narrower by construction (each table
load gets exactly the access that one table needs); a standing IAM
role/user is not, and the security review for a Glue deployment should
reflect that.

WHAT'S PROVEN VERSUS WHAT'S ADAPTED BUT NOT YET RE-VALIDATED: the core
mechanism (reserve/splice/detect-collisions/rewrite/retire-orphaned-files)
was validated against a live Fivetran-managed Polaris catalog -- see this
repo's CHANGELOG.md and docs/HOW_IT_WORKS.md for exactly what was tested,
including real reproductions of the copy-on-write duplication bug and its
fix. The catalog swap in THIS file (Glue instead of Polaris) has NOT yet
been run against a live AWS Glue Data Catalog + real source tables as of
this writing -- it's a direct, mechanical adaptation based on pyiceberg's
shared Catalog interface, not a live-tested one. Treat this file as a
starting point to validate in a sandbox/dev Glue catalog before pointing it
at anything you depend on, the same way the Polaris original was validated
before this repo's 1.0.0 release. In particular, the reserved-field-id
trick in create_target_table() (an itertools.count assigned to a private
pyiceberg UpdateSchema attribute, un-doc'd upstream -- see
docs/HOW_IT_WORKS.md) is pyiceberg-library-internal and SHOULD behave
identically regardless of catalog backend, but that's an expectation
carried over from the Polaris testing, not something independently
re-confirmed against Glue.

WHAT THIS DOES NOT HANDLE: identical list to the Polaris original -- see
this repo's README.md "What this does NOT yet handle" section. Renamed
columns, dropped columns, and changed data types carry the same caveats
documented there (including which rename scenario is fixed and which one
silently goes stale -- that fix is catalog-agnostic pyiceberg-library code,
so it applies here unchanged).
"""
import argparse
import fnmatch
import itertools
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import yaml
from dotenv import load_dotenv
from pyiceberg.catalog.glue import GlueCatalog
from pyiceberg.manifest import DataFile, DataFileContent
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import StringType

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("register_consolidation_glue")

# NOTE: unlike the Polaris original, nothing clears ambient AWS credentials
# here. Glue Data Catalog has no credential-vending mechanism to force -- this
# script is MEANT to run with real, standing AWS credentials in scope,
# whether that's an attached IAM role (EC2/ECS/Lambda instance profile,
# recommended), a named profile (GLUE_PROFILE_NAME / AWS_PROFILE), or
# explicit keys (GLUE_ACCESS_KEY_ID/GLUE_SECRET_ACCESS_KEY/GLUE_SESSION_TOKEN
# or their AWS_* equivalents) via catalog_properties() below. See this
# file's module docstring for the IAM permissions this needs.

# The consolidated table's own bookkeeping column (config: source_id_column)
# is deliberately assigned a field id from this permanently out-of-band
# range, instead of "whatever the next free id happens to be." No realistic
# source schema grows anywhere near this range, so it structurally cannot
# collide with a source's own future column additions. See
# create_target_table() and docs/HOW_IT_WORKS.md for why this requires a
# two-step create-then-widen dance rather than being set at creation time.
RESERVED_FIELD_ID_BASE = 100000

_thread_local = threading.local()


# --------------------------------------------------------------------------
# Config and catalog setup
# --------------------------------------------------------------------------


def load_config(path: str) -> dict:
    """Load and validate config.yaml (see config.example.yaml in this
    directory). Identical to the Polaris original -- config shape (target
    namespace/database, source namespaces/databases, source_id_column,
    worker counts) is catalog-agnostic."""
    try:
        with open(path) as f:
            cfg = yaml.safe_load(f) or {}
    except FileNotFoundError:
        raise SystemExit(
            f"Config file '{path}' not found. Copy config.example.yaml to "
            f"{path} and fill in your source databases."
        )

    required = ["target_namespace", "source_id_column"]
    missing = [k for k in required if not cfg.get(k)]
    if missing:
        raise SystemExit(f"{path} is missing required key(s): {missing}")

    has_list = bool(cfg.get("source_namespaces"))
    has_pattern = bool(cfg.get("source_namespace_pattern"))
    if has_list and has_pattern:
        raise SystemExit(
            f"{path}: set only ONE of 'source_namespaces' or 'source_namespace_pattern', not both "
            "-- ambiguous which one should win."
        )
    if not has_list and not has_pattern:
        raise SystemExit(
            f"{path} is missing required key(s): must set either 'source_namespaces' "
            "(an explicit list) or 'source_namespace_pattern' (a glob, e.g. 'tenant_*')."
        )
    if has_list and (not isinstance(cfg["source_namespaces"], list) or len(cfg["source_namespaces"]) < 2):
        raise SystemExit(
            f"{path}: 'source_namespaces' must be a list of 2 or more namespaces "
            "(consolidating a single namespace into itself isn't a meaningful use of this tool)."
        )

    cfg.setdefault("table_workers", 8)
    cfg.setdefault("source_workers", 8)
    return cfg


def resolve_source_namespaces(cat: "GlueCatalog", cfg: dict) -> list:
    """Return the concrete list of source namespaces (Glue databases) to
    consolidate. Identical logic to the Polaris original -- glue databases
    are exactly what pyiceberg's list_namespaces()/GlueCatalog surfaces as
    namespaces, so the glob-pattern resolution path works unchanged."""
    if cfg.get("source_namespaces"):
        return list(cfg["source_namespaces"])

    pattern = cfg["source_namespace_pattern"]
    all_namespaces = [".".join(ns) for ns in cat.list_namespaces()]
    matched = sorted(
        ns for ns in all_namespaces if fnmatch.fnmatch(ns, pattern) and ns != cfg["target_namespace"]
    )
    logger.info(
        "source_namespace_pattern '%s' matched %d namespace(s) in the Glue catalog: %s",
        pattern,
        len(matched),
        matched,
    )
    if len(matched) < 2:
        raise SystemExit(
            f"source_namespace_pattern '{pattern}' matched only {len(matched)} namespace(s): {matched}. "
            "Need 2 or more to consolidate. Check the pattern against your Glue catalog's actual database "
            "names (list_namespaces() is case-sensitive, exact-match-per-segment glob, not SQL LIKE)."
        )
    return matched


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise SystemExit(
            f"Missing required environment variable: {name}. "
            "Copy .env.example to .env and fill it in."
        )
    return val


def catalog_properties() -> dict:
    """Build the GlueCatalog property dict.

    Unlike the Polaris version, there's no OAuth handshake and no vended
    credentials -- pyiceberg's GlueCatalog builds a boto3 Session directly
    from these properties (confirmed by reading the installed pyiceberg
    0.11.1 source: GlueCatalog.__init__ constructs
    boto3.Session(profile_name=..., region_name=..., aws_access_key_id=...,
    aws_secret_access_key=..., aws_session_token=...) and falls back to
    boto3's own default credential chain -- env vars, shared config/profile,
    or an attached instance role -- for any property left unset here).

    'warehouse' is the S3 path new tables get created under (e.g.
    s3://your-bucket/warehouse/) -- required by pyiceberg's base Catalog
    class the same way it is for Polaris, just pointing at your own bucket
    instead of one Polaris manages.

    GLUE_ID is optional and only needed for cross-account Glue catalogs
    (AWS's "Glue Catalog ID," typically a 12-digit account id, when the
    catalog you're registering into isn't in the same AWS account this
    script's credentials belong to).
    """
    props = {
        "warehouse": _require_env("GLUE_WAREHOUSE_PATH"),
    }
    region = os.environ.get("AWS_REGION")
    if region:
        props["region_name"] = region
    profile = os.environ.get("AWS_PROFILE")
    if profile:
        props["profile_name"] = profile
    # Only set explicit keys if provided -- omitting these entirely (the
    # common case: run this from something that already has an IAM role,
    # e.g. EC2/ECS/Lambda/CodeBuild) lets boto3's own default credential
    # chain do the right thing without this script needing to know how.
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        props["aws_access_key_id"] = os.environ["AWS_ACCESS_KEY_ID"]
    if os.environ.get("AWS_SECRET_ACCESS_KEY"):
        props["aws_secret_access_key"] = os.environ["AWS_SECRET_ACCESS_KEY"]
    if os.environ.get("AWS_SESSION_TOKEN"):
        props["aws_session_token"] = os.environ["AWS_SESSION_TOKEN"]
    if os.environ.get("GLUE_CATALOG_ID"):
        props["glue.id"] = os.environ["GLUE_CATALOG_ID"]
    return props


def _make_catalog() -> GlueCatalog:
    return GlueCatalog("fivetran_mdls_glue", **catalog_properties())


def get_catalog() -> GlueCatalog:
    """Serial-path catalog instance, used by the top-level driver and by
    anything running outside a worker thread."""
    return _make_catalog()


def thread_catalog() -> GlueCatalog:
    """One GlueCatalog per worker thread, created lazily on first use in
    that thread and reused for the rest of that thread's work -- never
    shared across threads. pyiceberg's GlueCatalog wraps a boto3 Glue
    client, and boto3 clients are documented as thread-safe for read
    operations but this project follows the same conservative,
    one-catalog-per-thread pattern used for the Polaris RestCatalog rather
    than relying on that. The one-time client construction cost per thread
    is trivial next to the per-call network latency this parallelism is
    trying to hide in the first place."""
    cat = getattr(_thread_local, "cat", None)
    if cat is None:
        cat = _make_catalog()
        _thread_local.cat = cat
    return cat


# --------------------------------------------------------------------------
# Source discovery
# --------------------------------------------------------------------------


def discover_common_tables(cat: GlueCatalog, source_namespaces: list) -> list:
    """Auto-discover which table names exist in EVERY listed source
    namespace, so the customer never has to hand-enumerate tables.

    This replaces the internal reference implementation's discovery, which
    only read the first source's namespace and assumed every other source
    had exactly the same tables. A real customer's namespaces aren't
    guaranteed to be perfectly uniform (a tenant's connector might be newer,
    or missing a table another tenant has), so this takes the intersection
    across ALL sources instead, and warns loudly about anything skipped.
    """
    table_sets = []
    for ns in source_namespaces:
        try:
            names = {identifier[-1] for identifier in cat.list_tables(ns)}
        except Exception as e:
            raise SystemExit(f"Could not list tables in source namespace '{ns}': {e}")
        if not names:
            logger.warning("Source namespace '%s' has no tables.", ns)
        table_sets.append(names)

    common = set.intersection(*table_sets) if table_sets else set()
    all_seen = set.union(*table_sets) if table_sets else set()
    skipped = sorted(all_seen - common)
    if skipped:
        logger.warning(
            "%d table name(s) exist in SOME but not ALL source namespaces and will "
            "be SKIPPED (not consolidated), since this tool only auto-discovers "
            "tables common to every configured source: %s",
            len(skipped),
            skipped,
        )
    return sorted(common)


def ensure_namespace(cat: GlueCatalog, ns: str) -> None:
    try:
        cat.create_namespace(ns)
    except Exception as e:
        if "already exists" not in str(e).lower() and "AlreadyExists" not in type(e).__name__:
            raise


# --------------------------------------------------------------------------
# Correctness logic: reserve / widen / detect drift / resolve collision
#
# Everything below is copied verbatim from the validated Polaris original
# (register_consolidation.py) -- this logic is pyiceberg-library-level and
# catalog-agnostic, operating entirely on Table/Schema/Transaction objects
# once a table is loaded, regardless of which Catalog implementation loaded
# it. Unlike the Polaris version, there is no per-table credential vending
# here -- GlueCatalog's Table objects use whichever standing AWS credentials
# this process resolved at startup (see catalog_properties() above) for
# every table's FileIO, not a freshly-scoped credential per load. That's a
# difference in HOW access is granted, not in this section's logic itself.
# --------------------------------------------------------------------------


def create_target_table(cat: GlueCatalog, target_id: str, source_schema: Schema, source_id_column: str):
    """Create the target table, then reserve a permanently out-of-band field
    id for the bookkeeping source-id column in a SEPARATE schema-evolution
    call made after the table exists.

    This two-step dance is required, not stylistic, against Polaris --
    confirmed by inspecting the literal request payload against a real
    Polaris catalog: the catalog server renumbers all field ids on create
    regardless of what the client requests, and only a schema-evolution
    call made AFTER the table already exists is honored as sent. That
    specific server-side renumbering behavior has NOT been independently
    re-confirmed against AWS Glue's own CreateTable handling as of this
    writing -- Glue's field-id assignment on create could plausibly differ
    from Polaris's. Kept this two-step dance here regardless, since it's a
    strict superset of correct behavior (it still produces the right result
    if Glue happens to honor a requested field id on create; it's simply
    unverified whether the first step is still strictly necessary on Glue
    specifically). Confirm this assumption against your own Glue catalog
    before relying on it -- see docs/HOW_IT_WORKS.md in the repo root for
    the full explanation of the mechanism, including the private pyiceberg
    attribute (`_last_column_id`) this currently depends on.
    """
    target_table = cat.create_table(target_id, schema=Schema(*source_schema.fields, schema_id=0))
    with target_table.update_schema() as us:
        us._last_column_id = itertools.count(RESERVED_FIELD_ID_BASE)
        us.add_column(source_id_column, StringType())
    target_table = cat.load_table(target_id)
    with target_table.update_spec() as spec:
        spec.add_identity(source_id_column)
    return cat.load_table(target_id)


def file_has_drift_beyond(f, known_field_ids: set):
    """Return the field id of genuine schema drift in this file, or None.

    Compares null_value_counts (per field id) against record_count, not just
    whether a field id appears as a key in the file's stats at all. Iceberg
    backfills a stats entry for every field in a table's CURRENT schema onto
    every file, including files written before that field existed, so key
    presence alone produces false positives on old files. A field id the
    TARGET doesn't already know about, with fewer nulls than the file has
    records, means that file genuinely carries non-null data for a column
    the target's registered schema didn't have.

    known_field_ids MUST be sourced from the TARGET table's own current
    schema, never from any one source's schema. Field ids are assigned
    independently per source table in this project's design -- that's
    exactly why the collision-handling logic in register_table() exists at
    all (two sources landing on the same field id with different meanings) --
    so a source's own field-id numbering can't be trusted as "the baseline"
    either. This function used to take an integer threshold
    (baseline_max_field_id) borrowed from source_namespaces[0]'s live schema
    instead of a set from the target: any field id already present in that
    ONE source's current schema silently counted as "already known," even if
    that source had only just gained it moments earlier in the very sync
    being consolidated. See CHANGELOG.md's "Fixed: a source's own schema
    evolution could vanish into a stale baseline" entry for the live
    reproduction (a new column added by source_namespaces[0] itself never
    got detected as drift, so it never got widened into the target -- not a
    one-time miss, either: since the threshold was re-derived from that same
    source's now-already-evolved schema on every subsequent run, it stayed
    permanently invisible).
    """
    if not f.null_value_counts:
        return None
    for field_id, null_count in f.null_value_counts.items():
        if field_id not in known_field_ids and null_count < f.record_count:
            return field_id
    return None


def already_registered_by_source(target_table) -> dict:
    """Every file currently in the target table's manifest, grouped by which
    source namespace it was spliced in from (via the identity partition on
    source_id_column -- f.partition[0] is that source's namespace string).

    Two things this is used for:
      1. The original idempotency check: skip any file whose path is already
         registered, so re-running this script with nothing new from any
         source is a safe no-op instead of double-registering every file
         (the bug an early, non-idempotent version of this technique had: a
         rerun came back with every source's row count exactly doubled).
      2. NEW: orphan detection. Fivetran's Managed Data Lake writer is
         copy-on-write for UPDATE and DELETE (confirmed by reading
         ManagedDataLakeWriter.java: upsert()/update()/delete() all rewrite
         whichever existing physical file(s) could contain the affected
         primary key(s) into brand-new file(s), then atomically swap old for
         new via Iceberg's OverwriteFiles -- deleteFile() + addFile() in one
         transaction). That means a source file this tool already spliced in
         can simply stop existing in the source's CURRENT snapshot on a
         later sync, replaced by a new file with the updated content. This
         tool used to have no way to notice that -- it only ever asked "is
         this source file new to me, splice it in," never "did a file I
         already spliced in get retired at the source." The result: the
         stale old file stays in the target forever, sitting right alongside
         its replacement, so every row that file held becomes a permanent
         duplicate -- and for any row that was genuinely updated, the target
         holds both the stale AND current values simultaneously with no way
         to tell which is which. Reproduced live against real Fivetran
         infrastructure before this fix existed (see CHANGELOG.md).

         Returning DataFile objects here (not just path strings) is what
         lets the caller pass an orphaned entry straight to
         _OverwriteFiles.delete_data_file() -- that call needs the actual
         DataFile, not its path.
    """
    by_source = {}
    try:
        for task in target_table.scan().plan_files():
            f = task.file
            src = f.partition[0]
            by_source.setdefault(src, {})[f.file_path] = f
    except Exception:
        pass
    return by_source


def _load_source_files(source_namespace: str, table_name: str):
    """Runs inside the source-level thread pool -- one (load_table +
    plan_files) round trip per source, using this thread's own catalog."""
    cat = thread_catalog()
    src_table = cat.load_table(f"{source_namespace}.{table_name}")
    src_schema = src_table.schema()
    files = list(src_table.scan().plan_files())
    return source_namespace, src_schema, files


def register_table(table_name: str, cfg: dict) -> tuple:
    """Runs inside the table-level thread pool (or serially if called
    directly) -- one call registers one target table end to end, using its
    own catalog via thread_catalog(), so this is safe to call concurrently
    for different table_name values from different threads."""
    source_namespaces = cfg["source_namespaces"]
    target_namespace = cfg["target_namespace"]
    source_id_column = cfg["source_id_column"]
    source_workers = cfg["source_workers"]

    cat = thread_catalog()
    t_start = time.time()
    logger.info("=== %s ===", table_name)

    src0 = cat.load_table(f"{source_namespaces[0]}.{table_name}")
    source_schema = src0.schema()

    target_id = f"{target_namespace}.{table_name}"
    ensure_namespace(cat, target_namespace)
    if cat.table_exists(target_id):
        target_table = cat.load_table(target_id)
        logger.info("  [%s] target table already exists, reusing", table_name)
    else:
        target_table = create_target_table(cat, target_id, source_schema, source_id_column)
        actual_id = target_table.schema().find_field(source_id_column).field_id
        logger.info("  [%s] created target table, %s field_id=%d", table_name, source_id_column, actual_id)
        assert actual_id == RESERVED_FIELD_ID_BASE

    # The reference frame for "has any source drifted?" MUST be the target's
    # OWN current schema, not source_namespaces[0]'s -- see
    # file_has_drift_beyond()'s docstring for why borrowing one source's
    # live schema as a stand-in baseline silently swallows that same
    # source's own future schema changes.
    known_field_ids = {f.field_id for f in target_table.schema().fields}

    already_by_source = already_registered_by_source(target_table)
    already = {p for paths in already_by_source.values() for p in paths}

    # 1. Detect every source's drifted files, in parallel across sources.
    files_by_source = {}
    with ThreadPoolExecutor(max_workers=min(source_workers, len(source_namespaces))) as ex:
        futs = {ex.submit(_load_source_files, ns, table_name): ns for ns in source_namespaces}
        for fut in as_completed(futs):
            ns, src_schema, files = fut.result()
            files_by_source[ns] = (src_schema, files)

    drift_by_field_id = {}
    for ns, (src_schema, files) in files_by_source.items():
        for task in files:
            f = task.file
            drift_field = file_has_drift_beyond(f, known_field_ids)
            if drift_field is not None:
                field = src_schema.find_field(drift_field)
                drift_by_field_id.setdefault(drift_field, [])
                entry = (ns, field.name, field.field_type)
                if entry not in drift_by_field_id[drift_field]:
                    drift_by_field_id[drift_field].append(entry)

    # 2. Resolve each drifted field id: first distinct name seen owns the
    #    physical id (no-rewrite widen); any OTHER distinct name sharing
    #    that same physical id is a genuine collision -> fresh id + real
    #    rewrite for just its drifted rows.
    rewrite_needed = []
    for field_id, entries in drift_by_field_id.items():
        distinct_names = {}
        for ns, name, ftype in entries:
            distinct_names.setdefault(name, []).append(ns)
        names_in_order = list(distinct_names.keys())
        owner_name = names_in_order[0]
        owner_type = next(ft for s, n, ft in entries if n == owner_name)
        already_field_names = {f.name: f.field_id for f in target_table.schema().fields}
        if owner_name not in already_field_names:
            logger.info(
                "  [%s] widening (no-rewrite): '%s' at physical field_id=%d", table_name, owner_name, field_id
            )
            with target_table.update_schema() as us:
                us._last_column_id = itertools.count(field_id)
                us.add_column(owner_name, owner_type)
            target_table = cat.load_table(target_id)
        for other_name in names_in_order[1:]:
            for ns in distinct_names[other_name]:
                other_type = next(ft for s, n, ft in entries if n == other_name and s == ns)
                rewrite_needed.append((ns, field_id, other_name, other_type))
                logger.warning(
                    "  [%s] COLLISION at field_id=%d: '%s' already owns it, '%s'.'%s' needs a "
                    "fresh field id + rewrite",
                    table_name,
                    field_id,
                    owner_name,
                    ns,
                    other_name,
                )

    # 3. No-rewrite pass: splice in every file whose drift (if any) resolves
    #    to the id's registered owner name. Files belonging to a source that
    #    needs a rewrite for this field id are excluded here (handled in
    #    step 4) so we never splice in a file whose embedded field id
    #    doesn't match what we've told the target schema that id means.
    needs_rewrite_sources_by_field = {}
    for ns, field_id, name, ftype in rewrite_needed:
        needs_rewrite_sources_by_field.setdefault(field_id, set()).add(ns)

    new_files = []
    orphaned_files = []  # DataFile objects to retire: previously spliced in
                         # from a source, but no longer part of that
                         # source's CURRENT file listing (copy-on-write swap)
    total_new_rows = 0
    total_orphaned_rows = 0
    for ns in source_namespaces:
        _, files = files_by_source[ns]
        current_paths_this_source = {task.file.file_path for task in files}
        registered_this_source = already_by_source.get(ns, {})

        files_ok, rows_this_source, files_skipped_already, files_skipped_rewrite = 0, 0, 0, 0
        for task in files:
            f = task.file
            if f.file_path in already:
                files_skipped_already += 1
                continue
            drift_field = file_has_drift_beyond(f, known_field_ids)
            if drift_field is not None and ns in needs_rewrite_sources_by_field.get(drift_field, set()):
                files_skipped_rewrite += 1
                continue
            new_file = DataFile.from_args(
                content=DataFileContent.DATA,
                file_path=f.file_path,
                file_format=f.file_format,
                partition=Record(ns),
                record_count=f.record_count,
                file_size_in_bytes=f.file_size_in_bytes,
                column_sizes=f.column_sizes,
                value_counts=f.value_counts,
                null_value_counts=f.null_value_counts,
                nan_value_counts=f.nan_value_counts,
                lower_bounds=f.lower_bounds,
                upper_bounds=f.upper_bounds,
                spec_id=target_table.spec().spec_id,
            )
            new_files.append(new_file)
            files_ok += 1
            rows_this_source += f.record_count
        total_new_rows += rows_this_source

        # Orphan check: any path this tool already spliced in for this
        # source, that ISN'T in the source's current file listing anymore,
        # was retired at the source by a copy-on-write rewrite and needs to
        # be retired here too -- otherwise its rows sit stale in the target
        # forever, duplicated against whatever replacement file(s) just got
        # spliced in above.
        orphaned_this_source = [
            df for path, df in registered_this_source.items() if path not in current_paths_this_source
        ]
        orphaned_rows_this_source = sum(df.record_count for df in orphaned_this_source)
        orphaned_files.extend(orphaned_this_source)
        total_orphaned_rows += orphaned_rows_this_source

        note = []
        if files_skipped_already:
            note.append(f"{files_skipped_already} already registered")
        if files_skipped_rewrite:
            note.append(f"{files_skipped_rewrite} pending physical rewrite")
        if orphaned_this_source:
            note.append(f"{len(orphaned_this_source)} orphaned file(s) retired ({orphaned_rows_this_source} stale rows)")
        flag = f" ({', '.join(note)})" if note else ""
        logger.info("  [%s] %s: %d file(s) spliced in, %d rows%s", table_name, ns, files_ok, rows_this_source, flag)

    if orphaned_files:
        # A retirement is present -- must commit via an OVERWRITE-type
        # transaction (delete_data_file() + append_data_file() together, in
        # the same commit) rather than a pure fast-append. Under the hood
        # this only rewrites the small Avro MANIFEST file(s) that mixed a
        # retired entry in with other still-valid ones -- never the
        # underlying Parquet data files -- so it stays a metadata-only
        # operation, same as the pure-splice path, just with a bit more
        # manifest bookkeeping. Only paid when an orphan is actually
        # detected; every other run (the common case) still uses the
        # cheaper pure-append path below, unchanged.
        with target_table.transaction() as txn:
            with txn.update_snapshot({}).overwrite() as ov:
                for df in orphaned_files:
                    ov.delete_data_file(df)
                for df in new_files:
                    ov.append_data_file(df)
        target_table = cat.load_table(target_id)
    elif new_files:
        with target_table.transaction() as txn:
            with txn._append_snapshot_producer({}) as append_files:
                for df in new_files:
                    append_files.append_data_file(df)
        target_table = cat.load_table(target_id)

    # 4. Rewrite pass: for every (source, field_id) needing a fresh id, read
    #    just the drifted rows, add the fresh column, append for real. This
    #    is the only place actual bytes get rewritten in this whole
    #    pipeline, and only for the small sliver of genuinely-colliding new
    #    data, never the historical bulk.
    rewritten_rows = 0
    for ns, field_id, name, ftype in rewrite_needed:
        if name not in {f.name for f in target_table.schema().fields}:
            with target_table.update_schema() as us:
                us.add_column(name, ftype)
            target_table = cat.load_table(target_id)
            fresh_id = target_table.schema().find_field(name).field_id
            logger.info("  [%s] rewrite: '%s' (from %s) assigned fresh field_id=%d", table_name, name, ns, fresh_id)

        src_table = cat.load_table(f"{ns}.{table_name}")
        _, files = files_by_source[ns]
        to_rewrite = [
            task.file
            for task in files
            if task.file.file_path not in already and file_has_drift_beyond(task.file, known_field_ids) == field_id
        ]
        if not to_rewrite:
            continue

        # Idempotency check for the rewrite path: these are real physical
        # writes, not manifest splices, so they can't be deduped by
        # file_path the way the no-rewrite path is. Check by row content
        # instead (does the target already have non-null values for this
        # source+field?).
        already_rewritten = target_table.scan(
            row_filter=f"{source_id_column} = '{ns}' and {name} is not null",
            selected_fields=(name,),
        ).to_arrow().num_rows
        expected_rows = sum(f.record_count for f in to_rewrite)
        if already_rewritten >= expected_rows:
            logger.info(
                "  [%s] '%s' from %s: %d row(s) already rewritten/appended in a prior run, skipping",
                table_name,
                name,
                ns,
                already_rewritten,
            )
            continue

        for f in to_rewrite:
            tab = src_table.scan(row_filter=f"{name} is not null").to_arrow()
            if tab.num_rows == 0:
                continue
            tab = tab.append_column(source_id_column, pa.array([ns] * tab.num_rows, type=pa.string()))
            target_table.append(tab)
            rewritten_rows += tab.num_rows
            logger.info("  [%s] rewrote + appended %d row(s) from %s (field '%s')", table_name, tab.num_rows, ns, name)
        target_table = cat.load_table(target_id)

    total = total_new_rows + rewritten_rows
    elapsed = time.time() - t_start
    logger.info(
        "  DONE %s: %d file(s) spliced (%d rows, no-rewrite), %d row(s) rewritten/appended, "
        "%d orphaned file(s) retired (%d stale rows removed) (%.1fs)",
        table_name,
        len(new_files),
        total_new_rows,
        rewritten_rows,
        len(orphaned_files),
        total_orphaned_rows,
        elapsed,
    )
    return table_name, total_new_rows, rewritten_rows, len(orphaned_files), total_orphaned_rows, elapsed


# --------------------------------------------------------------------------
# Driver
# --------------------------------------------------------------------------


def run(cfg: dict, tables: list = None) -> dict:
    """Run one full registration pass for the given config and return a
    JSON-serializable summary dict. This is the shared entry point behind
    both the CLI (main(), below) and any other caller that already has a
    cfg dict in hand -- e.g. examples/aws-lambda/lambda_function.py builds
    cfg directly from Lambda environment variables instead of a config.yaml
    file on disk, and calls this function directly rather than duplicating
    the discovery/threading logic.

    Raises SystemExit if there are no tables to register (mirrors main()'s
    prior behavior); callers that want a non-fatal empty-result instead
    should catch that themselves.
    """
    t_run_start = time.time()
    driver_cat = get_catalog()
    cfg["source_namespaces"] = resolve_source_namespaces(driver_cat, cfg)
    tables = tables or discover_common_tables(driver_cat, cfg["source_namespaces"])
    if not tables:
        raise SystemExit(
            "No tables to register (no table name is common to every configured source namespace)."
        )

    logger.info(
        "Registering %d table(s) across %d source namespace(s) into '%s', table_workers=%d, source_workers=%d",
        len(tables),
        len(cfg["source_namespaces"]),
        cfg["target_namespace"],
        cfg["table_workers"],
        cfg["source_workers"],
    )

    grand_spliced, grand_rewritten = 0, 0
    grand_orphaned_files, grand_orphaned_rows = 0, 0
    per_table_timing = []
    with ThreadPoolExecutor(max_workers=cfg["table_workers"]) as ex:
        futs = {ex.submit(register_table, t, cfg): t for t in tables}
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                table_name, spliced, rewritten, orphaned_files, orphaned_rows, elapsed = fut.result()
            except Exception:
                logger.exception("FAILED registering table '%s'", t)
                raise
            grand_spliced += spliced
            grand_rewritten += rewritten
            grand_orphaned_files += orphaned_files
            grand_orphaned_rows += orphaned_rows
            per_table_timing.append((table_name, elapsed))

    wall_clock = time.time() - t_run_start
    slowest = sorted(per_table_timing, key=lambda x: -x[1])[:5]
    return {
        "tables": len(tables),
        "sources": len(cfg["source_namespaces"]),
        "total_spliced": grand_spliced,
        "total_rewritten": grand_rewritten,
        "total_orphaned_files_retired": grand_orphaned_files,
        "total_orphaned_rows_removed": grand_orphaned_rows,
        "grand_total": grand_spliced + grand_rewritten,
        "wall_clock_seconds": round(wall_clock, 1),
        "slowest_tables": slowest,
    }


def main():
    parser = argparse.ArgumentParser(
        description="No-rewrite Iceberg table consolidation for a customer-managed AWS Glue Data Catalog."
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml (default: ./config.yaml)")
    parser.add_argument(
        "tables",
        nargs="*",
        help="Optional: restrict to these table names instead of auto-discovering common tables",
    )
    args = parser.parse_args()

    load_dotenv()  # loads .env into the process environment if present
    cfg = load_config(args.config)
    summary = run(cfg, tables=args.tables or None)

    print("\n=== SUMMARY ===")
    print(f"TABLES: {summary['tables']}  SOURCES: {summary['sources']}")
    print(f"TOTAL SPLICED (no-rewrite): {summary['total_spliced']}")
    print(f"TOTAL REWRITTEN (collision-only): {summary['total_rewritten']}")
    print(f"TOTAL ORPHANED FILES RETIRED (copy-on-write cleanup): {summary['total_orphaned_files_retired']}")
    print(f"TOTAL STALE ROWS REMOVED: {summary['total_orphaned_rows_removed']}")
    print(f"GRAND TOTAL: {summary['grand_total']}")
    print(f"WALL CLOCK: {summary['wall_clock_seconds']}s")
    if summary["total_orphaned_files_retired"] > 0:
        print(
            f"\nNOTE: {summary['total_orphaned_files_retired']} file(s) previously spliced in were retired this "
            "run because they no longer exist in their source's current snapshot (a copy-on-write update or "
            "delete rewrote them at the source). Retiring them only rewrites the small manifest metadata file "
            "that referenced them -- no Parquet data was read or rewritten -- but it does mean this run's "
            "commit was an OVERWRITE-type snapshot, not a pure append, for every table where this happened."
        )
    if summary["total_rewritten"] > 0:
        print(
            f"\nNOTE: {summary['total_rewritten']} row(s) went through the rewrite/collision path this run. "
            "That's expected on the first run after a genuine cross-source field-id collision, but "
            "if a specific source keeps landing on this path across repeated runs, its schema is "
            "drifting more than this tool's steady-state cost model assumes -- worth a closer look."
        )
    print(f"Slowest 5 tables (elapsed, includes queueing behind the thread pool): {summary['slowest_tables']}")


if __name__ == "__main__":
    main()
