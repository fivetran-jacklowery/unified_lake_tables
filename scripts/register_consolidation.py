#!/usr/bin/env python3
"""register_consolidation.py -- no-rewrite Iceberg table consolidation for a
Fivetran-managed Polaris catalog / Managed Data Lake Service (MDLS)
destination.

This is a v1.0 clean release of a technique validated internally against a
real Fivetran-managed destination, Polaris catalog, and downstream
Snowflake/DuckDB reads (see docs/HOW_IT_WORKS.md and this repo's README for
the full story, including what's proven and what isn't). It is adapted from
an internal reference implementation for two things a real customer needs
that an internal sandbox script didn't:

  1. Vended credentials instead of static AWS keys. This script never asks
     for an AWS access key or secret key. It authenticates to Polaris using
     OAuth client-credentials only (POLARIS_CLIENT_ID/POLARIS_CLIENT_SECRET),
     and Polaris vends short-lived, scoped AWS storage credentials
     automatically whenever a table is loaded through pyiceberg's
     RestCatalog -- see catalog_properties() below and "Credentials: vended,
     not static" in docs/HOW_IT_WORKS.md.
  2. Config-driven source discovery instead of a naming-convention hack.
     Either list your actual source namespaces in config.yaml explicitly,
     or -- at real scale, where hand-listing 400 namespaces defeats the
     point -- give a glob pattern (source_namespace_pattern, e.g.
     "tenant_*") and this script resolves it against the catalog's actual
     namespaces itself (see resolve_source_namespaces()). Either way, this
     script then auto-discovers which table NAMES exist in ALL of those
     namespaces (see discover_common_tables()) rather than requiring every
     table to be enumerated by hand.

WHAT THIS ACTUALLY DOES, in one paragraph: for every table name common to
all configured source namespaces, it creates (or reuses) one target table
per table name in a target namespace, and splices in every source's Parquet
data files directly via manifest/partition-value injection -- no data is
read or rewritten for the common case. It also detects the one real
correctness risk this technique introduces (two independently-evolving
sources landing a new column on the same internal Iceberg field id with
different meanings) and resolves it safely, at a real but delta-scoped
rewrite cost, only when that collision actually happens. It also detects
and retires files that Fivetran's copy-on-write writer has silently
replaced at a source since the last run (see
already_registered_by_source() and the orphan-retirement logic in
register_table()), so a source doing row-level UPDATEs or DELETEs doesn't
leave stale, duplicated data behind in the target. See docs/HOW_IT_WORKS.md
for the full mechanism, including measured overhead for the retirement
path.

WHAT THIS DOES NOT HANDLE (see the README before running this against
production data): renamed columns, dropped columns, and changed data types.
Only "a source adds a new nullable column" and "a source performs a
row-level UPDATE/DELETE handled via copy-on-write" have been validated.
Running this against a source doing anything else (a rename, a drop, a type
change) is unsupported and may produce incorrect results.
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
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.manifest import DataFile, DataFileContent
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import StringType

from multi_source_io import MultiSourceFileIO

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("register_consolidation")

# Deliberately clear any ambient AWS credentials/profile out of this
# process's environment before pyiceberg or pyarrow ever look at it. This
# tool is designed to run on Polaris-vended credentials only (see
# catalog_properties() below); if vending ever silently fails to return
# credentials for some reason, pyarrow's S3FileSystem falls back to its own
# default AWS credential discovery (env vars, shared config, instance
# role) rather than raising a clear error. Clearing these here means that
# failure mode surfaces as a loud, immediate auth error instead of quietly
# reading (or writing) through whatever ambient AWS identity happens to be
# sitting in the environment the script runs in.
for _var in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"):
    os.environ.pop(_var, None)

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
    """Load and validate config.yaml (see config.example.yaml)."""
    try:
        with open(path) as f:
            cfg = yaml.safe_load(f) or {}
    except FileNotFoundError:
        raise SystemExit(
            f"Config file '{path}' not found. Copy config.example.yaml to "
            f"{path} and fill in your source namespaces."
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


def resolve_source_namespaces(cat: "RestCatalog", cfg: dict) -> list:
    """Return the concrete list of source namespaces to consolidate.

    If cfg['source_namespaces'] is set, that explicit list is used as-is
    (unchanged behavior). If cfg['source_namespace_pattern'] is set instead,
    this auto-discovers every namespace that currently exists in the catalog
    and matches that glob pattern (fnmatch semantics: '*' / '?' / '[seq]'),
    rather than requiring every tenant/shard/region namespace to be
    hand-enumerated one at a time.

    This matters at the scale this tool is actually built for: asking
    someone to list 400 Azure SQL databases or 10,000 ERP tables by name in
    a config file defeats the point of a tool meant to handle exactly that
    scale. The mental model is deliberately similar to dbt-utils'
    get_relations_by_pattern -- discover what matches a schema pattern,
    rather than requiring an exhaustive explicit list.

    The target namespace is excluded from the match even if the pattern
    would otherwise catch it (e.g. a target named 'tenant_consolidated'
    under a 'tenant_*' pattern), since consolidating a target into itself
    isn't meaningful. Raises SystemExit if a pattern is configured but
    matches fewer than 2 namespaces, same validation load_config() already
    applies to an explicit list.

    Always logs the resolved list at INFO level -- auto-discovering which
    namespaces get spliced into a schema-modifying tool is exactly the kind
    of thing that should be visible and auditable, not a silent side effect
    of a glob that matched more (or less) than intended.
    """
    if cfg.get("source_namespaces"):
        return list(cfg["source_namespaces"])

    pattern = cfg["source_namespace_pattern"]
    all_namespaces = [".".join(ns) for ns in cat.list_namespaces()]
    matched = sorted(
        ns for ns in all_namespaces if fnmatch.fnmatch(ns, pattern) and ns != cfg["target_namespace"]
    )
    logger.info(
        "source_namespace_pattern '%s' matched %d namespace(s) in the catalog: %s",
        pattern,
        len(matched),
        matched,
    )
    if len(matched) < 2:
        raise SystemExit(
            f"source_namespace_pattern '{pattern}' matched only {len(matched)} namespace(s): {matched}. "
            "Need 2 or more to consolidate. Check the pattern against your catalog's actual namespace "
            "names (list_namespaces() is case-sensitive, exact-match-per-segment glob, not SQL LIKE)."
        )
    return matched


def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise SystemExit(
            f"Missing required environment variable: {name}. "
            "Copy .env.example to .env and fill it in (see that file for where "
            "to find each value in the Fivetran dashboard)."
        )
    return val


def catalog_properties() -> dict:
    """Build the RestCatalog property dict for Polaris OAuth
    client-credentials auth.

    Deliberately builds NO s3.* / AWS properties here. pyiceberg's
    RestCatalog requests vended storage credentials from the catalog server
    by default on every table load (it sends the
    'X-Iceberg-Access-Delegation: vended-credentials' header automatically
    -- confirmed in pyiceberg 0.11.1's catalog/rest/__init__.py, no extra
    client-side config needed to opt in). When Polaris returns those
    credentials, pyiceberg attaches them to the loaded Table's FileIO
    automatically. That's the whole mechanism -- there's no separate
    "vended credentials mode" flag to flip.
    """
    props = {
        "uri": _require_env("POLARIS_CATALOG_URI"),
        "warehouse": _require_env("POLARIS_WAREHOUSE"),
        "credential": f'{_require_env("POLARIS_CLIENT_ID")}:{_require_env("POLARIS_CLIENT_SECRET")}',
        "scope": os.environ.get("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL"),
    }
    token_uri = os.environ.get("POLARIS_TOKEN_URI")
    if token_uri:
        props["oauth2-server-uri"] = token_uri
    else:
        logger.warning(
            "POLARIS_TOKEN_URI is not set. pyiceberg will fall back to deriving an "
            "OAuth2 token endpoint from POLARIS_CATALOG_URI, but pyiceberg 0.11.x "
            "explicitly flags that fallback as deprecated and scheduled for "
            "removal in a future release (see its own deprecation warning). Set "
            "POLARIS_TOKEN_URI in .env to avoid this breaking later for no reason."
        )
    return props


def _make_catalog() -> RestCatalog:
    return RestCatalog("fivetran_mdls", **catalog_properties())


def get_catalog() -> RestCatalog:
    """Serial-path catalog instance, used by the top-level driver and by
    anything running outside a worker thread."""
    return _make_catalog()


def thread_catalog() -> RestCatalog:
    """One RestCatalog per worker thread, created lazily on first use in that
    thread and reused for the rest of that thread's work -- never shared
    across threads. pyiceberg's RestCatalog isn't documented as thread-safe,
    and a per-thread catalog instance sidesteps that question entirely
    rather than relying on undocumented behavior holding up under
    concurrent load. The one-time OAuth handshake cost of an extra catalog
    instance per thread is trivial next to the per-call network latency
    this parallelism is trying to hide in the first place."""
    cat = getattr(_thread_local, "cat", None)
    if cat is None:
        cat = _make_catalog()
        _thread_local.cat = cat
    return cat


# --------------------------------------------------------------------------
# Source discovery
# --------------------------------------------------------------------------


def discover_common_tables(cat: RestCatalog, source_namespaces: list) -> list:
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


def ensure_namespace(cat: RestCatalog, ns: str) -> None:
    try:
        cat.create_namespace(ns)
    except Exception as e:
        if "already exists" not in str(e).lower() and "AlreadyExists" not in type(e).__name__:
            raise


# --------------------------------------------------------------------------
# Correctness logic: reserve / widen / detect drift / resolve collision
#
# Everything below is unchanged in substance from the validated internal
# reference implementation (register_consolidated_v7.py). Only names were
# generalized (a configurable source_id_column instead of a hardcoded
# "source_db_id", configurable source namespaces instead of a hardcoded
# {prefix}_{NN} list) and all manual AWS FileIO handling was removed, since
# vended credentials mean the Table objects returned by the catalog already
# carry correctly-scoped IO.
# --------------------------------------------------------------------------


def create_target_table(cat: RestCatalog, target_id: str, source_schema: Schema, source_id_column: str):
    """Create the target table, then reserve a permanently out-of-band field
    id for the bookkeeping source-id column in a SEPARATE schema-evolution
    call made after the table exists.

    This two-step dance is required, not stylistic. Passing the desired
    field id directly into the schema at CREATE TABLE time does not work --
    the catalog server renumbers all field ids on create regardless of what
    the client requests (confirmed by inspecting the literal request
    payload against a real Polaris catalog). Only a schema-evolution call
    made AFTER the table already exists is honored as sent. See
    docs/HOW_IT_WORKS.md for the full explanation, including the private
    pyiceberg attribute (`_last_column_id`) this currently depends on.
    """
    target_table = cat.create_table(target_id, schema=Schema(*source_schema.fields, schema_id=0))
    with target_table.update_schema() as us:
        us._last_column_id = itertools.count(RESERVED_FIELD_ID_BASE)
        us.add_column(source_id_column, StringType())
    target_table = cat.load_table(target_id)
    with target_table.update_spec() as spec:
        spec.add_identity(source_id_column)
    return cat.load_table(target_id)


def file_has_drift_beyond(f, known_field_names: dict, src_schema):
    """Return the field id of genuine schema drift in this file, or None.

    Compares null_value_counts (per field id) against record_count, not just
    whether a field id appears as a key in the file's stats at all. Iceberg
    backfills a stats entry for every field in a table's CURRENT schema onto
    every file, including files written before that field existed, so key
    presence alone produces false positives on old files. A field id the
    TARGET doesn't already know about, with fewer nulls than the file has
    records, means that file genuinely carries non-null data for a column
    the target's registered schema didn't have.

    known_field_names MUST be sourced from the TARGET table's own current
    schema (a {field_id: name} dict), never from any one source's schema.
    Field ids are assigned independently per source table in this project's
    design -- that's exactly why the collision-handling logic in
    register_table() exists at all (two sources landing on the same field id
    with different meanings) -- so a source's own field-id numbering can't be
    trusted as "the baseline" either. This function used to take an integer
    threshold (baseline_max_field_id) borrowed from source_namespaces[0]'s
    live schema instead of a set from the target: any field id already
    present in that ONE source's current schema silently counted as "already
    known," even if that source had only just gained it moments earlier in
    the very sync being consolidated. See CHANGELOG.md's "Fixed: a source's
    own schema evolution could vanish into a stale baseline" entry for the
    live reproduction (a new column added by source_namespaces[0] itself
    never got detected as drift, so it never got widened into the target --
    not a one-time miss, either: since the threshold was re-derived from
    that same source's now-already-evolved schema on every subsequent run,
    it stayed permanently invisible).

    CRITICAL, confirmed-by-reproduction fix: knowing a field id ISN'T THE
    SAME as that id's collision (if any) actually being resolved for THIS
    source. A field id being present in known_field_names only means SOME
    source's name for it has been registered -- it says nothing about
    whether THIS file's own source agrees that's what the id means. Two
    ways this bit us for real, both against the identical live data (see
    CHANGELOG.md "Fixed: silent field-id collision misattribution"):

      1. Day one, no interruption needed at all: create_target_table() seeds
         the target's starting schema from source_namespaces[0]'s full
         schema, so every field id source #1 already happens to use is
         "known" from the moment the target table is created -- before this
         function is ever called for any OTHER source's files. A second
         source landing a differently-named column on that same numeric id
         (which independently-evolving sources do routinely, since each
         starts counting from its own schema's next free id) was never
         flagged as drift and got silently spliced in under source #1's
         name for that id.
      2. An interrupted collision-rewrite: the "owner" name for a colliding
         id is committed as soon as the collision is FIRST detected (see the
         widening step in register_table()), independently of whether the
         LOSING source's rewrite (a separate, later commit) ever completes.
         If that rewrite is interrupted, a rerun sees the id as "known" from
         the owner's already-committed widening and never re-flags the
         loser's still-unresolved file, even though that file's rewrite
         never actually happened.

    Both are the same underlying mistake: treating "is this id known" as
    equivalent to "is this id known to mean what THIS source thinks it
    means." The fix is to also compare THIS source's own name for the field
    id (from src_schema, that source's live schema) against the target's
    registered name for it -- a mismatch is still-unresolved drift,
    regardless of whether the id itself is otherwise "known."
    """
    if not f.null_value_counts:
        return None
    for field_id, null_count in f.null_value_counts.items():
        if null_count >= f.record_count:
            continue
        target_name = known_field_names.get(field_id)
        if target_name is None:
            return field_id
        src_field = src_schema.find_field(field_id) if src_schema else None
        if src_field is not None and src_field.name != target_name:
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

    Returns a (by_source, spliced_by_source) pair. by_source is every
    registered file, used for the plain "already registered, don't
    re-splice" idempotency check. spliced_by_source is the subset actually
    eligible for copy-on-write orphan detection: only files spliced in BY
    REFERENCE still physically live at the SOURCE's own storage location,
    so only those can be meaningfully compared against "does this path
    still appear in the source's current file listing." A file created by
    the collision-rewrite path (register_table()'s step 4, via
    Transaction.append()) is a brand-new physical file written under the
    TARGET's own storage location -- it will never appear in any source's
    own file listing, by construction, regardless of whether anything is
    wrong with it. Treating it as orphan-eligible anyway was a real,
    reproduced bug: every such file got deleted and re-derived on every
    subsequent run, since "not in the source's current files" was always
    true for it. See CHANGELOG.md "Fixed: rewrite-path files were
    perpetually treated as copy-on-write orphans".
    """
    by_source = {}
    spliced_by_source = {}
    target_location = target_table.metadata.location
    try:
        for task in target_table.scan().plan_files():
            f = task.file
            src = f.partition[0]
            by_source.setdefault(src, {})[f.file_path] = f
            if not f.file_path.startswith(target_location):
                spliced_by_source.setdefault(src, {})[f.file_path] = f
    except Exception:
        pass
    return by_source, spliced_by_source


def _load_source_files(source_namespace: str, table_name: str):
    """Runs inside the source-level thread pool -- one (load_table +
    plan_files) round trip per source, using this thread's own catalog."""
    cat = thread_catalog()
    src_table = cat.load_table(f"{source_namespace}.{table_name}")
    src_schema = src_table.schema()
    files = list(src_table.scan().plan_files())
    return source_namespace, src_schema, files


def find_field_id_disagreements(schemas_by_source: dict) -> list:
    """Find field ids that DIFFERENT SOURCES already disagree about.

    This guards a failure mode the collision logic structurally cannot see,
    and which silently corrupts data. Reported by a customer 2026-09-22 and
    reproduced with two sources whose columns differ only in declaration
    order (see CHANGELOG.md).

    The collision path in register_table() only ever fires on *drift* --
    file_has_drift_beyond() flags a field id the TARGET does not already
    know about. But the target's schema is seeded from source_namespaces[0]
    alone, so every field id that source already uses is "known" from the
    moment the target is created. If a DIFFERENT source uses those same ids
    for different columns -- not as drift, but in its base schema -- nothing
    is ever flagged. No collision, no warning, matching row counts, and
    files spliced in by reference.

    Iceberg then resolves columns by field id, so that source's files get
    read through the first source's names and types. The customer saw
    metrics land in the wrong columns and a DOUBLE truncated to a LONG.

    This is not exotic. Fivetran assigns field ids in column-declaration
    order, so two tenants whose tables were created with the same columns in
    a different order, or which evolved along different paths, diverge here
    with no error and no invalid schema on either side.

    As of e1650c1 the tool DOES handle this correctly -- file_has_drift_beyond()
    compares each source's own name for a field id against the target's
    registered name, so a divergent source is routed through the rewrite
    path and its data lands right. Verified end to end against two sources
    whose columns differ only in declaration order: values correct for
    both, no corruption.

    But it handles it at full price, and says nothing about that. Because a
    file's field ids are baked into its Parquet footer, the ONLY way to make
    a divergent source safe is to rewrite it physically -- so that source
    gets 0 files spliced and 100% of its rows rewritten, silently reported
    as an ordinary "collision." For base-schema divergence across many
    sources that is an enormous first run and forfeits the whole no-rewrite
    premise, and today nothing warns anyone before they start.

    Hence: this is a WARNING, not an abort. The run is correct either way;
    the point is to say up front which sources will be rewritten wholesale
    and why, so nobody discovers it from a bill.

    Returns a list of (field_id, [(namespace, name, type), ...]) for every
    field id where sources disagree on either the name or the type.
    """
    seen = {}  # field_id -> list of (ns, name, type_str)
    for ns, schema in schemas_by_source.items():
        for f in schema.fields:
            seen.setdefault(f.field_id, []).append((ns, f.name, str(f.field_type)))

    disagreements = []
    for field_id, entries in sorted(seen.items()):
        distinct = {(name, ftype) for _, name, ftype in entries}
        if len(distinct) > 1:
            disagreements.append((field_id, entries))
    return disagreements


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

    # Load every source's schema + file list FIRST, before creating or
    # touching anything, so a source-disagreement abort leaves no trace.
    # (This used to happen after target creation; the reorder is what makes
    # the check below a true pre-flight.)
    files_by_source = {}
    with ThreadPoolExecutor(max_workers=min(source_workers, len(source_namespaces))) as ex:
        futs = {ex.submit(_load_source_files, ns, table_name): ns for ns in source_namespaces}
        for fut in as_completed(futs):
            ns, src_schema, files = fut.result()
            files_by_source[ns] = (src_schema, files)

    # PRE-FLIGHT: do the sources actually agree on what each field id means?
    # Splicing by reference is only safe if they do -- see
    # find_field_id_disagreements() for why the collision path cannot catch
    # this and why there is no cheap repair.
    disagreements = find_field_id_disagreements(
        {ns: schema for ns, (schema, _) in files_by_source.items()}
    )
    if disagreements:
        # Which sources hold the minority reading of a contested id? Those are
        # the ones routed through the rewrite path rather than spliced.
        baseline = source_namespaces[0]
        contested_ids = {fid for fid, _ in disagreements}
        divergent = set()
        for _, entries in disagreements:
            base = {(n, t) for ns, n, t in entries if ns == baseline}
            for ns, n, t in entries:
                if ns != baseline and (n, t) not in base:
                    divergent.add(ns)

        # Measure the actual cost rather than assuming it. A contested id that
        # every one of a source's files carries data for means its BASE schema
        # diverges, so the whole source gets rewritten. A contested id only
        # recent files carry is ordinary drift, which the collision path
        # already handles cheaply and already logs. Same schema symptom, very
        # different bill -- so report the measured fraction and only escalate
        # when it is big.
        affected = {}
        for ns in sorted(divergent):
            _, files = files_by_source[ns]
            hit = sum(
                1 for t in files
                if any(
                    (t.file.null_value_counts or {}).get(fid, t.file.record_count) < t.file.record_count
                    for fid in contested_ids
                )
            )
            affected[ns] = (hit, len(files))

        worst = max((h / m if m else 0) for h, m in affected.values()) if affected else 0
        if worst < 0.9:
            logger.info(
                "  [%s] %d field id(s) differ across sources, but only on recently-drifted "
                "files (%s) -- this is the routine collision path, handled below at normal cost.",
                table_name,
                len(disagreements),
                ", ".join(f"{ns} {h}/{m} files" for ns, (h, m) in affected.items()),
            )
            disagreements = []

    if disagreements:
        lines = [
            f"[{table_name}] source namespaces disagree about what "
            f"{len(disagreements)} field id(s) mean.",
            "",
            "This is NOT the drift collision this tool resolves as routine: that is",
            "a NEW column landing on an occupied id. Here the sources' BASE schemas",
            "disagree -- typically because Fivetran assigns field ids in column",
            "declaration order, and these tables were created with their columns in",
            "different orders.",
            "",
        ]
        for field_id, entries in disagreements:
            lines.append(f"  field id {field_id}:")
            for ns, name, ftype in sorted(entries):
                lines.append(f"    {ns:<32} {name} ({ftype})")
        lines += [
            "",
            "The run below is CORRECT -- each divergent source is routed through the",
            "rewrite path, so its data lands under the right names and types. But a",
            "file's field ids are baked into its Parquet footer, so rewriting is the",
            "only way to make it safe. Files to be physically rewritten:",
            "",
        ]
        for ns, (hit, total) in sorted(affected.items()):
            lines.append(f"    {ns:<32} {hit}/{total} files ({100 * hit / total:.0f}%)")
        lines += [
            "",
            "That is a one-time cost per source (later runs are still no-ops), but",
            "at scale it is a large first run and it forfeits the no-rewrite premise",
            "for those sources. To keep consolidation cheap, consolidate sources",
            "whose field-id mapping already agrees. See docs/HOW_IT_WORKS.md.",
        ]
        logger.warning("\n".join(lines))

    source_schema = files_by_source[source_namespaces[0]][0]

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
    # source's own future schema changes. Keyed by name (not just a set of
    # ids) so drift-detection can tell "known and this source agrees what
    # it means" apart from "known, but only because a DIFFERENT source's
    # name for it won" -- see file_has_drift_beyond()'s docstring.
    known_field_names = {f.field_id: f.name for f in target_table.schema().fields}

    already_by_source, spliced_by_source = already_registered_by_source(target_table)
    already = {p for paths in already_by_source.values() for p in paths}

    # 1. Detect every source's drifted files. The per-source (load_table +
    #    plan_files) round trips already happened above, as the pre-flight
    #    field-id agreement check needs every source's schema before
    #    anything is created -- files_by_source is reused here rather than
    #    re-fetched.
    drift_by_field_id = {}
    for ns, (src_schema, files) in files_by_source.items():
        for task in files:
            f = task.file
            drift_field = file_has_drift_beyond(f, known_field_names, src_schema)
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
    #
    #    IMPORTANT: "first distinct name seen" only gets to become the
    #    no-rewrite owner if the target doesn't ALREADY have a registered
    #    name for this physical field id. The target's schema starts out
    #    seeded from source_namespaces[0]'s full schema at creation (see
    #    create_target_table()), so a field id can already mean something
    #    to the target on the very first run this collision is even
    #    noticed -- before any widening this function does. Trying to widen
    #    onto an id the target already uses for a DIFFERENT name isn't a
    #    free no-rewrite claim, it's a genuine conflict, and Polaris
    #    correctly rejects it (HTTP 400: duplicate field id) rather than
    #    silently overwriting what that id already means. Reproduced live
    #    while validating the file_has_drift_beyond() fix above -- see
    #    CHANGELOG.md "Fixed: silent field-id collision misattribution".
    #    When the target already has a name for this id, that name is the
    #    true owner outright, and EVERY distinct name seen this run is a
    #    loser needing the rewrite path -- none of them get a free widen.
    rewrite_needed = []
    for field_id, entries in drift_by_field_id.items():
        distinct_names = {}
        for ns, name, ftype in entries:
            distinct_names.setdefault(name, []).append(ns)
        names_in_order = list(distinct_names.keys())

        existing_name_for_id = known_field_names.get(field_id)
        if existing_name_for_id is not None:
            owner_name = existing_name_for_id
            losers = names_in_order
        else:
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
            losers = names_in_order[1:]

        for other_name in losers:
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
        src_schema, files = files_by_source[ns]
        current_paths_this_source = {task.file.file_path for task in files}
        # Orphan-eligible files ONLY -- spliced_by_source excludes anything
        # written by the collision-rewrite path (step 4's Transaction.append()
        # calls), since those files physically live under the TARGET's own
        # storage location and will never appear in current_paths_this_source
        # regardless of whether anything is wrong with them. Using the full
        # already_by_source here instead was a real, reproduced bug: every
        # rewritten/appended file got treated as orphaned and deleted+
        # re-derived on every subsequent run. See
        # already_registered_by_source()'s docstring and CHANGELOG.md
        # "Fixed: rewrite-path files were perpetually treated as
        # copy-on-write orphans".
        registered_this_source = spliced_by_source.get(ns, {})

        files_ok, rows_this_source, files_skipped_already, files_skipped_rewrite = 0, 0, 0, 0
        for task in files:
            f = task.file
            if f.file_path in already:
                files_skipped_already += 1
                continue
            drift_field = file_has_drift_beyond(f, known_field_names, src_schema)
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
        src_schema, files = files_by_source[ns]
        to_rewrite = [
            task.file
            for task in files
            if task.file.file_path not in already
            and file_has_drift_beyond(task.file, known_field_names, src_schema) == field_id
        ]
        if not to_rewrite:
            continue

        src_table = cat.load_table(f"{ns}.{table_name}")
        column_already_exists = name in {f.name for f in target_table.schema().fields}

        if column_already_exists:
            # The column is already registered. With the atomic fix below,
            # that alone would normally be enough to know its data is there
            # too -- but a run from BEFORE this fix could have left the
            # column registered with none of this source's data ever
            # appended (see CHANGELOG.md "Fixed: silent field-id collision
            # misattribution" and file_has_drift_beyond()'s docstring for
            # the full mechanism). Check row content explicitly rather than
            # trusting "column exists" alone, so a pre-existing broken state
            # gets healed instead of silently staying broken.
            #
            # This scan's row_filter restricts to source_id_column = ns, but
            # the matching files (spliced in by reference in a prior run,
            # per this tool's whole no-rewrite design) still physically
            # live under NS's OWN storage location, not the target's -- and
            # Polaris only ever vends the target table's credential scoped
            # to the target's own location. Reading through target_table.io
            # alone hits AWS ACCESS_DENIED here (reproduced live, see
            # CHANGELOG.md's "Known issue: collision-rewrite path can hit
            # ACCESS_DENIED under vended credentials"). Temporarily swap in
            # a FileIO that also tries this source's own vended credential
            # -- src_table.io -- before falling back to failure; restore
            # the target's own IO immediately after.
            # source_id_column MUST stay in selected_fields even though only
            # `name` is counted. The target is identity-partitioned on
            # source_id_column, and pyiceberg's _get_column_projection_values
            # resolves the partition source field against the PROJECTED
            # schema, not the table schema:
            #     partition_schema = partition_spec.partition_type(projected_schema)
            # That line is reached whenever the projection contains a field id
            # the physical file lacks -- which is always true here, because a
            # collision gives `name` a fresh out-of-band field id that no
            # spliced source file physically carries. Projecting `name` alone
            # therefore crashes with "ValueError: Could not find field with
            # id: <reserved id>" (reproduced live 2026-09-10 on a real
            # two-source collision). Keeping source_id_column in the
            # projection puts the partition source field in the projected
            # schema, so it resolves and the identity value backfills.
            original_io = target_table.io
            target_table.io = MultiSourceFileIO([target_table.io, src_table.io])
            try:
                already_rewritten = target_table.scan(
                    row_filter=f"{source_id_column} = '{ns}' and {name} is not null",
                    selected_fields=(name, source_id_column),
                ).to_arrow().num_rows
            finally:
                target_table.io = original_io
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
            logger.warning(
                # Two very different situations reach here, and the message used to
                # assert the rarer one as fact. Either (a) a prior run was
                # interrupted before the atomic-commit fix and left the column
                # registered with no data, or (b) -- far more common -- the column
                # already exists simply because ANOTHER source contributed it, and
                # this source's rows for it have legitimately never been written.
                # Case (b) happens on the very first run whenever sources' base
                # schemas disagree about a field id (see
                # find_field_id_disagreements), where blaming "an interrupted run"
                # sends whoever reads the log hunting a failure that never occurred.
                "  [%s] '%s' from %s: column exists but only %d/%d expected row(s) present. Either "
                "another source contributed this column and none of THIS source's rows for it have "
                "been written yet (normal on a first run when sources' base schemas disagree about a "
                "field id), or a pre-atomic-commit run was interrupted. Appending the missing rows "
                "now (schema unchanged, data-only commit).",
                table_name,
                name,
                ns,
                already_rewritten,
                expected_rows,
            )

        # ONE scan, not one per file in to_rewrite. This used to be a
        # `for f in to_rewrite:` loop whose body never referenced `f` -- it
        # re-ran the identical whole-table scan on every iteration and
        # appended every copy, so a source with N drifted files had its
        # drifted rows landed N times over. Reproduced live against a
        # purpose-built two-source collision (see CHANGELOG.md):
        # 7 drifted files produced 175,000 rows where 25,000 were correct,
        # an exact 7x. See CHANGELOG.md "Fixed: collision rewrite appended
        # one duplicate copy per drifted file".
        #
        # A single scan is already complete: `name is not null` selects every
        # drifted row across all of this source's files at once, which is
        # exactly what the rewrite needs. to_rewrite is still what decides
        # WHETHER there is anything to rewrite, and how many rows to expect.
        tab = src_table.scan(row_filter=f"{name} is not null").to_arrow()
        if tab.num_rows == 0:
            continue
        tab = tab.append_column(source_id_column, pa.array([ns] * tab.num_rows, type=pa.string()))
        tabs = [tab]

        # THE FIX: schema evolution (if the column is new) and every file's
        # data append for this collision now commit as ONE atomic
        # Transaction -- either both land together in a single snapshot, or
        # neither does. Before this, add_column() was its own standalone
        # commit, followed by a separate append() per file; an interruption
        # between them left the column registered with no data, AND (see
        # file_has_drift_beyond()'s docstring) made drift-detection stop
        # flagging this source's file at all on the next run, since the
        # field id was already "known" -- reproduced live, see
        # CHANGELOG.md "Fixed: silent field-id collision misattribution".
        with target_table.transaction() as txn:
            if not column_already_exists:
                with txn.update_schema() as us:
                    us.add_column(name, ftype)
            for tab in tabs:
                txn.append(tab)
        target_table = cat.load_table(target_id)
        fresh_id = target_table.schema().find_field(name).field_id
        rows_this_pass = sum(t.num_rows for t in tabs)
        rewritten_rows += rows_this_pass
        logger.info(
            "  [%s] rewrite: '%s' (from %s) field_id=%d, %d row(s) rewritten + appended (%s)",
            table_name,
            name,
            ns,
            fresh_id,
            rows_this_pass,
            "schema + data committed atomically" if not column_already_exists else "data-only, healing pre-fix state",
        )

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
        description="No-rewrite Iceberg table consolidation for a Fivetran-managed Polaris/MDLS catalog."
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
