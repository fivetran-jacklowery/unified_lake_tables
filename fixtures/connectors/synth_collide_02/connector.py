"""synth_collide_02 -- half of a two-source fixture that forces an Iceberg
field-id COLLISION, to reproduce a suspected duplication bug in
unified-lake-tables/scripts/register_consolidation.py's rewrite pass.

WHY THIS EXISTS
---------------
`register_consolidation.py` step 4 handles the case where two sources
independently land DIFFERENT column names on the SAME physical Iceberg
field id. The winner keeps the id (free metadata widen); the loser gets a
fresh id and has its drifted rows physically rewritten into the target.

That rewrite pass looks like this:

    for f in to_rewrite:
        tab = src_table.scan(row_filter=f"{name} is not null").to_arrow()
        ...
        tabs.append(tab)
    ...
    for tab in tabs:
        txn.append(tab)

`f` is never used in the body. The scan covers the WHOLE source table and
is re-run identically on every iteration, so `tabs` ends up holding
len(to_rewrite) identical copies -- and every one of them gets appended.

The bug is therefore invisible whenever the losing source has only ONE
drifted file, which is why it has survived: it needs len(to_rewrite) >= 2.
This fixture manufactures exactly that condition.

HOW THE COLLISION IS MANUFACTURED
---------------------------------
synth_collide_01 and _02 declare a byte-identical base schema, so Fivetran
assigns both tables the same field ids for every base column. Each then
grows ONE new column with a DIFFERENT name:

    synth_collide_01 -> "alpha_metric"
    synth_collide_02 -> "beta_flag"       (this file)

Because both tables are at the same highest field id when the new column
arrives, both new columns land on the SAME next field id with different
names. That is the collision.

The drift column is deliberately NOT declared in schema(). It simply starts
appearing as an extra key in the upserted row dict on sync 2, which is how
real source drift actually reaches Fivetran -- and it lets one deploy cover
both phases, with no config edit between syncs.

SYNC PLAN
---------
  sync 1: BASE_ROWS rows, no drift column at all -> establishes the base
          schema and its field ids.
  sync 2: DRIFT_ROWS additional rows, every one carrying a non-null
          drift-column value -> must span >= 2 physical files for the bug
          to reproduce. Verify the file count before consolidating; run a
          third sync if the writer packed it into one file.

Generators, never lists: Connector SDK containers get 1 GB RAM and these
row counts would OOM if materialized (same gotcha the synth_src_* fixtures
document).
"""

import random

from faker import Faker
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

TABLE = "widgets"

# The whole point of the fixture: same field id, different name per source.
DRIFT_COLUMN = "beta_flag"

BASE_ROWS = 20_000    # sync 1: no drift column
DRIFT_ROWS = 25_000   # sync 2: all carry a non-null drift column
CHECKPOINT_EVERY = 2_000

CATEGORIES = ["fastener", "bearing", "gasket", "coupling", "bracket", "seal"]
REGIONS = ["AMER", "EMEA", "APAC", "LATAM"]


def schema(configuration: dict):
    """Base schema ONLY.

    `DRIFT_COLUMN` is deliberately absent -- it appears for the first time
    as an extra key in the row dicts on sync 2, so the destination grows the
    column the same way it would for a genuinely evolving source.
    """
    return [
        {
            "table": TABLE,
            "primary_key": ["widget_id"],
            "columns": {
                "widget_id": "LONG",
                "sku": "STRING",
                "name": "STRING",
                "category": "STRING",
                "quantity": "LONG",
                "unit_price": "DOUBLE",
                "region": "STRING",
                "is_active": "BOOLEAN",
                "created_at": "UTC_DATETIME",
            },
        }
    ]


def gen_widgets(fake, start, end, with_drift):
    """Yield widgets [start, end]. One row at a time -- never a list."""
    for i in range(start, end + 1):
        row = {
            "widget_id": i,
            "sku": f"WGT-{i:08d}",
            "name": fake.catch_phrase(),
            "category": random.choice(CATEGORIES),
            "quantity": random.randint(0, 5_000),
            "unit_price": round(random.uniform(0.5, 900.0), 2),
            "region": random.choice(REGIONS),
            "is_active": random.random() > 0.1,
            "created_at": fake.date_time_between(
                start_date="-2y", end_date="now"
            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if with_drift:
            # Non-null on EVERY drifted row: file_has_drift_beyond() compares
            # null_value_counts against record_count, so a file only counts as
            # drifted when it genuinely carries values for the new field id.
            row[DRIFT_COLUMN] = random.choice(["red", "amber", "green"])
        yield row


def update(configuration: dict, state: dict):
    seed = int(configuration.get("seed", "1"))
    sync_count = int(state.get("sync_count", 0)) + 1
    rows_done = int(state.get("rows_done", 0))

    fake = Faker()
    Faker.seed(seed + sync_count)
    random.seed(seed + sync_count)

    if sync_count == 1:
        start, end, with_drift = 1, BASE_ROWS, False
        log.info(f"sync 1: {BASE_ROWS} base rows, no '{DRIFT_COLUMN}' column")
    elif sync_count == 2:
        start, end, with_drift = BASE_ROWS + 1, BASE_ROWS + DRIFT_ROWS, True
        log.info(
            f"sync 2: {DRIFT_ROWS} rows WITH '{DRIFT_COLUMN}' -- this is the "
            "drift that must span >= 2 files"
        )
    else:
        # Idempotent afterwards: nothing new unless you bump the constants.
        log.info(f"sync {sync_count}: nothing further to emit")
        yield op.checkpoint(state={"sync_count": sync_count, "rows_done": rows_done})
        return

    emitted = 0
    for row in gen_widgets(fake, start, end, with_drift):
        yield op.upsert(table=TABLE, data=row)
        emitted += 1
        if emitted % CHECKPOINT_EVERY == 0:
            yield op.checkpoint(
                state={"sync_count": sync_count, "rows_done": rows_done + emitted}
            )

    log.info(f"sync {sync_count}: emitted {emitted} rows")
    yield op.checkpoint(
        state={"sync_count": sync_count, "rows_done": rows_done + emitted}
    )


connector = Connector(update=update, schema=schema)
