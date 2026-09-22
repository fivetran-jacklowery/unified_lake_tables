"""synth_order_02 -- half of a two-source fixture reproducing SILENT DATA
CORRUPTION when sources assign Iceberg field ids in different orders.

Reported by a customer (2026-09-22) running unified-lake-tables against
their Fivetran MDLS lake: on a table whose sources assign field ids in
different orders, the consolidated table returned wrong values -- metrics
landed in the wrong columns and a float column was truncated to integers --
while the registration run reported success, 0 collisions, 0 warnings, and
matching row counts.

WHY THE EXISTING COLLISION LOGIC DOES NOT CATCH THIS
----------------------------------------------------
register_consolidation.py seeds the target's schema from source_namespaces[0]
alone, then builds `known_field_names` from the target. file_has_drift_beyond()
only flags a field id the target does NOT already know about. When two sources
use the SAME field ids for DIFFERENT columns from the outset, every id is
already "known", so nothing is ever flagged as drift, no collision is raised,
and the files are spliced in by reference. Iceberg then resolves columns by
field id, so the second source's files are read through the first source's
names and types.

The existing three-layer fix (reserve / widen / rewrite) only covers the case
where a NEW column appears and lands on an occupied id. It does not cover base
schemas that disagree about what an id means.

HOW THIS FIXTURE MANUFACTURES IT
---------------------------------
Both sources declare the SAME table with the SAME column names and the SAME
types. Only the declaration ORDER differs, and Fivetran assigns Iceberg field
ids in declaration order:

    synth_order_01:              metric_id, clicks, ctr, impressions, label
    synth_order_02 (this file):  metric_id, ctr, clicks, impressions, label

So field id 2 is `clicks` (LONG) here and `ctr` (DOUBLE) there; field id 3 is
the reverse. Nothing about either schema is invalid, and no column is new.

The values are chosen to make the corruption unmistakable:
    clicks -> large integers (1000..9999)
    ctr    -> small floats   (0.0..1.0)

If the ids get crossed, `clicks` shows 0 (a float under 1 truncated to a
long) and `ctr` shows values in the thousands.
"""

import random

from faker import Faker
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

TABLE = "metrics"
ROWS = 20_000
CHECKPOINT_EVERY = 2_000

LABELS = ["organic", "paid", "referral", "direct"]


def schema(configuration: dict):
    """Declaration order IS the field-id order. ctr before clicks here."""
    return [
        {
            "table": TABLE,
            "primary_key": ["metric_id"],
            "columns": {
                "metric_id": "LONG",
                "ctr": "DOUBLE",         # field id 2 here, 3 in synth_order_01
                "clicks": "LONG",        # field id 3 here, 2 in synth_order_01
                "impressions": "LONG",
                "label": "STRING",
            },
        }
    ]


def gen_metrics(fake, start, end):
    for i in range(start, end + 1):
        yield {
            "metric_id": i,
            # Deliberately disjoint value ranges from ctr, so a crossed
            # field id is obvious rather than plausible.
            "clicks": random.randint(1_000, 9_999),
            "ctr": round(random.uniform(0.0, 1.0), 6),
            "impressions": random.randint(10_000, 999_999),
            "label": random.choice(LABELS),
        }


def update(configuration: dict, state: dict):
    seed = int(configuration.get("seed", "1"))
    if state.get("loaded"):
        log.info("already loaded; nothing to emit")
        yield op.checkpoint(state=state)
        return

    fake = Faker()
    Faker.seed(seed)
    random.seed(seed)

    emitted = 0
    for row in gen_metrics(fake, 1, ROWS):
        yield op.upsert(table=TABLE, data=row)
        emitted += 1
        if emitted % CHECKPOINT_EVERY == 0:
            yield op.checkpoint(state={"rows_done": emitted})

    log.info(f"emitted {emitted} rows")
    yield op.checkpoint(state={"loaded": True, "rows_done": emitted})


connector = Connector(update=update, schema=schema)
