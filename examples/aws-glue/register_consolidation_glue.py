#!/usr/bin/env python3
"""register_consolidation_glue.py -- no-rewrite Iceberg table consolidation
against a customer-managed AWS Glue Data Catalog.

This is a thin adapter, NOT a reimplementation. All consolidation logic
(schema reservation, no-rewrite splicing, drift detection, field-id
collision resolution, copy-on-write orphan retirement) lives in
scripts/register_consolidation.py and runs here unchanged.

WHY THIS FILE IS NINETY LINES AND NOT EIGHT HUNDRED
---------------------------------------------------
It used to be eight hundred. This was a full fork of
scripts/register_consolidation.py with only catalog_properties() and
_make_catalog() changed, and its header said exactly that: everything else
"copied verbatim, unmodified, from the validated Polaris version."

That was true the day it was written, and false within weeks. The Polaris
script then received a series of field-id correctness fixes -- the
name-comparison in file_has_drift_beyond() (e1650c1), the
find_field_id_disagreements() pre-flight, the one-scan rewrite, the
spliced_by_source orphan filter, the atomic schema+data commit -- and this
fork received none of them. Nothing flagged the drift, because a copy
doesn't announce that its original moved.

A customer then ran this fork against a six-tenant SQL Server replication
in Glue, where the same column carries a different Iceberg field id per
tenant. It spliced files by reference whose embedded field ids disagreed
with the target schema, producing a consolidated table that returned the
wrong column's values -- the precise corruption those fixes exist to
prevent -- and reported success with 0 rewrites. See CHANGELOG.md.

So the fork is gone. The backend is now a switch (CATALOG_BACKEND) inside
the one validated implementation, and every current and future fix reaches
Glue for free. Do not reintroduce a copy of the main script here.

WHAT ACTUALLY DIFFERS ON GLUE
------------------------------
Exactly one thing: credentials. Polaris vends short-lived, per-table-scoped
AWS storage credentials on every table load, and the Polaris path
deliberately strips ambient AWS credentials to force that vended-only path.
Glue vends nothing. The IAM principal running this needs its own standing
credentials covering:

  - Glue read across every source database (glue:GetDatabase, glue:GetTable,
    glue:GetTables, glue:GetPartitions), plus create/alter on the target
    (glue:CreateDatabase, glue:CreateTable, glue:UpdateTable).
  - S3 read across every source table's data location, and read+write on the
    target's.

That is a necessarily BROADER IAM footprint than the Polaris path needs,
because there is no per-table vending to scope it down. Size it deliberately
with whoever owns IAM policy; see this directory's README.md.

Setting CATALOG_BACKEND in code below, before importing the main module, is
load-bearing. The main module decides at IMPORT time whether to purge
ambient AWS credentials, and on Glue those credentials are the only ones
there are. Putting CATALOG_BACKEND in .env would be too late -- load_dotenv()
doesn't run until main(). That single ordering constraint is the whole
reason this wrapper exists rather than a line in the README telling you to
export a variable.
"""
import os
import sys

# MUST precede the import below -- see the module docstring. The main module
# gates its import-time ambient-AWS-credential purge on this value, and that
# purge would remove the only credentials the Glue backend has.
os.environ["CATALOG_BACKEND"] = "glue"

# The main script lives in scripts/, two directories up from this example.
sys.path.insert(
    0,
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts"),
)

import register_consolidation as rc  # noqa: E402

if __name__ == "__main__":
    # rc.main() handles argparse (--config, optional table names), .env
    # loading, config validation, the run itself and the summary output,
    # identically to the Polaris CLI. Nothing to duplicate here.
    rc.main()
