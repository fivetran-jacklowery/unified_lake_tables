---
name: unified-lake-tables
description: >
  Consolidate many structurally-identical Iceberg tables — one per tenant,
  one per regional database, one per customer, however a Fivetran account's
  connections are split up — into a single queryable table per table type,
  on a Fivetran-managed Polaris catalog and Managed Data Lake Service (MDLS)
  destination, without rewriting any underlying data. Use this skill whenever
  someone wants to reduce a large number of same-schema Fivetran connections
  down to one logical table, mentions "many:1", "many-to-one", "table-level
  many to one", consolidating per-tenant or per-customer database connections,
  replacing a `UNION ALL` view or a materialized merge job across many
  identical sources, cutting query cost across dozens or hundreds of
  connections, or handling schema drift (new columns) across many similar
  Iceberg tables — even if they don't name Iceberg, Polaris, or MDLS
  explicitly. Also use it if someone asks how to avoid rewriting all their
  data every time they want one merged table out of many connections.
---

# unified-lake-tables

This skill drives the toolkit in this repository to actually run a no-rewrite
consolidation pass against a real Fivetran-managed Polaris catalog. It is not
a general Iceberg or data-modeling skill — it does one specific thing well:
fold N structurally-identical source tables into one target table by
splicing existing data files into a new manifest, instead of rewriting data.

Read `README.md` first if you haven't already — it explains what this is and
why it's cheaper than a `UNION ALL` view or a merge job, with a card-catalog
analogy that's worth internalizing before touching the script. Read
`docs/HOW_IT_WORKS.md` for the actual mechanism (field-id reservation,
splicing, schema-drift detection and resolution, idempotency) — you'll want
that context to explain what's happening to whoever you're helping, and to
recognize when something has gone wrong versus working as intended.

## Before doing anything: check whether this is actually a safe fit

This is the single most important part of this skill. The technique here has
a real, validated safety mechanism for exactly one kind of change: a source
adding a new, nullable column. It has **not** been validated for:

- Renamed columns, dropped columns, or changed data types (these are a
  different Iceberg schema-evolution mechanism entirely)
- Sources whose Fivetran sync represents updates/deletes as Iceberg delete
  files rather than rewritten data files (merge-on-read) — splicing a source
  like this in today will silently resurrect logically-deleted rows, with no
  error anywhere

Before running the script against anything that matters, ask (or check)
whether any of the source connections do updates or deletes, and whether the
customer expects to rename/retype columns rather than only add new ones. If
the answer is "yes" or "not sure," say so plainly and point to the README's
"What this does NOT yet handle" section rather than proceeding — this
mirrors how the rest of this project's docs treat these gaps: named
directly, never glossed over, never assumed away because a demo happened to
work.

If the sources are genuinely append-only or use in-place row rewrites (not
merge-on-read) and only ever add columns, this technique is a strong fit and
the rest of this skill applies cleanly.

## Step 1 — Confirm prerequisites

You need, from whoever you're helping:

1. A Fivetran destination running on **Managed Data Lake Service** (not a
   warehouse destination) with a Polaris catalog behind it.
2. Polaris OAuth client-credentials for that catalog: the catalog URI, token
   URI, client ID, client secret, scope, and warehouse name. These come from
   the Fivetran dashboard's destination/catalog settings, not from the
   underlying cloud provider — this tool never needs raw AWS/GCS/Azure keys,
   it uses Polaris's vended-credentials support instead (see
   `docs/HOW_IT_WORKS.md` and the README's credentials section if asked why).
3. The list of source **namespaces** (the Polaris/Iceberg namespaces the
   relevant Fivetran connections land into) that should be folded together —
   these must share the same table names and a structurally compatible
   schema. If you don't already know these, list the account's Fivetran
   connections and their destination schemas/namespaces first rather than
   guessing.

## Step 2 — Set up the environment

```bash
pip install -r requirements.txt
cp .env.example .env        # fill in the Polaris credentials from Step 1
cp config.example.yaml config.yaml
```

Edit `config.yaml`: set `target_namespace` (a new namespace name for the
consolidated tables — it doesn't need to exist yet), a `source_id_column`
name that won't collide with a real column in any source, and EITHER list
every `source_namespaces` entry explicitly OR set `source_namespace_pattern`
to a glob (e.g. `"tenant_*"`) if you're dealing with more sources than is
reasonable to hand-enumerate — the pattern gets resolved against the
catalog's real namespaces at run time (see `config.example.yaml` and
`docs/HOW_IT_WORKS.md`'s "Choosing which sources to consolidate" section).
If you use a pattern, always check the resolved namespace list the script
logs before trusting a run against production data — a pattern that's too
broad or too narrow is a real, silent failure mode. Leave `table_workers` /
`source_workers` at their defaults unless you're consolidating at real scale
(dozens of tables and/or sources) and want to tune concurrency — see
`docs/HOW_IT_WORKS.md`'s parallelism section for what each one actually
controls before changing them.

`.env` and `config.yaml` are both gitignored on purpose — never commit
real credentials or a customer's real namespace names.

## Step 3 — Run the consolidation

```bash
python scripts/register_consolidation.py
```

This is safe to re-run — it's idempotent by design (it tracks which files
are already registered and which rows have already been rewritten, so a
second run against unchanged sources is a no-op, and a run against sources
with genuinely new data only processes what's new). Re-running it is the
normal way to pick up new data, not a special recovery mode.

Watch the output for three things as it runs:

- Per-source file counts being spliced in — this is the normal, cheap,
  no-rewrite path and should be the overwhelming majority of activity.
- Any line mentioning a schema **collision** — this means two source
  namespaces independently added a different column that happened to land on
  the same internal Iceberg field id. The script resolves this automatically
  (first-seen name keeps the free path, the other gets a fresh id and a real
  but small rewrite of just its drifted rows) — this is expected occasional
  behavior at scale, not a failure, but it's worth mentioning to whoever
  you're helping so they understand why a run occasionally does more work
  than usual.
- Any actual error or traceback — stop and read `docs/HOW_IT_WORKS.md`'s
  troubleshooting section and this repo's README before retrying blindly;
  a few real gotchas (Polaris silently renumbering field ids on table
  creation, vended-credential token expiry on long runs) are already
  documented there because they were hit for real during development.

## Step 4 — Verify

```bash
python scripts/verify_consolidation.py
```

This independently re-scans the sources and checks the consolidated tables'
row counts against them, checks for duplicate rows (a sign the idempotency
logic failed), and does a basic partition-pruning sanity check. Treat a
clean run of this script as the actual definition of "done," not just the
absence of errors in Step 3 — this project's own validation always favored
independent verification over trusting a script's own success output.

## When you're done

Report back concretely: how many source namespaces and tables were
consolidated, whether any collisions were resolved (and what that means, in
plain terms, for whoever you're helping), and the verification result. If
this is the first real run for a given account, suggest re-running Step 3 on
a schedule (however Fivetran connections already refresh) rather than
treating this as a one-time migration — the whole value proposition here is
that repeated runs stay cheap.
