# Benchmarks: what consolidation actually buys you

Measured 2026-09-22 against the live Fivetran-managed Polaris catalog, on
the `shipments` table across the eight `synth_src_changing_*` source
namespaces consolidated into `for_jack_consolidated.shipments`
(145 files, ~492k rows at time of measurement).

Reproduce with:

```bash
python scripts/bench_vs_union.py --table shipments --with-duckdb
```

**Headline: the README's original pitch was wrong about where the win comes
from.** It claimed a `UNION ALL` view "re-scans every source's full data on
every single query." Against a competent query optimizer that is false. The
real, and still substantial, advantage of this tool is **planning cost**,
not data-scan volume. The README has been corrected accordingly.

---

## 1. Data-scan cost cannot differ. This is structural.

Worth stating before any numbers, because it makes one whole category of
claim unavailable to this tool: no-rewrite consolidation points the target
manifest at **the same physical Parquet files** a `UNION ALL` over the same
sources would read. Same bytes, same files, same row groups. There is no
mechanism by which scanning the consolidated table can move less data than
scanning the union.

Any "we read less data" claim about this technique is therefore either
wrong, or is really a claim about *pruning* — which, as section 3 shows, a
union also gets.

## 2. Planning cost: consolidation wins, 11x, and it scales with source count

Pure catalog and manifest work — how long from "I want this data" to "here
is the file list." No data read.

| approach | files | MB | rows | plan ms |
|---|---:|---:|---:|---:|
| consolidated, no filter | 145 | 44.9 | 491,878 | **672** |
| consolidated, filter 1 source | 22 | 6.4 | 69,924 | **305** |
| UNION ALL, no filter | 156 | 45.5 | 497,650 | **7,567** |
| UNION ALL, filter (branch-pruned) | 24 | 6.5 | 70,699 | **894** |

- **11.3x** cheaper to plan unfiltered, **~3x** filtered.
- Identity-partition pruning on the consolidated table works: 22/145 files
  (15.2%), 14.2% of rows.
- The union must issue 8 `loadTable` calls and traverse 8 manifest trees;
  the consolidated table issues one. **This gap grows roughly linearly with
  source count** — at 400 tenants rather than 8 it is the whole ballgame.

The small file-count difference (145 vs 156, 492k vs 498k rows) is *not* a
layout difference. It is staleness — see section 4.

## 3. The decisive test: a `UNION ALL` prunes too

The union's branches each tag rows with a literal (`'synth_src_changing_03'
AS source_connection_id`). The question is whether an optimizer
constant-folds that literal and eliminates branches that cannot satisfy a
filter on it. DuckDB 1.5.5, same data, materialized locally so the
measurement isolates optimizer behavior from catalog and S3 latency:

| filter | approach | ms | source files in plan |
|---|---|---:|---:|
| on `source_connection_id` | union | 2.2 | **1 / 8** |
| on `source_connection_id` | consolidated | 1.8 | 1 / 8 |
| on another column (`status`) | union | 2.8 | 8 / 8 |
| on another column (`status`) | consolidated | 1.7 | single glob scan¹ |

¹ A hive-partitioned glob scan does not name individual files in the plan,
so that cell is not evidence of pruning. With a non-source filter both
approaches must read everything.

The filtered union's plan collapses to a single scan node:

```
│        READ_PARQUET       │
│   /synth_src_changing_03  │
│          .parquet         │
```

Seven of eight branches are gone. **The union prunes as well as the
identity partition does.**

So the honest summary of the query-time story:

- **Filter on the source column:** union and consolidated do the same work.
- **Filter on anything else:** both read everything. No advantage either way.
- **No filter:** both read everything; the union pays extra for `UNION ALL`
  operator overhead (6.4 ms vs 1.9 ms on this data), which is real but
  small and does not scale with source count the way planning does.

Caveat: **DuckDB is not Snowflake.** Whether Snowflake folds literals in
union branches the same way is untested and worth confirming before
repeating any of this to a customer on Snowflake.

## 4. Staleness is real and currently observable

Per source, rows in the consolidated table vs rows live at the source:

| source | consolidated | live | delta |
|---|---:|---:|---:|
| synth_src_changing_01 | 97,710 | 99,501 | +1,791 |
| synth_src_changing_02 | 61,464 | 61,464 | 0 |
| synth_src_changing_03 | 69,924 | 70,699 | +775 |
| synth_src_changing_04 | 58,554 | 58,554 | 0 |
| synth_src_changing_05 | 30,175 | 31,519 | +1,344 |
| synth_src_changing_06 | 65,312 | 65,418 | +106 |
| synth_src_changing_07 | 63,882 | 64,110 | +228 |
| synth_src_changing_08 | 44,857 | 46,385 | +1,528 |
| **total** | **491,878** | **497,650** | **+5,772** |

Six of eight sources drifted. All deltas positive — new files appended at
the source, not yet spliced — which is the benign shape: a re-run fixes it
and nothing is broken or duplicated.

The dangerous shape is the opposite one, and it is still unguarded: if a
source's own file maintenance (compaction, snapshot expiry, orphan cleanup)
*deletes* a physical file the consolidated manifest points at, queries fail
outright. The safety property is a race:

> **consolidation interval < source snapshot retention / orphan-cleanup interval**

Compaction does not break the target immediately — the pre-compaction files
survive until the source's retention policy actually deletes them, and
during that window the consolidated table serves correct (if stale) data.
Finding Fivetran MDLS's actual retention setting converts this from an
unbounded worry into arithmetic. This remains an open gap; see the
CHANGELOG's "Explicitly out of scope" list.

## 5. Independent corroboration from a customer workload

A customer benchmarked this against their production read path on
2026-09-22 (tool @ `e6a2287`, pyiceberg 0.11.1, DuckDB 1.5.2 with the
iceberg + httpfs extensions). Their existing approach is exactly the
alternative this tool replaces: per-source `iceberg_scan` combined with
`UNION ALL BY NAME`, reading from Fivetran MDLS. Ten sources, Google Search
Console, all with an identical field-id → (name, type) mapping.

Correctness: row count and content hash **byte-identical** to their
baseline for every tenant, through both the S3 and Polaris read paths.

| Variant | Median | Min | vs. baseline |
|---|---|---|---|
| `union` (baseline) | 128.4s | 127.7s | — |
| `unified_s3` | 102.7s | 102.3s | **−20%** |
| `unified_polaris` | 100.2s | 99.1s | **−22%** |
| `union_1org` (baseline) | 30.5s | 29.7s | — |
| `unified_s3_1org` | 18.6s | 17.7s | **−39%** |

Plus a cost their baseline pays that these timings exclude: a compile-time
metadata discovery glob, ~21s for this table and ~5s for GA4. The Polaris
read path does not need that step at all.

**This sharpens §3 rather than contradicting it.** Their own read is that
both variants read the same Parquet files, so the savings come from
per-source overhead — one scan and one metadata resolution per source —
not from scan throughput. That is the same conclusion §2 reaches from
planning cost alone, now confirmed in end-to-end wall clock on a real
workload.

The single-source case is the instructive one. In §3's local test DuckDB
folded the literal and eliminated seven of eight branches, which made the
union look nearly free. Here, filtering to one org still gained **39%** —
because with per-source `iceberg_scan`, each branch resolves its own
Iceberg metadata whether or not its rows survive the filter. Branch
elimination on a literal does not save you the per-source metadata
resolution that a real multi-table union pays.

So the honest refinement: a union's *data* pruning can match the identity
partition, but its *metadata* cost scales with source count no matter what
the optimizer does. That is the durable advantage, and at ten sources it is
worth 20–39% of end-to-end query time.

## 6. Where this leaves the pitch

Strongest framing, supported by measurement:

> One stable object per table type, with ~11x cheaper query planning that
> improves as tenant count grows, and no data duplication.

Framings to retire:

- ~~"avoids re-scanning every source's data"~~ — a union prunes too.
- ~~"reads less data than a union"~~ — structurally impossible; same files.

The open question that actually decides this for a given customer is
**which optimizer they run**. Against one that folds union literals, this
tool's advantage is planning cost and operational tidiness. Against one that
does not, the union genuinely does re-scan everything and this tool wins
outright.
