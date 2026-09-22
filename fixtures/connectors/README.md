# Test fixtures: Fivetran Connector SDK sources

Purpose-built connectors that manufacture the exact source conditions two
correctness bugs needed. Neither could be reproduced with ordinary sources,
which is why they exist: the general-purpose synthetic fixtures are
byte-identical by construction, so they never disagree about anything.

Each pair is deployed as two separate Fivetran connections whose schemas
differ in one specific, deliberate way.

| Fixture | Reproduces | Mechanism |
|---|---|---|
| `synth_collide_01` / `_02` | Collision rewrite appending one duplicate copy per drifted file | Same base table; each grows ONE new column with a *different name* (`alpha_metric` vs `beta_flag`), emitted as an undeclared extra key on sync 2. Both land on the same field id. |
| `synth_order_01` / `_02` | Base-schema field-id divergence (customer-reported) | Identical column names and types; only the *declaration order* differs, so `clicks`/`ctr` swap field ids between the two sources. |

Both are documented in `CHANGELOG.md` with the measured before/after.

## Running them

```bash
cd fixtures/connectors/synth_collide_01
pip install -r requirements.txt
fivetran debug --configuration configuration.json    # local, writes to files/warehouse.db
```

To deploy against a real destination:

```bash
fivetran deploy --api-key "$(printf '%s:%s' "$FIVETRAN_API_KEY" "$FIVETRAN_API_SECRET" | base64)" \
  --destination "$FIVETRAN_DESTINATION_NAME" \
  --connection synth_collide_01 \
  --configuration configuration.json \
  --python-version 3.12 --yes
```

Two things that cost time the first time round:

- **SDK-deployed connections are created paused.** A sync trigger does
  nothing until you resume the connection (`PATCH /v1/connections/{id}`
  with `{"paused": false}`), which also kicks off the initial sync.
- **`synth_collide_*` needs two syncs.** Sync 1 lands the base rows with no
  drift column; sync 2 adds the column and enough rows to span **several
  files**, which is the condition the duplication bug requires. Check the
  file count before consolidating — with only one drifted file the bug
  cannot appear.

`configuration.json` holds nothing but a seed. These fixtures need no
credentials of their own.
