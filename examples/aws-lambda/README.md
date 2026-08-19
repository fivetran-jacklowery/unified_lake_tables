# Example: running this on AWS Lambda

A minimal, real example of hosting `register_consolidation.py` as a
scheduled AWS Lambda function instead of running it by hand or from a
server you maintain. This is one of several reasonable places to run this
tool -- see the main [README](../../README.md) and
[HOW_IT_WORKS.md](../../docs/HOW_IT_WORKS.md) for the mechanism itself,
which is completely unchanged here. This directory only adds a thin
Lambda handler and a packaging script around it.

**Why Lambda is a reasonable fit for this specific tool:** consolidation
runs are short (a real test run consolidating 100 tables across 3 sources
completed in under a minute) and infrequent (once per sync cycle, not
continuously), so paying for an always-on server is real waste. A
serverless scheduled function costs nothing between runs and needs no
infrastructure to babysit.

**Why this Lambda's own IAM role needs almost no permissions --** and this
is worth understanding, not just configuring: `register_consolidation.py`
never uses this Lambda's AWS identity to touch S3 at all. It authenticates
to Polaris using OAuth client-credentials, and Polaris vends short-lived,
scoped AWS storage credentials automatically on every table load (see
["Credentials: vended, not static"](../../docs/HOW_IT_WORKS.md#credentials-vended-not-static)).
This function's execution role only needs enough to run and write its own
CloudWatch Logs -- the managed `AWSLambdaBasicExecutionRole` policy is
sufficient. There is no S3 policy to write, because there's no static AWS
credential in this picture to scope one to.

## Files here

| File | Purpose |
|---|---|
| `lambda_function.py` | The handler. Builds config from Lambda environment variables (not `config.yaml`) and calls the same `run()` function the CLI uses. |
| `requirements.txt` | Same pinned dependencies as the repo root, packaged for this function. |
| `build.sh` | Installs dependencies into a `build/` directory, copies in the handler + `scripts/register_consolidation.py`, trims a few genuinely-unused pyarrow components, and zips it. |

## Prerequisites

- Everything the main [README](../../README.md) already requires (a
  Fivetran MDLS destination, Polaris OAuth client-credentials, 2+ source
  namespaces sharing table names).
- An AWS account and the `aws` CLI configured with permission to create an
  IAM role, a Lambda function, and an EventBridge rule.
- Python 3.12 and `pip`, to build the deployment package.
- **Docker, if you're not building on Linux/arm64 already** -- see
  "A note on package size and architecture" below.

## Step 1 -- build the deployment package

```bash
./build.sh
```

This produces `deployment.zip`. Real, measured numbers from building this
exact package (pyiceberg[pyarrow]==0.11.1, pinned in `requirements.txt`):

- **Untrimmed: 244MB unzipped, 81MB zipped.**
- The trim step in `build.sh` removes `pyarrow/tests`, `pyarrow/include`,
  and `pyarrow/libarrow_flight.so` (Arrow Flight RPC -- this tool never
  uses it). Removing Flight specifically was verified safe with a real
  import test (`pyarrow`, `pyarrow.parquet`, `pyarrow.dataset`, and every
  pyiceberg symbol this tool imports, all still import cleanly with it
  gone). **Do not** also remove `pyarrow/libarrow_substrait.so` --
  pyarrow's own core import depends on it at load time even though this
  tool never calls Substrait functionality directly; removing it breaks
  the import entirely (confirmed by trying it).
- Together those three removals total roughly 37MB (`pyarrow/tests` ~4MB +
  `pyarrow/include` ~5MB + `libarrow_flight.so` ~27MB), which would bring a
  clean build to roughly **207MB unzipped**. `build.sh` prints the actual
  before/after size every time you run it -- some filesystems (a few CI
  runners, certain synced/mounted dev folders) block deleting these
  specific files even though the untrimmed package works fine; the script
  tells you plainly if that happened on your machine instead of silently
  reporting a number that isn't real.

**Either way, read this before deploying:**

- **81MB is over Lambda's 50MB direct `--zip-file` upload limit.** You
  must upload through S3 first (Step 2 below handles this).
- **244MB (or ~207MB trimmed) is under Lambda's 250MB unzipped ceiling,
  but not by a lot.** Re-run `./build.sh` and check the printed size again
  after bumping `pyiceberg`/`pyarrow` versions. If a future version pushes
  this over 250MB, the fix is switching to a container-image-based Lambda
  (`aws lambda create-function --package-type Image`) instead of a zip --
  that has a much larger 10GB limit and sidesteps this ceiling entirely.

### A note on package size and architecture

pyarrow ships compiled, platform-specific binaries, so the package you
build must match your Lambda function's configured architecture. **arm64
is recommended** -- it's cheaper per millisecond in Lambda, and this
example's measured sizes above were built for it.

If you're building on Linux/arm64 already, `./build.sh`'s plain `pip
install` is correct as-is. If you're building on macOS, Windows, or want a
guaranteed-correct result regardless of your own machine, build inside the
official Lambda base image instead (this is what `build.sh`'s header
comment documents):

```bash
docker run --rm -v "$PWD":/var/task -w /var/task \
  public.ecr.aws/sam/build-python3.12:latest-arm64 \
  pip install -r requirements.txt -t build
```

then let the rest of `build.sh` (copy handler, trim, zip) run against that
same `build/` directory.

## Step 2 -- create the IAM role

```bash
aws iam create-role \
  --role-name unified-lake-tables-lambda \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]
  }'

aws iam attach-role-policy \
  --role-name unified-lake-tables-lambda \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```

That's the whole policy. As explained above, this function's own AWS
identity never touches S3 or Iceberg -- Polaris vends those credentials
per table load, so there's nothing else to grant here.

## Step 3 -- upload the package and create the function

The zip is too large for a direct `--zip-file` upload, so upload it to S3
first:

```bash
aws s3 cp deployment.zip s3://YOUR_BUCKET/unified-lake-tables/deployment.zip

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws lambda create-function \
  --function-name unified-lake-tables-consolidation \
  --runtime python3.12 \
  --architectures arm64 \
  --role "arn:aws:iam::${ACCOUNT_ID}:role/unified-lake-tables-lambda" \
  --handler lambda_function.handler \
  --code S3Bucket=YOUR_BUCKET,S3Key=unified-lake-tables/deployment.zip \
  --timeout 300 \
  --memory-size 512 \
  --environment "Variables={
    POLARIS_CATALOG_URI=https://your-polaris-catalog-uri,
    POLARIS_WAREHOUSE=your_warehouse,
    POLARIS_CLIENT_ID=your_client_id,
    POLARIS_CLIENT_SECRET=your_client_secret,
    POLARIS_TOKEN_URI=https://your-token-endpoint,
    TARGET_NAMESPACE=consolidated,
    SOURCE_NAMESPACE_PATTERN=tenant_*,
    SOURCE_ID_COLUMN=source_connection_id,
    TABLE_WORKERS=8,
    SOURCE_WORKERS=8
  }"
```

Environment variables this function reads (see `lambda_function.py`'s
`build_config_from_env()`):

| Variable | Required | Notes |
|---|---|---|
| `POLARIS_CATALOG_URI` | yes | Same as `.env`'s `POLARIS_CATALOG_URI` in the CLI setup. |
| `POLARIS_WAREHOUSE` | yes | |
| `POLARIS_CLIENT_ID` | yes | |
| `POLARIS_CLIENT_SECRET` | yes | See "Beyond this example" below before using this in anything but a first test. |
| `POLARIS_TOKEN_URI` | recommended | Same deprecation-avoidance reason as the CLI's `.env.example`. |
| `TARGET_NAMESPACE` | yes | |
| `SOURCE_NAMESPACES` | yes, unless using the pattern below | Comma-separated (this is the one shape difference from `config.yaml`'s YAML list). Fine for a handful of sources. |
| `SOURCE_NAMESPACE_PATTERN` | yes, unless using the explicit list above | A glob (`tenant_*`, not SQL `LIKE`) resolved against the catalog's real namespaces on every run -- use this instead of `SOURCE_NAMESPACES` once you have more sources than is reasonable to hand-list, or want new tenant namespaces picked up automatically without a config change. Set exactly one of these two, not both. The resolved list is always logged -- check CloudWatch Logs after a pattern-based run's first invoke to confirm it matched what you expected. |
| `SOURCE_ID_COLUMN` | yes | |
| `TABLE_WORKERS` / `SOURCE_WORKERS` | no | Default to 8, same as `load_config()`'s defaults. |

**Update code on a later change** with `aws lambda update-function-code
--function-name unified-lake-tables-consolidation --s3-bucket YOUR_BUCKET
--s3-key unified-lake-tables/deployment.zip` after re-uploading a rebuilt
zip.

## Step 4 -- test it once, manually, before scheduling it

```bash
aws lambda invoke --function-name unified-lake-tables-consolidation \
  --payload '{}' --cli-binary-format raw-in-base64-out response.json
cat response.json
```

Pass `{"tables": ["orders"]}` as the payload instead of `{}` to restrict a
test run to one table, mirroring the CLI's optional `tables` arguments.
Check `response.json` (the same summary dict `run()` returns: tables,
sources, total spliced/rewritten, wall clock) and CloudWatch Logs
(`/aws/lambda/unified-lake-tables-consolidation`) for the detailed
per-table log lines.

## Step 5 -- schedule it with EventBridge

```bash
aws events put-rule \
  --name unified-lake-tables-schedule \
  --schedule-expression "rate(6 hours)"

aws lambda add-permission \
  --function-name unified-lake-tables-consolidation \
  --statement-id unified-lake-tables-eventbridge \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:$(aws configure get region):${ACCOUNT_ID}:rule/unified-lake-tables-schedule"

aws events put-targets \
  --rule unified-lake-tables-schedule \
  --targets "Id"="1","Arn"="arn:aws:lambda:$(aws configure get region):${ACCOUNT_ID}:function:unified-lake-tables-consolidation"
```

Adjust `rate(6 hours)` to whatever cadence matches your sources' own sync
frequency -- see the main README's "schedule it however you'd schedule any
batch job" note. `register_consolidation.py`'s idempotency guarantees (see
[HOW_IT_WORKS.md](../../docs/HOW_IT_WORKS.md#idempotency)) mean an
unnecessary run against unchanged sources is a safe, fast no-op, not a
correctness risk -- err on the side of running this more often than
strictly necessary rather than less.

**One constraint that applies regardless of where this runs:** this
repo's [CHANGELOG](../../CHANGELOG.md) notes that concurrent/overlapping
registration runs against the same target are untested. A single
EventBridge scheduled rule invoking a single Lambda function naturally
avoids this as long as your schedule interval is comfortably longer than
one run takes -- don't also wire this function to a second trigger that
could overlap with the scheduled one.

## Beyond this example

This is a starting point, not a hardened production deployment. Before
running this against a real customer's data long-term, consider:

- **Move `POLARIS_CLIENT_SECRET` to AWS Secrets Manager** instead of a
  plain Lambda environment variable, and fetch it at the top of
  `handler()`. Environment variables are encrypted at rest but are visible
  to anyone with `lambda:GetFunctionConfiguration` on this function;
  Secrets Manager adds access logging and rotation.
- **VPC configuration**, if your Polaris catalog or its underlying S3
  storage aren't reachable from Lambda's default public networking path.
- **Alerting on failure** -- wire this function's errors (or a non-zero
  `total_rewritten` count you didn't expect) to an SNS topic or CloudWatch
  Alarm rather than only checking logs after the fact.
- **Concurrency controls** (Lambda reserved concurrency = 1 for this
  function) if you have any risk of a second trigger overlapping with the
  scheduled one, given the untested-concurrent-runs limitation noted above.
