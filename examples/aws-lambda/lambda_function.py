"""lambda_function.py -- AWS Lambda handler that runs register_consolidation.py
on a schedule (via EventBridge), instead of as a CLI invocation.

This is a thin adapter, not a reimplementation. All of the actual
consolidation logic (schema reservation, no-rewrite splicing, drift
detection, collision-only rewrite) lives in scripts/register_consolidation.py
and is unchanged here -- see that file and docs/HOW_IT_WORKS.md for the
mechanism. This handler exists to answer one question: how does a customer
run that script on a schedule without babysitting a server?

Two things are deliberately different from the CLI version, both because a
Lambda execution environment isn't a persistent server you control:

  1. Config comes from Lambda environment variables, not config.yaml. There's
     no reason to bundle a customer's real namespace list into the deployment
     package when the Lambda console/CLI already has a first-class place to
     set that (and Polaris credentials) as environment variables -- see
     build_config_from_env() below, and README.md for exactly which
     variables to set.
  2. There's no .env file to load_dotenv() from. Polaris OAuth
     client-credentials and everything else register_consolidation.py needs
     from the environment should be set directly on the Lambda function's
     configuration (or, for anything beyond a first example, pulled from AWS
     Secrets Manager into the environment at cold start -- see the
     "Beyond this example" section of README.md).

Notably NOT different: this Lambda's own IAM execution role needs no S3 or
Iceberg permissions at all. register_consolidation.py never uses the
Lambda's AWS identity to touch S3 -- it authenticates to Polaris via OAuth,
and Polaris vends short-lived, scoped AWS storage credentials per table load
(see docs/HOW_IT_WORKS.md's "Credentials: vended, not static" section). The
execution role attached to this function only needs enough to run and write
its own CloudWatch Logs (AWSLambdaBasicExecutionRole is sufficient).
"""
import json
import logging
import os

import register_consolidation as rc

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def build_config_from_env() -> dict:
    """Build the same cfg dict register_consolidation.load_config() would
    return from config.yaml, but from Lambda environment variables instead.
    Deliberately does NOT call load_config() -- there's no config.yaml file
    in this deployment package, and there shouldn't be one: a customer's
    real source-namespace list is configuration, not code, and belongs in
    the Lambda function's own config (or Secrets Manager), not committed to
    a repo or baked into a deployment zip.
    """
    target_namespace = os.environ.get("TARGET_NAMESPACE")
    source_namespaces_raw = os.environ.get("SOURCE_NAMESPACES")
    source_id_column = os.environ.get("SOURCE_ID_COLUMN")

    missing = [
        name
        for name, val in [
            ("TARGET_NAMESPACE", target_namespace),
            ("SOURCE_NAMESPACES", source_namespaces_raw),
            ("SOURCE_ID_COLUMN", source_id_column),
        ]
        if not val
    ]
    if missing:
        raise RuntimeError(
            f"Missing required Lambda environment variable(s): {missing}. "
            "See examples/aws-lambda/README.md for the full list of variables "
            "this function needs (Polaris OAuth credentials + these config values)."
        )

    source_namespaces = [ns.strip() for ns in source_namespaces_raw.split(",") if ns.strip()]
    if len(source_namespaces) < 2:
        raise RuntimeError(
            "SOURCE_NAMESPACES must be a comma-separated list of 2 or more namespaces "
            f"(got: {source_namespaces_raw!r})."
        )

    return {
        "target_namespace": target_namespace,
        "source_namespaces": source_namespaces,
        "source_id_column": source_id_column,
        "table_workers": int(os.environ.get("TABLE_WORKERS", "8")),
        "source_workers": int(os.environ.get("SOURCE_WORKERS", "8")),
    }


def handler(event, context):
    """Entry point (set as the function's Handler: lambda_function.handler).

    `event` is whatever triggered this invocation. For the EventBridge
    scheduled-rule case documented in README.md, the event body isn't used --
    every configured source is auto-discovered and registered, same as
    running the CLI with no table-name arguments. For an ad hoc manual test
    invoke, you can pass {"tables": ["orders", "customers"]} to restrict a
    single run to just those table names, mirroring the CLI's optional
    positional `tables` arguments.
    """
    event = event or {}
    cfg = build_config_from_env()
    tables = event.get("tables") or None

    logger.info(
        "Starting registration run: target=%s sources=%s tables=%s",
        cfg["target_namespace"],
        cfg["source_namespaces"],
        tables or "auto-discover",
    )

    summary = rc.run(cfg, tables=tables)
    logger.info("Registration run complete: %s", json.dumps(summary))
    return summary
