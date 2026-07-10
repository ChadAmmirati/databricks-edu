"""
Silver Spark Declarative Pipeline.

For every resource declared in `resources.yml`, defines two SDP tables:

    silver.{name}_base   — typed columns extracted from the raw JSON payload
                            plus a normalized `payload` column for downstream
                            ad-hoc access. EDU equivalent: base_ef3__{name}.
    silver.{name}        — deduplicated by surrogate key, with /deletes
                            tombstones anti-joined out. EDU equivalent:
                            stg_ef3__{name} (incl. its delete post-hook).

Keys are built from the manifest's `key_paths` — full JSON paths validated
against the Ed-Fi 7.3 OpenAPI spec — NEVER by guessing that a key field is
top-level. Extraction recipes below are likewise spec-validated; a recipe
column that doesn't resolve on real data is a bug, not a tolerated fallback.
"""
from __future__ import annotations

from typing import Callable, Dict, List

import dlt  # Lakeflow Spark Declarative Pipelines runtime
from pyspark.sql import DataFrame, SparkSession, functions as F

from silver.recipes import RECIPES
from silver.transforms import get_json, surrogate_key, dedupe_latest, resolve_descriptors


def _spark() -> SparkSession:
    s = SparkSession.getActiveSession()
    if s is None:
        s = SparkSession.builder.getOrCreate()
    return s


# Read once when SDP first calls a table function. Cached in module globals.
_CONFIG = {}

def _conf(key: str, default: str = "") -> str:
    if not _CONFIG:
        s = _spark()
        _CONFIG["edu.catalog"] = s.conf.get("edu.catalog", "edu_demo")
        _CONFIG["edu.bronze_schema"] = s.conf.get("edu.bronze_schema", "edu_bronze")
        _CONFIG["edu.silver_schema"] = s.conf.get("edu.silver_schema", "edu_silver")
    return _CONFIG.get(key) or _spark().conf.get(key, default)


def _load_resources() -> List[dict]:
    import yaml, os
    here = os.path.dirname(os.path.abspath(__file__))
    manifest = os.path.normpath(os.path.join(here, "..", "edfi_extract", "resources.yml"))
    with open(manifest) as f:
        return yaml.safe_load(f)["resources"]


def _norm_guid(col):
    """Match deletes GUIDs to record GUIDs: lowercase, dashes stripped."""
    return F.lower(F.regexp_replace(F.coalesce(col, F.lit("")), "-", ""))


# Descriptor columns that feed surrogate keys (k_program, k_graduation_plan,
# k_grading_period) MUST stay as their Ed-Fi codeValue so our keys match EDU's
# dbt_utils keys 1:1. Every OTHER descriptor column is resolved to its human
# label in Silver; these are left as the raw URI (Gold's extract_descriptor
# turns them into the codeValue, unchanged).
_KEY_DESCRIPTOR_COLS = {
    "program_type_descriptor",
    "graduation_plan_type_descriptor",
    "grading_period_descriptor",
}


@dlt.table(
    name="_descriptors",
    comment="Union of all Ed-Fi *Descriptors endpoints (descriptor_uri -> short_description); "
            "lets Silver resolve descriptor codes to human labels. EDU equivalent: edu_edfi_source descriptor seeds.",
    table_properties={"quality": "silver"},
)
def _descriptors():
    catalog = _conf("edu.catalog")
    bronze = _conf("edu.bronze_schema")
    names = [r["name"] for r in _load_resources() if r["name"].endswith("_descriptors")]
    schema = ("descriptor_resource STRING, namespace STRING, code_value STRING, "
              "short_description STRING, description STRING")
    dfs = []
    for name in names:
        raw = _spark().read.table(f"`{catalog}`.`{bronze}`.`{name}_raw`")
        dfs.append(
            raw.select(
                F.lit(name).alias("descriptor_resource"),
                get_json(F.col("payload"), "$.namespace").alias("namespace"),
                get_json(F.col("payload"), "$.codeValue").alias("code_value"),
                get_json(F.col("payload"), "$.shortDescription").alias("short_description"),
                get_json(F.col("payload"), "$.description").alias("description"),
            )
        )
    out = dfs[0] if dfs else _spark().createDataFrame([], schema)
    for d in dfs[1:]:
        out = out.unionByName(d)
    return (
        out.where(F.col("namespace").isNotNull() & F.col("code_value").isNotNull())
           .withColumn("descriptor_uri", F.concat(F.col("namespace"), F.lit("#"), F.col("code_value")))
           .dropDuplicates(["descriptor_uri"])
    )


def _make_base_table(resource: dict) -> Callable[[], DataFrame]:
    name = resource["name"]
    recipe = RECIPES.get(name, {})

    @dlt.table(
        name=f"{name}_base",
        comment=f"Typed unpacking of bronze.{name}_raw. Replaces EDU dbt model base_ef3__{name}.",
        table_properties={"quality": "silver"},
    )
    def _base():
        catalog = _conf("edu.catalog")
        bronze = _conf("edu.bronze_schema")
        df = _spark().read.table(f"`{catalog}`.`{bronze}`.`{name}_raw`")
        cols = [
            F.col("tenant_code"),
            F.col("api_year"),
            F.col("pull_timestamp"),
            F.col("pull_change_version"),
            F.col("record_guid"),
            F.col("payload"),
        ]
        for silver_col, json_path in recipe.items():
            cols.append(get_json(F.col("payload"), json_path).alias(silver_col))
        df = df.select(*cols)
        # Resolve descriptor URIs to human labels via the `_descriptors` lookup,
        # EXCEPT key-feeding descriptors (kept as codeValue for EDU key parity).
        descriptor_cols = [
            c for c in recipe
            if c.endswith("_descriptor") and c not in _KEY_DESCRIPTOR_COLS
        ]
        if descriptor_cols:
            df = resolve_descriptors(df, dlt.read("_descriptors"), descriptor_cols)
        return df

    return _base


def _make_stage_table(resource: dict) -> Callable[[], DataFrame]:
    name = resource["name"]
    key_paths = resource.get("key_paths") or []

    expectations = {
        "tenant_code_not_null": "tenant_code IS NOT NULL",
        "api_year_not_null":    "api_year IS NOT NULL",
    }
    if key_paths:
        # The surrogate key hashes coalesced parts, so it is never NULL — the
        # meaningful assertion is that the RAW key parts resolved. A key part
        # that is NULL on every row means the manifest path is wrong for this
        # ODS version (the bug class that collapses tables).
        expectations[f"k_{name}_parts_resolved"] = "_k_null_parts < " + str(len(key_paths))

    @dlt.table(
        name=name,
        comment=f"Deduplicated staging for {name} (deletes applied). Replaces EDU dbt model stg_ef3__{name}.",
        table_properties={"quality": "silver"},
    )
    @dlt.expect_all(expectations)
    def _stage():
        df = dlt.read(f"{name}_base")

        if key_paths:
            parts = [get_json(F.col("payload"), f"$.{p}") for p in key_paths]
            null_count = sum(F.when(p.isNull(), 1).otherwise(0) for p in parts)
            df = df.withColumn(
                f"k_{name}",
                surrogate_key(F.col("tenant_code"), F.col("api_year"), *parts),
            ).withColumn("_k_null_parts", null_count)
            df = dedupe_latest(
                df,
                partition_by=[f"k_{name}"],
                order_by=["pull_change_version", "pull_timestamp"],
            )

        # Apply /deletes tombstones (EDU: stg post-hook DELETE on is_deleted).
        catalog = _conf("edu.catalog")
        bronze = _conf("edu.bronze_schema")
        deletes = (
            _spark().read.table(f"`{catalog}`.`{bronze}`.`_deletes_raw`")
            .where(F.col("resource") == name)
            .select(
                F.col("tenant_code").alias("_d_tenant"),
                F.col("api_year").alias("_d_year"),
                _norm_guid(F.col("record_guid")).alias("_d_guid"),
            )
            .distinct()
        )
        df = df.join(
            deletes,
            on=[
                df["tenant_code"] == deletes["_d_tenant"],
                df["api_year"] == deletes["_d_year"],
                _norm_guid(df["record_guid"]) == deletes["_d_guid"],
            ],
            how="left_anti",
        )
        return df

    return _stage


# ----- Generate one base + one stage table per resource -----
for _r in _load_resources():
    # Descriptor endpoints feed the `_descriptors` lookup (above), not a
    # base/stage pair — skip generic table generation for them.
    if _r["name"].endswith("_descriptors"):
        continue
    _make_base_table(_r)
    _make_stage_table(_r)
