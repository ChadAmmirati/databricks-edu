"""
Reusable PySpark transforms for the Silver layer.

These cover the slice of EDU's dbt macro library that ~95% of staging models
actually use:
    extract_descriptor()                → extract_descriptor()
    dbt_utils.generate_surrogate_key()  → surrogate_key()
    dbt_utils.deduplicate()             → dedupe_latest()
"""
from __future__ import annotations

from typing import List

from pyspark.sql import Column, DataFrame, functions as F
from pyspark.sql.window import Window


def get_json(col: Column, path: str) -> Column:
    """
    Pull a field out of the bronze 'payload' JSON. `path` uses jsonpath-style
    syntax accepted by Spark's get_json_object, e.g. '$.studentReference.studentUniqueId'.
    """
    return F.get_json_object(col, path)


def extract_descriptor(col: Column) -> Column:
    """
    Resolve an Ed-Fi descriptor reference to its code value.

    Ed-Fi descriptors are URIs like 'uri://ed-fi.org/SexDescriptor#Female'; this
    returns the part after '#' (the Ed-Fi codeValue). Mirrors EDU's
    `extract_descriptor` macro.

    IDEMPOTENT: a value that is not a descriptor URI — e.g. an already-resolved
    label ('Female') or a plain code — is returned unchanged. This lets Silver
    resolve descriptor columns to human labels (see `resolve_descriptors`) while
    Gold keeps calling `extract_descriptor` on the same columns without
    double-processing, and keeps key-feeding descriptors as their codeValue.
    """
    return (
        F.when(col.isNull(), F.lit(None))
         .when(col.contains("://"), F.element_at(F.split(col, "#"), -1))
         .otherwise(col)
    )


def resolve_descriptors(df: DataFrame, lookup: DataFrame, columns: List[str]) -> DataFrame:
    """
    Replace each Ed-Fi descriptor-URI column in `columns` with its human-readable
    label from `lookup` — the Silver `_descriptors` table, with columns
    `descriptor_uri` and `short_description`.

    A value with no matching descriptor row falls back to its bare code value
    (`extract_descriptor`) — NEVER NULL — so a code missing from the ODS's
    descriptor tables degrades to the code instead of an empty cell. `lookup`
    is de-duplicated on `descriptor_uri`, so each left join is 1:1 (no fan-out).
    """
    lk = F.broadcast(
        lookup.select("descriptor_uri", "short_description").dropDuplicates(["descriptor_uri"])
    )
    for c in columns:
        u, s = f"__d_uri_{c}", f"__d_sd_{c}"
        l2 = lk.select(F.col("descriptor_uri").alias(u), F.col("short_description").alias(s))
        df = (
            df.join(l2, df[c] == F.col(u), "left")
              .withColumn(c, F.coalesce(F.col(s), extract_descriptor(F.col(c))))
              .drop(u, s)
        )
    return df


def chronic_absent_flag(attendance_rate: Column, total_days: Column, threshold_pct: float, min_days: int) -> Column:
    """Chronic-absenteeism flag: a student enrolled at least `min_days` who was
    present for less than `threshold_pct`% of them (i.e. absent >= 100-threshold_pct%).
    Mirrors EDU's chronic-absence rule (TN defaults: threshold 90, min_days 20).
    Null-safe: too few days, or a null rate, yields False (never NULL)."""
    return F.when(
        (total_days >= F.lit(min_days))
        & attendance_rate.isNotNull()
        & (attendance_rate < F.lit(threshold_pct / 100.0)),
        F.lit(True),
    ).otherwise(F.lit(False))


def program_window_flags(begin_col: Column, end_col: Column, year_int_col: Column):
    """(is_annual, is_active) for a dated program association. The academic year
    runs (year-1)-07-01 .. year-06-30. `is_annual`: the [begin, end] window overlaps
    that year (served at any point). `is_active`: the association is still open as of
    year-end (exited students excluded). Mirrors EDU's agg_types ['annual','active'].
    Returns a (is_annual, is_active) tuple of Columns."""
    yb = F.make_date(year_int_col - F.lit(1), F.lit(7), F.lit(1))
    ye = F.make_date(year_int_col, F.lit(6), F.lit(30))
    is_annual = (begin_col <= ye) & (end_col.isNull() | (end_col >= yb))
    is_active = (begin_col <= ye) & (end_col.isNull() | (end_col >= ye))
    return is_annual, is_active


def race_ethnicity_rollup(race_codes: Column, hispanic: Column,
                          multiple_label: str, latinx_label: str, unknown_label: str) -> Column:
    """Federal race/ethnicity rollup from a (distinct) array of race code values
    plus a Hispanic/Latino ethnicity flag: Hispanic -> latinx_label (ethnicity
    reported first), else >1 race -> multiple_label, else the single race, else
    unknown_label. Null hispanic is treated as not-Hispanic; null/empty races as
    unknown."""
    n = F.size(F.coalesce(race_codes, F.array()))
    return (
        F.when(hispanic == True, F.lit(latinx_label))   # noqa: E712 (Spark Column truth)
         .when(n > 1, F.lit(multiple_label))
         .when(n == 1, F.element_at(race_codes, 1))
         .otherwise(F.lit(unknown_label))
    )


def surrogate_key(*cols, lowercase: bool = True) -> Column:
    """
    Stable surrogate key over the given columns. Matches dbt_utils.generate_surrogate_key:
    md5(concat_ws('-', coalesce(cast(c as string), '_dbt_utils_surrogate_key_null_'), ...)).
    """
    NULL_MARKER = "_dbt_utils_surrogate_key_null_"
    parts: List[Column] = []
    for c in cols:
        c = F.col(c) if isinstance(c, str) else c
        s = F.coalesce(c.cast("string"), F.lit(NULL_MARKER))
        if lowercase:
            s = F.lower(s)
        parts.append(s)
    return F.md5(F.concat_ws("-", *parts))


def dedupe_latest(df: DataFrame, partition_by: List[str], order_by: List[str]) -> DataFrame:
    """Keep the most recent row per partition. Mirrors dbt_utils.deduplicate."""
    win = Window.partitionBy(*[F.col(c) for c in partition_by]).orderBy(*[F.col(c).desc() for c in order_by])
    return (
        df.withColumn("_rn", F.row_number().over(win))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )
