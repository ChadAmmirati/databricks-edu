"""Unit tests for silver.transforms — uses a local SparkSession."""
from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession, functions as F  # noqa: E402

from silver.transforms import (  # noqa: E402
    extract_descriptor, surrogate_key, dedupe_latest, get_json, resolve_descriptors,
    chronic_absent_flag, program_window_flags, race_ethnicity_rollup,
)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder
         .master("local[2]")
         .appName("test_transforms")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def test_extract_descriptor_returns_suffix(spark):
    df = spark.createDataFrame([("uri://ed-fi.org/SexDescriptor#Female",), (None,)], "d STRING")
    out = df.select(extract_descriptor(F.col("d")).alias("r")).collect()
    assert [r["r"] for r in out] == ["Female", None]


def test_surrogate_key_is_deterministic_and_md5(spark):
    df = spark.createDataFrame([("demo", 2026, "abc")], "tenant STRING, yr INT, sid STRING")
    out = df.select(surrogate_key("tenant", "yr", "sid").alias("k")).collect()
    assert len(out[0]["k"]) == 32  # md5 hex length
    # determinism
    out2 = df.select(surrogate_key("tenant", "yr", "sid").alias("k")).collect()
    assert out2[0]["k"] == out[0]["k"]


def test_dedupe_latest_keeps_max(spark):
    df = spark.createDataFrame(
        [("a", 1, "old"), ("a", 2, "new"), ("b", 1, "only")],
        "k STRING, ts INT, v STRING",
    )
    out = dedupe_latest(df, partition_by=["k"], order_by=["ts"]).orderBy("k").collect()
    assert [(r["k"], r["v"]) for r in out] == [("a", "new"), ("b", "only")]


def test_get_json_pulls_nested(spark):
    df = spark.createDataFrame([('{"studentReference":{"studentUniqueId":"S1"}}',)], "p STRING")
    out = df.select(get_json(F.col("p"), "$.studentReference.studentUniqueId").alias("s")).collect()
    assert out[0]["s"] == "S1"


def test_extract_descriptor_is_idempotent(spark):
    # URI -> codeValue; an already-resolved label or plain code -> unchanged; null -> null.
    df = spark.createDataFrame(
        [("uri://ed-fi.org/TermDescriptor#Year Round",), ("Year Round",), ("Female",), (None,)],
        "d STRING",
    )
    out = [r["r"] for r in df.select(extract_descriptor(F.col("d")).alias("r")).collect()]
    assert out == ["Year Round", "Year Round", "Female", None]


def test_resolve_descriptors_labels_with_codevalue_fallback(spark):
    df = spark.createDataFrame(
        [("uri://ed-fi.org/SexDescriptor#F",),       # matches lookup -> label
         ("uri://ed-fi.org/SexDescriptor#ZZ",),      # no match -> codeValue fallback
         (None,)],                                   # null stays null
        "sex_descriptor STRING",
    )
    lookup = spark.createDataFrame(
        [("uri://ed-fi.org/SexDescriptor#F", "Female"),
         ("uri://ed-fi.org/SexDescriptor#F", "Female")],  # dup -> deduped, no fan-out
        "descriptor_uri STRING, short_description STRING",
    )
    out = resolve_descriptors(df, lookup, ["sex_descriptor"])
    vals = [r["sex_descriptor"] for r in out.orderBy("sex_descriptor").collect()]
    assert out.count() == 3                 # no row fan-out from the join
    assert "Female" in vals                 # resolved to label
    assert "ZZ" in vals                     # fallback to codeValue (never empty)
    assert None in vals                     # null preserved
    assert not any(v and "://" in v for v in vals)  # no raw URI leaks through


def test_chronic_absent_flag(spark):
    # (attendance_rate, total_days): chronic iff days >= 20 AND rate < 0.90
    df = spark.createDataFrame(
        [(0.80, 40), (0.97, 40), (0.50, 10), (None, 0)],
        "rate DOUBLE, days INT",
    )
    flags = [
        r["c"] for r in df.select(
            chronic_absent_flag(F.col("rate"), F.col("days"), 90, 20).alias("c")
        ).orderBy(F.col("rate").asc_nulls_last()).collect()
    ]
    # ordered by rate asc, nulls last -> [0.50/10d, 0.80/40d, 0.97/40d, null/0d]
    assert flags == [False, True, False, False]


def test_program_window_flags(spark):
    # academic year 2026 = 2025-07-01 .. 2026-06-30; 'active' evaluated at year-end
    df = spark.createDataFrame(
        [("2025-09-01", None),          # ongoing      -> annual T, active T
         ("2025-09-01", "2026-02-15"),  # exited mid-yr -> annual T, active F
         ("2024-09-01", "2025-01-01")], # prior year   -> annual F, active F
        "begin STRING, endd STRING",
    )
    is_annual, is_active = program_window_flags(
        F.col("begin").cast("date"), F.col("endd").cast("date"), F.lit(2026)
    )
    rows = df.select(is_annual.alias("a"), is_active.alias("c")).collect()
    assert [r["a"] for r in rows] == [True, True, False]
    assert [r["c"] for r in rows] == [True, False, False]


def test_race_ethnicity_rollup(spark):
    df = spark.createDataFrame(
        [(["White"], False),            # single race
         (["White", "Asian"], False),   # multi-race -> Multiple
         ([], False),                   # no race    -> Unknown
         (["White"], True)],            # Hispanic overrides -> Latinx
        "races ARRAY<STRING>, hisp BOOLEAN",
    )
    out = df.select(
        race_ethnicity_rollup(F.col("races"), F.col("hisp"), "Multiple", "Latinx", "Unknown").alias("r")
    ).collect()
    assert [r["r"] for r in out] == ["White", "Multiple", "Unknown", "Latinx"]
