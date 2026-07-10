"""Unit tests for the per_org_export extension.

The pure-Python tests (slug, output-root, delivery seam) run anywhere. The
partition/write test uses a local SparkSession and a temp directory — no Databricks.
"""
from __future__ import annotations

import pytest

from per_org_export.export import (
    ExportConfig,
    deliver,
    org_slug,
    output_root,
    write_partitions,
)


# --- pure-Python: no Spark required -----------------------------------------

def test_org_slug_is_filesystem_safe():
    assert org_slug("Region 7 / North Valley") == "Region_7_North_Valley"
    assert org_slug("lea-12.3_x") == "lea-12.3_x"
    assert org_slug(42) == "42"
    assert org_slug("") == "unknown"
    assert org_slug(None) == "unknown"


def test_output_root_defaults_to_volume_and_honors_override():
    cfg = ExportConfig(catalog="c", gold_schema="edu_gold", table="fct_x", org_column="lea_id")
    assert output_root(cfg) == "/Volumes/c/edu_gold/exports/fct_x"

    cfg2 = ExportConfig(catalog="c", gold_schema="edu_gold", table="fct_x",
                        org_column="lea_id", export_name="attendance", volume="dist")
    assert output_root(cfg2) == "/Volumes/c/edu_gold/dist/attendance"

    cfg3 = ExportConfig(catalog="c", gold_schema="edu_gold", table="fct_x",
                        org_column="lea_id", output_root="/tmp/out/")
    assert output_root(cfg3) == "/tmp/out"


def test_deliver_is_an_unimplemented_seam():
    # The external-delivery seam must stay explicitly unimplemented, not silently pass.
    with pytest.raises(NotImplementedError):
        deliver([])


# --- Spark: local session + temp dir ----------------------------------------

@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    s = (SparkSession.builder
         .master("local[2]")
         .appName("test_per_org_export")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def test_write_partitions_one_file_per_org_skipping_nulls(spark, tmp_path):
    df = spark.createDataFrame(
        [("SU1", "s1"), ("SU1", "s2"), ("SU2", "s3"), (None, "s4")],
        "org STRING, student_id STRING",
    )
    cfg = ExportConfig(
        catalog="c", gold_schema="edu_gold", table="fct_x",
        org_column="org", file_format="csv", output_root=str(tmp_path),
    )
    results = write_partitions(df, cfg)

    # NULL org is not routable -> skipped; two real orgs remain, sorted.
    assert [r.org for r in results] == ["SU1", "SU2"]
    assert {r.org: r.rows for r in results} == {"SU1": 2, "SU2": 1}
    assert sum(r.rows for r in results) == 3  # the 4th row (null org) dropped on purpose

    # Each org produced a real, separately-readable file with the right rows.
    for r in results:
        assert (tmp_path / r.org).is_dir()
        back = spark.read.option("header", "true").csv(r.output_path)
        assert back.count() == r.rows
        assert set(back.columns) == {"org", "student_id"}


def test_write_partitions_raises_on_missing_org_column(spark, tmp_path):
    df = spark.createDataFrame([("s1",)], "student_id STRING")
    cfg = ExportConfig(catalog="c", gold_schema="edu_gold", table="fct_x",
                       org_column="org", output_root=str(tmp_path))
    with pytest.raises(ValueError, match="org_column"):
        write_partitions(df, cfg)
