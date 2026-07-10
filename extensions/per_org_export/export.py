"""
Per-organization export — a downstream-distribution extension.

The core pipeline (``src/``) ends at the gold warehouse. Some agencies must then push
*subsets* of that warehouse back out to the organizations that contributed the data.
A common example: an agency splits an attendance mart by the
intermediate unit that owns each district (an ESC, LEA group, or **Supervisory Union**)
and delivers one extract per unit over SFTP. This module is the reusable scaffold for that
pattern — generalized so any agency can split by any organization column.

It is deliberately split into two stages with different ownership:

  1. ``split_by_org(spark, cfg)`` — REAL + tested. Reads a gold mart, partitions it by
     an organization column, and writes one file per organization to a Unity Catalog
     Volume (or any path you point it at). Generic; nothing agency-specific.

  2. ``deliver(results, ...)`` — EXTENSION SEAM. Moving those files to an external
     destination (SFTP, S3, a vendor API) needs infrastructure only the adopter has:
     a host, credentials, and a per-organization directory map. It is intentionally a
     ``NotImplementedError`` with a documented contract — see ``extensions/README.md``.

Nothing here is wired into the default Asset Bundle. The opt-in job that runs it lives
at ``extensions/per_org_export/resources/per_org_export_job.yml``.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional

try:  # pyspark is always present on Databricks; guarded so the pure-Python parts import without it.
    from pyspark.sql import DataFrame, functions as F
except Exception:  # pragma: no cover
    DataFrame = None  # type: ignore
    F = None  # type: ignore


@dataclass
class ExportConfig:
    """What to export, split how, and where to land the per-org files."""

    catalog: str
    gold_schema: str
    table: str                 # gold mart to export, e.g. "fct_student_school_attendance_event"
    org_column: str            # column to split on, e.g. "supervisory_union_id" / "lea_id"
    volume: str = "exports"    # UC Volume under {catalog}.{gold_schema} to land files in
    export_name: Optional[str] = None       # subfolder name; defaults to `table`
    file_format: str = "csv"                # csv | parquet | json
    columns: List[str] = field(default_factory=list)   # empty = all columns
    where: Optional[str] = None             # optional SQL filter, e.g. "school_year = 2026"
    output_root: Optional[str] = None       # override the /Volumes/... destination (tests, external locations)


@dataclass
class OrgExportResult:
    org: str
    rows: int
    output_path: str


def output_root(cfg: ExportConfig) -> str:
    """Resolve the destination root: an explicit override, else a UC Volume path."""
    if cfg.output_root:
        return cfg.output_root.rstrip("/")
    name = cfg.export_name or cfg.table
    return f"/Volumes/{cfg.catalog}/{cfg.gold_schema}/{cfg.volume}/{name}"


def org_slug(value) -> str:
    """Filesystem-safe folder name for an organization id (keeps it readable)."""
    if value is None:
        return "unknown"
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value)).strip("_")
    return s or "unknown"


def read_mart(spark, cfg: ExportConfig) -> "DataFrame":
    """Read the gold mart, applying the optional column projection and row filter."""
    df = spark.read.table(f"{cfg.catalog}.{cfg.gold_schema}.{cfg.table}")
    if cfg.where:
        df = df.where(cfg.where)
    if cfg.columns:
        df = df.select(*cfg.columns)
    return df


def split_by_org(spark, cfg: ExportConfig) -> List[OrgExportResult]:
    """Read the gold mart and write one file per organization. See ``write_partitions``."""
    return write_partitions(read_mart(spark, cfg), cfg)


def write_partitions(df: "DataFrame", cfg: ExportConfig) -> List[OrgExportResult]:
    """
    Write one file per distinct value of ``cfg.org_column`` under ``output_root(cfg)``.

    Returns a result per organization (id, row count, output path). Rows whose
    ``org_column`` is NULL are skipped (they cannot be routed to a destination) — if any
    exist that is a data-quality signal worth surfacing upstream, not silently dropping
    in production. ``coalesce(1)`` lands a single part-file per org, the shape most
    downstream/SFTP receivers expect.

    Split out from ``split_by_org`` so it can be exercised on any DataFrame without a
    live catalog table (see the unit tests).
    """
    if cfg.org_column not in df.columns:
        raise ValueError(
            f"org_column {cfg.org_column!r} is not a column of {cfg.table}. "
            f"Available: {df.columns}"
        )

    root = output_root(cfg)
    orgs = [
        row[0]
        for row in df.select(cfg.org_column).distinct().collect()
        if row[0] is not None and str(row[0]) != ""
    ]

    results: List[OrgExportResult] = []
    for org in sorted(orgs, key=str):
        part = df.where(F.col(cfg.org_column) == F.lit(org))
        out = f"{root}/{org_slug(org)}"
        writer = part.coalesce(1).write.mode("overwrite").format(cfg.file_format)
        if cfg.file_format == "csv":
            writer = writer.option("header", "true")
        writer.save(out)
        results.append(OrgExportResult(org=str(org), rows=part.count(), output_path=out))
    return results


def deliver(
    results: List[OrgExportResult],
    *,
    sftp_host: str = "",
    secret_scope: str = "",
    remote_dir_template: str = "",
) -> None:
    """
    EXTENSION SEAM — not implemented on purpose.

    Push each per-org file produced by ``split_by_org`` to its external destination.
    The destination, auth, and per-org directory layout are agency-specific, so this is
    left for the adopter. A typical SFTP implementation, kept Databricks-native:

      1. Store credentials in a Databricks **secret scope** (never inline). Read with
         ``dbutils.secrets.get(secret_scope, "sftp_user" | "sftp_password" | "sftp_key")``.
      2. Add ``paramiko`` to the job environment (see the job-yaml template).
      3. For each ``OrgExportResult``, map ``result.org`` -> a remote directory via
         ``remote_dir_template.format(org=result.org)`` and SFTP-put the file(s) under
         ``result.output_path``.
      4. Return / log a per-org delivery status so the job can **fail closed** on a
         partial delivery rather than report success.

    Related agency-specific seams that often live alongside delivery:
      - Legacy-format conversion (e.g. re-encoding grade levels or SSCED codes to the
        encoding a downstream system expects) applied before the file is written.
      - A manifest / checksum file per org for the receiving system to validate.
    """
    raise NotImplementedError(
        "Per-organization delivery is an adopter-specific extension seam. "
        "See extensions/README.md for the contract and a worked SFTP example."
    )
