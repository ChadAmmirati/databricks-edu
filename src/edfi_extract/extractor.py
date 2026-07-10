"""
Bronze extractor: lands raw Ed-Fi JSON into Delta tables.

One table per resource:    {catalog}.{schema}.{resource}_raw
One shared deletes table:  {catalog}.{schema}._deletes_raw
One state table:           {catalog}.{schema}._meta_change_versions

Schema of every *_raw table:
    tenant_code     STRING
    api_year        INT
    pull_timestamp  TIMESTAMP
    pull_change_version  BIGINT
    record_guid     STRING       -- copied from JSON 'id' when present
    payload         STRING       -- the raw JSON record as a string (use from_json downstream)

Incremental contract (mirrors edu_edfi_airflow):
  - ONE change-version snapshot is taken per run and used as maxChangeVersion
    for every resource, so the raw layer never drifts across resources.
  - The state table is keyed by (tenant_code, api_year, resource, kind) so
    multiple years/tenants coexist and deletes carry their own watermark.
  - snapshot < stored watermark  -> hard error: the ODS was restored from a
    backup; a full_refresh run is required.
  - snapshot == stored watermark -> resource skipped (ODS unchanged).
  - A 404 on an `optional` endpoint marks it SKIPPED (version-gated endpoints
    like 'parents' on Data Standard 5.x). Any other per-resource failure is
    recorded, the remaining resources still run, and the job fails at the end
    with a summary — a failed resource's watermark is NOT advanced, so it is
    automatically re-pulled next run.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from edfi_extract.client import EdFiClient, EdFiNotFound

logger = logging.getLogger(__name__)


@dataclass
class ResourceSpec:
    name: str
    path: str
    namespace: str = "ed-fi"
    key_paths: List[str] = field(default_factory=list)
    deletes: bool = True
    optional: bool = False

    @classmethod
    def from_dict(cls, d: dict) -> "ResourceSpec":
        return cls(
            name=d["name"],
            path=d["path"],
            namespace=d.get("namespace", "ed-fi"),
            key_paths=d.get("key_paths") or [],
            deletes=bool(d.get("deletes", True)),
            optional=bool(d.get("optional", False)),
        )


def load_manifest(path: Optional[str] = None) -> List[ResourceSpec]:
    if path is None:
        path = str(Path(__file__).with_name("resources.yml"))
    with open(path, "r") as f:
        doc = yaml.safe_load(f)
    return [ResourceSpec.from_dict(r) for r in doc["resources"]]


# ---------- runtime config ----------
@dataclass
class ExtractConfig:
    catalog: str
    schema: str
    tenant_code: str
    api_year: int
    data_mode: str        # 'sandbox' | 'synthetic' | 'custom'
    edfi_base_url: str
    edfi_client_key: str = ""
    edfi_client_secret: str = ""
    full_refresh: bool = False
    resources_filter: List[str] = field(default_factory=list)  # empty = all resources

    @classmethod
    def from_env(cls, *, full_refresh: bool = False) -> "ExtractConfig":
        return cls(
            catalog=os.environ["EDU_CATALOG"],
            schema=os.environ["EDU_BRONZE_SCHEMA"],
            tenant_code=os.environ.get("EDU_TENANT_CODE", "demo"),
            api_year=int(os.environ.get("EDU_API_YEAR", "2026")),
            data_mode=os.environ.get("EDU_DATA_MODE", "synthetic"),
            edfi_base_url=os.environ.get("EDU_EDFI_BASE_URL", ""),
            edfi_client_key=os.environ.get("EDU_EDFI_CLIENT_KEY", ""),
            edfi_client_secret=os.environ.get("EDU_EDFI_CLIENT_SECRET", ""),
            full_refresh=full_refresh,
            resources_filter=[r for r in os.environ.get("EDU_RESOURCES_FILTER", "").split("|") if r],
        )


@dataclass
class ResourceResult:
    status: str           # 'ok' | 'skipped_unchanged' | 'skipped_missing' | 'failed'
    rows: int = 0
    deletes: int = 0
    error: str = ""


# ---------- main entry point ----------
def extract_all(spark, config: ExtractConfig, manifest_path: Optional[str] = None) -> Dict[str, ResourceResult]:
    """Extracts every resource in the manifest. Returns {resource_name: ResourceResult}.

    Raises RuntimeError at the END if any non-optional resource failed, so one
    bad endpoint never blocks the rest of the pull.
    """
    resources = load_manifest(manifest_path)
    _ensure_schema(spark, config.catalog, config.schema)
    _ensure_state_table(spark, config.catalog, config.schema)
    _ensure_deletes_table(spark, config.catalog, config.schema)

    if config.data_mode == "synthetic":
        from edfi_extract.synthetic import generate_synthetic_dataset
        synthetic = generate_synthetic_dataset(school_year=config.api_year)
        client = None
        # Synthetic CVs are small monotonic counters per (tenant, year) — never
        # wall-clock, so they can never poison a later real-API watermark.
        run_cv = _max_state_cv(spark, config) + 1
    else:
        synthetic = None
        client = EdFiClient(
            base_url=config.edfi_base_url,
            client_key=config.edfi_client_key,
            client_secret=config.edfi_client_secret,
        )
        # ONE snapshot for the whole run. If this call fails, fail the run —
        # falling back to a fabricated version silently corrupts the watermark.
        run_cv = client.get_latest_change_version()

    results: Dict[str, ResourceResult] = {}
    _filter = set(config.resources_filter or [])
    for resource in resources:
        # Silver declares a table per manifest resource, so the raw table must
        # exist even when the endpoint is version-gated/missing on this ODS.
        table_fqn = f"`{config.catalog}`.`{config.schema}`.`{resource.name}_raw`"
        _ensure_raw_table(spark, table_fqn)
        # Optional smoke-test subset: pull only named resources (+ all descriptors,
        # so labels still resolve). Raw tables for the rest are still ensured above,
        # so Silver/Gold build (empty) without error.
        if _filter and resource.name not in _filter and not resource.name.endswith("_descriptors"):
            results[resource.name] = ResourceResult(status="skipped_filtered")
            continue
        if config.full_refresh:
            # Clear the slice up front — including for endpoints that turn out
            # to be missing on this ODS, so stale rows never survive a rebuild.
            _delete_tenant_year(spark, table_fqn, config)
            _delete_tenant_year_deletes(spark, config, resource.name)
        try:
            results[resource.name] = extract_resource(
                spark, config, resource, run_cv,
                client=client, synthetic=synthetic,
            )
        except EdFiNotFound:
            if resource.optional:
                logger.warning("Endpoint %s not on this ODS (version-gated); skipped.", resource.path)
                results[resource.name] = ResourceResult(status="skipped_missing")
            else:
                logger.exception("Required endpoint %s returned 404.", resource.path)
                results[resource.name] = ResourceResult(status="failed", error=f"404 on required endpoint {resource.path}")
        except Exception as exc:
            logger.exception("Failed to extract %s; continuing with remaining resources.", resource.name)
            results[resource.name] = ResourceResult(status="failed", error=str(exc)[:500])

    failed = {n: r.error for n, r in results.items() if r.status == "failed"}
    landed = sum(r.rows for r in results.values())
    logger.info(
        "Extraction complete: %d rows landed, %d ok, %d unchanged, %d missing, %d failed.",
        landed,
        sum(1 for r in results.values() if r.status == "ok"),
        sum(1 for r in results.values() if r.status == "skipped_unchanged"),
        sum(1 for r in results.values() if r.status == "skipped_missing"),
        len(failed),
    )
    if failed:
        raise RuntimeError(f"Extraction failed for {len(failed)} resource(s): {failed}")
    return results


def extract_resource(
    spark,
    config: ExtractConfig,
    resource: ResourceSpec,
    run_cv: int,
    *,
    client: Optional[EdFiClient] = None,
    synthetic: Optional[Dict[str, List[dict]]] = None,
) -> ResourceResult:
    table_fqn = f"`{config.catalog}`.`{config.schema}`.`{resource.name}_raw`"
    pull_ts = datetime.now(timezone.utc)

    last_cv = 0 if config.full_refresh else _get_state_cv(spark, config, resource.name, "records")
    if run_cv < last_cv:
        raise RuntimeError(
            f"{resource.name}: ODS change version went BACKWARD ({run_cv} < watermark {last_cv}). "
            "The ODS was likely restored from a backup. Re-run the job with full_refresh=true."
        )
    if run_cv == last_cv:
        return ResourceResult(status="skipped_unchanged")

    # ----- pull records -----
    if synthetic is not None:
        if resource.name not in synthetic:
            # Generator marks version-gated endpoints by omitting the key,
            # exercising the same skip path a real 404 takes.
            raise EdFiNotFound(resource.path)
        raws = synthetic[resource.name]
        rows = [_to_landing_row(r, config, pull_ts, run_cv) for r in raws]
    else:
        assert client is not None
        rows = []
        for page in client.get_resource(
            resource.path,
            min_change_version=(last_cv + 1 if last_cv else None),
            max_change_version=run_cv,
        ):
            for raw in page:
                rows.append(_to_landing_row(raw, config, pull_ts, run_cv))

    _ensure_raw_table(spark, table_fqn)
    if rows:
        df = spark.createDataFrame(rows, schema=_RAW_SCHEMA)
        df.write.mode("append").saveAsTable(table_fqn)

    # ----- pull deletes (their own watermark) -----
    # A real-API full refresh skips /deletes: the rebuilt raw layer simply
    # doesn't contain deleted records (mirrors edu_edfi_airflow). Synthetic
    # tombstones are part of the dataset's semantics, so they land on EVERY
    # synthetic run — delete propagation must always be exercised.
    n_deletes = 0
    if resource.deletes and (synthetic is not None or not config.full_refresh):
        n_deletes = _extract_deletes(spark, config, resource, run_cv, pull_ts, client=client, synthetic=synthetic)

    _set_state_cv(spark, config, resource.name, "records", run_cv, pull_ts, len(rows))
    if resource.deletes:
        # On full refresh the raw table was rebuilt from scratch, so historical
        # deletes are moot — seed the deletes watermark at run_cv too.
        _set_state_cv(spark, config, resource.name, "deletes", run_cv, pull_ts, n_deletes)

    logger.info("Landed %d rows (+%d deletes) into %s", len(rows), n_deletes, table_fqn)
    return ResourceResult(status="ok", rows=len(rows), deletes=n_deletes)


def _extract_deletes(spark, config, resource, run_cv, pull_ts, *, client, synthetic) -> int:
    last_cv = _get_state_cv(spark, config, resource.name, "deletes")
    if run_cv <= last_cv:
        return 0

    if synthetic is not None:
        tombstones = (synthetic.get("_deletes") or {}).get(resource.name, [])
    else:
        tombstones = []
        try:
            for page in client.get_deletes(
                resource.path,
                min_change_version=(last_cv + 1 if last_cv else None),
                max_change_version=run_cv,
            ):
                tombstones.extend(page)
        except EdFiNotFound:
            logger.info("%s has no /deletes endpoint; skipping deletes for it.", resource.path)
            return 0

    if not tombstones:
        return 0
    rows = [
        (
            config.tenant_code,
            config.api_year,
            resource.name,
            t.get("id"),
            int(t.get("changeVersion") or 0),
            pull_ts,
            json.dumps(t, separators=(",", ":"), default=str),
        )
        for t in tombstones
    ]
    df = spark.createDataFrame(rows, schema=_DELETES_SCHEMA)
    df.write.mode("append").saveAsTable(_deletes_fqn(config))
    return len(rows)


# ---------- helpers ----------
_RAW_SCHEMA = "tenant_code STRING, api_year INT, pull_timestamp TIMESTAMP, pull_change_version BIGINT, record_guid STRING, payload STRING"
_DELETES_SCHEMA = "tenant_code STRING, api_year INT, resource STRING, record_guid STRING, change_version BIGINT, pull_timestamp TIMESTAMP, payload STRING"


def _to_landing_row(raw: dict, config: ExtractConfig, pull_ts: datetime, pull_cv: int):
    return (
        config.tenant_code,
        config.api_year,
        pull_ts,
        pull_cv,
        raw.get("id"),
        json.dumps(raw, separators=(",", ":"), default=str),
    )


def _deletes_fqn(config: ExtractConfig) -> str:
    return f"`{config.catalog}`.`{config.schema}`.`_deletes_raw`"


def _ensure_schema(spark, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def _ensure_raw_table(spark, table_fqn: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_fqn} (
            tenant_code STRING,
            api_year INT,
            pull_timestamp TIMESTAMP,
            pull_change_version BIGINT,
            record_guid STRING,
            payload STRING
        )
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """
    )


def _ensure_deletes_table(spark, catalog: str, schema: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`_deletes_raw` (
            tenant_code STRING,
            api_year INT,
            resource STRING,
            record_guid STRING,
            change_version BIGINT,
            pull_timestamp TIMESTAMP,
            payload STRING
        ) USING DELTA
        """
    )


def _ensure_state_table(spark, catalog: str, schema: str) -> None:
    fqn = f"`{catalog}`.`{schema}`.`_meta_change_versions`"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            tenant_code STRING,
            api_year INT,
            resource STRING,
            kind STRING,
            last_change_version BIGINT,
            last_pull_timestamp TIMESTAMP,
            last_row_count BIGINT
        ) USING DELTA
        """
    )
    cols = {f.name for f in spark.table(fqn.replace("`", "")).schema.fields}
    if "tenant_code" not in cols or "kind" not in cols:
        # v1 state table (keyed by resource only). The watermark semantics
        # changed, so a clean rebuild is the only safe migration: dropping it
        # just forces a full re-pull, which Silver's dedupe absorbs.
        logger.warning("Migrating _meta_change_versions to tenant/year-keyed schema (forces full re-pull).")
        spark.sql(f"DROP TABLE {fqn}")
        _ensure_state_table(spark, catalog, schema)


def _state_fqn(config: ExtractConfig) -> str:
    return f"`{config.catalog}`.`{config.schema}`.`_meta_change_versions`"


def _get_state_cv(spark, config: ExtractConfig, resource: str, kind: str) -> int:
    row = spark.sql(
        f"SELECT last_change_version FROM {_state_fqn(config)} "
        f"WHERE tenant_code = '{config.tenant_code}' AND api_year = {config.api_year} "
        f"AND resource = '{resource}' AND kind = '{kind}'"
    ).collect()
    return int(row[0][0]) if row else 0


def _max_state_cv(spark, config: ExtractConfig) -> int:
    row = spark.sql(
        f"SELECT MAX(last_change_version) FROM {_state_fqn(config)} "
        f"WHERE tenant_code = '{config.tenant_code}' AND api_year = {config.api_year}"
    ).collect()
    return int(row[0][0]) if row and row[0][0] is not None else 0


def _set_state_cv(spark, config: ExtractConfig, resource: str, kind: str, cv: int, ts: datetime, n: int) -> None:
    spark.sql(
        f"""
        MERGE INTO {_state_fqn(config)} t
        USING (SELECT '{config.tenant_code}' AS tenant_code, {config.api_year} AS api_year,
                      '{resource}' AS resource, '{kind}' AS kind,
                      {cv} AS cv, TIMESTAMP'{ts.isoformat()}' AS ts, {n} AS n) s
        ON t.tenant_code = s.tenant_code AND t.api_year = s.api_year
           AND t.resource = s.resource AND t.kind = s.kind
        WHEN MATCHED THEN UPDATE SET last_change_version = s.cv, last_pull_timestamp = s.ts, last_row_count = s.n
        WHEN NOT MATCHED THEN INSERT (tenant_code, api_year, resource, kind, last_change_version, last_pull_timestamp, last_row_count)
             VALUES (s.tenant_code, s.api_year, s.resource, s.kind, s.cv, s.ts, s.n)
        """
    )


def _delete_tenant_year(spark, table_fqn: str, config: ExtractConfig) -> None:
    spark.sql(
        f"DELETE FROM {table_fqn} WHERE tenant_code = '{config.tenant_code}' AND api_year = {config.api_year}"
    )


def _delete_tenant_year_deletes(spark, config: ExtractConfig, resource: str) -> None:
    spark.sql(
        f"DELETE FROM {_deletes_fqn(config)} WHERE tenant_code = '{config.tenant_code}' "
        f"AND api_year = {config.api_year} AND resource = '{resource}'"
    )
