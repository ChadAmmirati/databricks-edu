# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: Ed-Fi → Delta
# MAGIC
# MAGIC Pulls every resource defined in `resources.yml` from the configured Ed-Fi source
# MAGIC (sandbox, custom API, or synthetic generator) and lands raw JSON into
# MAGIC `{catalog}.{bronze_schema}.{resource}_raw`, with /deletes tombstones in
# MAGIC `_deletes_raw` and per-(tenant, year, resource, kind) watermarks in
# MAGIC `_meta_change_versions`.
# MAGIC
# MAGIC This notebook is the replacement for `edu_edfi_airflow`'s `EdFiResourceDAG`.
# MAGIC
# MAGIC **Credentials:** set `edfi_secret_scope` to a Databricks secret scope holding
# MAGIC keys `edfi_client_key` / `edfi_client_secret`. The plaintext widgets are a
# MAGIC fallback for throwaway sandbox keys only — they are visible in the job UI.

# COMMAND ----------

dbutils.widgets.text("catalog", "edu_demo")
dbutils.widgets.text("bronze_schema", "edu_bronze")
dbutils.widgets.text("tenant_code", "demo")
dbutils.widgets.text("api_year", "2026")
dbutils.widgets.dropdown("data_mode", "synthetic", ["synthetic", "sandbox", "custom"])
dbutils.widgets.text("edfi_base_url", "https://api.ed-fi.org/v7.3/api")
dbutils.widgets.text("edfi_secret_scope", "")
dbutils.widgets.text("edfi_client_key", "")
dbutils.widgets.text("edfi_client_secret", "")
dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"])
dbutils.widgets.text("resources_filter", "")  # pipe-separated resource names; empty = all (smoke-test subset)

# COMMAND ----------

import os, sys

# Make src/ importable whether run from the bundle root or from a workspace path.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__) if "__file__" in globals() else os.getcwd(), ".."))
src_path = os.path.join(project_root, "src")
if os.path.isdir(src_path) and src_path not in sys.path:
    sys.path.insert(0, src_path)

# COMMAND ----------

from edfi_extract.extractor import ExtractConfig, extract_all

secret_scope = dbutils.widgets.get("edfi_secret_scope").strip()
if secret_scope:
    client_key = dbutils.secrets.get(secret_scope, "edfi_client_key")
    client_secret = dbutils.secrets.get(secret_scope, "edfi_client_secret")
else:
    client_key = dbutils.widgets.get("edfi_client_key")
    client_secret = dbutils.widgets.get("edfi_client_secret")

config = ExtractConfig(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("bronze_schema"),
    tenant_code=dbutils.widgets.get("tenant_code"),
    api_year=int(dbutils.widgets.get("api_year")),
    data_mode=dbutils.widgets.get("data_mode"),
    edfi_base_url=dbutils.widgets.get("edfi_base_url"),
    edfi_client_key=client_key,
    edfi_client_secret=client_secret,
    full_refresh=(dbutils.widgets.get("full_refresh").lower() == "true"),
    resources_filter=[r for r in dbutils.widgets.get("resources_filter").split("|") if r],
)

results = extract_all(spark, config)
display(sorted(
    [{"resource": r, "status": res.status, "rows_landed": res.rows, "deletes_landed": res.deletes, "error": res.error}
     for r, res in results.items()],
    key=lambda d: d["resource"],
))

# COMMAND ----------

dbutils.notebook.exit(str(sum(r.rows for r in results.values())))
