# Databricks notebook source
# MAGIC %md
# MAGIC # Extension: Per-organization export
# MAGIC
# MAGIC Splits a gold mart by an organization column and writes one file per organization
# MAGIC to a Unity Catalog Volume. This is the **opt-in** downstream-distribution
# MAGIC extension — it is *not* part of the default ingestion job. See
# MAGIC `extensions/README.md` for the pattern and the worked per-unit/SFTP example.
# MAGIC
# MAGIC The split is implemented and tested. External **delivery** (SFTP/S3/vendor API)
# MAGIC is an adopter-specific seam (`export.deliver`) you complete with your own host and
# MAGIC credentials — leave `deliver` set to `false` until you have.

# COMMAND ----------

dbutils.widgets.text("catalog", "edu_demo")
dbutils.widgets.text("gold_schema", "edu_gold")
dbutils.widgets.text("table", "fct_student_school_attendance_event")
dbutils.widgets.text("org_column", "school_id")  # e.g. lea_id once modeled
dbutils.widgets.text("volume", "exports")
dbutils.widgets.text("export_name", "")          # subfolder; blank = table name
dbutils.widgets.dropdown("file_format", "csv", ["csv", "parquet", "json"])
dbutils.widgets.text("where", "")                # optional SQL filter, e.g. school_year = 2026
dbutils.widgets.dropdown("deliver", "false", ["true", "false"])

# COMMAND ----------

import os, sys

# Make extensions/ (and src/) importable whether run from the bundle root or a workspace path.
nb_dir = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()
repo_root = os.path.abspath(os.path.join(nb_dir, "..", ".."))
for rel in ("extensions", "src"):
    p = os.path.join(repo_root, rel)
    if os.path.isdir(p) and p not in sys.path:
        sys.path.insert(0, p)

# COMMAND ----------

from per_org_export.export import ExportConfig, split_by_org, deliver

cfg = ExportConfig(
    catalog=dbutils.widgets.get("catalog"),
    gold_schema=dbutils.widgets.get("gold_schema"),
    table=dbutils.widgets.get("table"),
    org_column=dbutils.widgets.get("org_column"),
    volume=dbutils.widgets.get("volume"),
    export_name=(dbutils.widgets.get("export_name").strip() or None),
    file_format=dbutils.widgets.get("file_format"),
    where=(dbutils.widgets.get("where").strip() or None),
)

results = split_by_org(spark, cfg)
display([{"org": r.org, "rows": r.rows, "output_path": r.output_path} for r in results])

# COMMAND ----------

# Delivery is an adopter-specific seam — implement export.deliver() before enabling.
if dbutils.widgets.get("deliver").lower() == "true":
    deliver(results)

# COMMAND ----------

dbutils.notebook.exit(str(sum(r.rows for r in results)))
