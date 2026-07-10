# Databricks notebook source
# MAGIC %md
# MAGIC # Silver SDP entrypoint
# MAGIC
# MAGIC This is the notebook the Spark Declarative Pipeline points at. The pipeline
# MAGIC will import `silver.pipeline`, which defines one `*_base` and one `*` (stage)
# MAGIC table per resource in resources.yml.

# COMMAND ----------

import sys, os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__) if "__file__" in globals() else os.getcwd(), ".."))
src_path = os.path.join(project_root, "src")
if os.path.isdir(src_path) and src_path not in sys.path:
    sys.path.insert(0, src_path)

# COMMAND ----------

import silver.pipeline  # noqa: F401  triggers @dlt.table registration
