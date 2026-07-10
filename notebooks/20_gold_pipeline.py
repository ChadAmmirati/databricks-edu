# Databricks notebook source
# MAGIC %md
# MAGIC # Gold SDP entrypoint
# MAGIC
# MAGIC The pipeline's source notebook. Importing `gold.pipeline` registers every
# MAGIC dim/fact table with the SDP runtime.

# COMMAND ----------

import sys, os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__) if "__file__" in globals() else os.getcwd(), ".."))
src_path = os.path.join(project_root, "src")
if os.path.isdir(src_path) and src_path not in sys.path:
    sys.path.insert(0, src_path)

# COMMAND ----------

import gold.pipeline  # noqa: F401
import gold.qc        # noqa: F401  registers qc_* SDP tables
