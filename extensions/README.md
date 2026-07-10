# Extensions

The core pipeline in `src/` does one job and stops at a governed dimensional warehouse:

```
Ed-Fi ODS → bronze → silver → gold (dim/fct/msr/brg + qc)
```

**Extensions are opt-in modules that sit *on top of* the gold warehouse** to do
agency-specific work the core deliberately leaves out. They live here, outside `src/`,
so the core stays small and every adopter can see exactly what is "the warehouse" vs.
"things a particular agency bolted on."

Rules for anything in this folder:

- **Reads gold, never reaches into bronze/silver.** An extension consumes the public
  `dim_*` / `fct_*` contract — the same surface an analyst or dashboard would.
- **Not deployed by default.** The root `databricks.yml` only includes `resources/*.yml`.
  An extension's job YAML lives under `extensions/<name>/resources/` and is inert until
  you add it to the bundle `include:` list.
- **Honest seams.** Where an extension genuinely needs infrastructure only you have
  (an SFTP host, credentials, a vendor endpoint), that function raises
  `NotImplementedError` with a documented contract rather than pretending to work.
- **Tested where it's generic.** The parts that are the same for every agency ship with
  unit tests; the adopter-specific seams are clearly marked.

---

## `per_org_export` — downstream distribution

**The need.** After the warehouse is built, some agencies push *subsets* of it back out
to the organizations that contributed the data. A common case: an agency splits an
attendance mart by the intermediate unit that owns each district (an ESC, LEA group, or
**Supervisory Union**) and delivers one extract per unit over **SFTP** — the per-unit
split plus paramiko SFTP push a hand-written pipeline often grows into.

**What ships here.** A generalized version of that pattern — split a gold mart by *any*
organization column, write one file per organization. Two stages, on purpose:

| Stage | Status | What it does |
| --- | --- | --- |
| `split_by_org(spark, cfg)` | **Implemented + tested** | Reads a gold mart, partitions by `org_column`, writes one file per org to a UC Volume (`/Volumes/{catalog}/{gold_schema}/{volume}/{export_name}/{org}/`). NULL org rows are skipped (they can't be routed). |
| `deliver(results, ...)` | **Extension seam** (`NotImplementedError`) | Push each per-org file to its external destination. Auth + host + per-org directory map are yours, so you implement it. The docstring spells out the typical SFTP shape. |

**Files**

```
extensions/per_org_export/
├── export.py                       # ExportConfig + split_by_org (real) + deliver (seam)
├── run_export.py                   # notebook task entry point (widgets -> split_by_org)
└── resources/
    └── per_org_export_job.yml      # opt-in Lakeflow job (not auto-included)
```

**Wire it up**

1. Decide your routing column. `school_id` works out of the box; to split by an
   intermediate unit (SU / ESC / LEA group), model that column onto the mart first —
   e.g. join the org hierarchy in a small `@dlt.table` in `src/gold/pipeline.py`, or add
   it to `dim_school`, then point `org_column` at it.
2. Add the job to the bundle and deploy:
   ```yaml
   # databricks.yml
   include:
     - resources/*.yml
     - extensions/per_org_export/resources/per_org_export_job.yml
   ```
   ```bash
   databricks bundle deploy
   databricks bundle run edu_per_org_export_job
   ```
   This produces per-org files in the Volume. `deliver` stays `false`.
3. Implement `export.deliver()` for your destination (see its docstring), store
   credentials in a Databricks **secret scope**, then flip the job's `deliver` parameter
   to `true`. Make it **fail closed** on a partial delivery.

**Try it locally**

`tests/unit/test_per_org_export.py` runs the split against a local SparkSession and a
temp directory — no Databricks needed — and asserts the delivery seam is still a seam.

---

## Adding your own extension

1. `extensions/<your_name>/` with an `__init__.py` and your module(s).
2. Read only the gold contract.
3. Put the job YAML under `extensions/<your_name>/resources/` and document the
   `include:` opt-in.
4. Unit-test the generic logic; mark adopter-specific seams with `NotImplementedError`.
5. Add a short section to this README.
