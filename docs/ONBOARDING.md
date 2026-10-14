# Deployment guide

A step-by-step path from an empty Databricks workspace to a governed Ed-Fi dimensional
warehouse running against your own ODS. Follow the steps in order. Each one ends with a
check you can run, so you always know whether to continue or stop and fix something.

Read [Status, license, and support](../README.md#status-license-and-support-read-first) in
the README first. In short: this is a reference implementation provided as-is, it is not an
officially supported Databricks product, and reconciling the numbers against your own
reports is your acceptance step.

**Time to first working pipeline:** about an hour of setup, plus a 15 to 25 minute first
run. No Ed-Fi credentials are needed to get that far.

---

## What you will end up with

Deployed into a Unity Catalog catalog you choose:

| Schema | Contents |
| --- | --- |
| `edu_bronze` | One raw table per Ed-Fi resource (JSON payload plus load metadata), plus `_meta_change_versions` holding the incremental-load watermarks |
| `edu_silver` | Roughly 147 typed and deduplicated staging tables, the equivalent of EDU's `base_ef3__*` and `stg_ef3__*` models |
| `edu_gold` | 58 dimensional marts (`dim_*`, `fct_*`, `msr_*`, `brg_*`) plus 7 `qc_*` data-quality tables |

Plus one Lakeflow Job that orchestrates the whole run, and two Spark Declarative Pipelines
(Silver and Gold). Everything is defined as code in this bundle, so it deploys the same way
into dev, test, and production.

---

## Step 1. Confirm prerequisites

**In Databricks:**

- A workspace with **Unity Catalog** enabled and **serverless compute** enabled.
- An account (your user, or a service principal for automated deploys) that can:
  - create a catalog, or `CREATE SCHEMA` on a catalog someone else pre-creates for you
  - create Jobs and Pipelines
  - use serverless compute
- If your platform team controls catalog creation, ask them for one catalog now and for
  `USE CATALOG` plus `CREATE SCHEMA` on it. That is enough. This project creates its own
  three schemas inside whatever catalog you point it at.

**On your laptop:**

```bash
# Databricks CLI v0.230 or later
brew install databricks          # macOS; other platforms: https://docs.databricks.com/dev-tools/cli/install.html
databricks --version

# Terraform v1.5 or later (Asset Bundles use it internally)
brew install terraform
terraform -version

# Python 3.10+ (only needed if you want to run the unit tests locally)
python3 --version
```

**Check:** all three commands print a version.

---

## Step 2. Get the code

If you received this as an archive, unpack it and put it under your own version control
before you change anything:

```bash
unzip edu-databricks.zip
cd edu-databricks
git init && git add -A && git commit -m "Vendor drop: edu-databricks baseline"
```

That first commit matters. It is what lets a colleague later see exactly what your agency
changed versus what shipped.

If you were given repository access instead, clone it and create your own working branch.

---

## Step 3. Authenticate to your workspace

```bash
databricks auth login --host https://<your-workspace-host>
# Choose a profile name when prompted, for example: my-agency
```

**Check:**

```bash
databricks current-user me -p my-agency
```

This should print your own name and email. Every command below takes `PROFILE=my-agency`,
which is how the bundle knows which workspace to target. Nothing about your workspace is
stored in this repository.

---

## Step 4. Point it at your catalog

Open `databricks.yml` and set the `catalog` variable default to the catalog you were given:

```yaml
variables:
  catalog:
    default: my_agency_edfi     # was: edu_demo
```

Leave everything else alone for now. The schema names (`edu_bronze`, `edu_silver`,
`edu_gold`), `data_mode: synthetic`, and the business-rule tunables all have working
defaults, and Step 7 covers changing them.

**Check:**

```bash
make validate PROFILE=my-agency
```

This resolves the whole bundle against your workspace without deploying anything. It should
report the bundle name, your target, and no errors. If it fails here, it is a configuration
or permissions problem, and deploying will not help.

---

## Step 5. First run, with no credentials

Deploy and run in **synthetic** mode. This generates a small, internally consistent Ed-Fi
dataset inside your workspace and pushes it through the real pipeline. It proves the whole
stack works before you involve your ODS or your security team.

```bash
make deploy PROFILE=my-agency     # creates the schemas, job, and two pipelines
make run    PROFILE=my-agency     # runs it, blocking until the job finishes
```

Expect **15 to 25 minutes** on a cold start, most of it serverless environment setup on the
Bronze task. Later runs are 5 to 10 minutes. Use `make run-async` instead if you want a run
URL immediately rather than waiting.

**Check:** the job finishes green. The final task is an end-to-end test that fails the run
on any data-quality breach, so a green run is a meaningful signal, not just "no crash".

If it fails, open the job run in the workspace UI and click into the failed task. For
pipeline failures, the Spark Declarative Pipelines UI shows the exact Python traceback.
[README §18](../README.md#18-troubleshooting) lists the failures adopters hit most, and the
first one (a Terraform signing-key error) is worth reading before you start.

---

## Step 6. Look at what it built

```sql
-- 58 marts and 7 quality tables
SHOW TABLES IN my_agency_edfi.edu_gold;

-- Every quality check with its pass/fail status. This is the health dashboard.
SELECT * FROM my_agency_edfi.edu_gold.qc_scorecard ORDER BY status DESC;
```

Then open `notebooks/90_demo_queries.sql` in the workspace and run it against `edu_gold`.
It is seven worked queries (enrollment, attendance, chronic absenteeism, program
participation, and so on) that show the shape of the warehouse and confirm the marts return
sensible rows.

If any `qc_scorecard` row shows FAIL, drill into the matching detail table
(`qc_dim_pk_audit`, `qc_fct_fk_audit`, `qc_descriptor_audit`, `qc_business_invariants`) to
see the offending rows.

---

## Step 7. Connect to your real Ed-Fi ODS

### 7a. Answer these about your ODS first

These four answers determine whether the default field mapping fits you, and getting them
before you start saves a debugging cycle:

1. **Which Ed-Fi ODS/API version and which Data Standard version?** Also, do you use Ed-Fi
   **Profiles** to restrict fields? This drives field mapping, demographics most of all.
2. **Which domains do you actually populate?** Attendance, programs (special education,
   English learner, Title I), assessments, discipline, teacher preparation (TPDM).
3. **Do you have extensions or custom descriptors?**
4. **API access:** the base URL, and a client key and secret.

> **Demographics.** Race and sex mapping is validated against Data Standard 5.x, where those
> fields sit on `studentEducationOrganizationAssociation`. Some newer Data Standard versions
> moved them. If your ODS is newer, or a Profile restricts them, confirm where demographics
> live and adjust the recipe in `src/silver/recipes.py`. The pipeline degrades gracefully
> (race resolves to "Unknown") instead of failing, so this is easy to miss: check it
> explicitly rather than assuming.

### 7b. Store credentials in a secret scope

Never put real Ed-Fi credentials in `databricks.yml`. Bundle variables are visible in the
job UI and go into version control.

```bash
databricks secrets create-scope edu_edfi -p my-agency
databricks secrets put-secret edu_edfi edfi_client_key -p my-agency
databricks secrets put-secret edu_edfi edfi_client_secret -p my-agency
```

Grant READ on the scope to whoever or whatever runs the job (you, or the service principal
in production). The Bronze notebook pulls the keys at runtime with
`dbutils.secrets.get()`, so nothing sensitive appears in the job definition, the run
parameters, or the run history.

### 7c. Optional intermediate step: smoke-test the real-data path

Before pointing at production, confirm the live-API path works using the Ed-Fi Alliance
public sandbox:

```bash
make smoke PROFILE=my-agency
```

This pulls a 13-resource subset plus all descriptors into isolated `edu_*_smoke` schemas in
about 8 minutes, so it exercises real OAuth, real paging, and real descriptor resolution
without a full historical load or your own credentials.

Note that `make smoke` repoints the bundle at the `_smoke` schemas. Run `make deploy` again
afterwards to restore the normal schema names.

### 7d. Switch to your ODS

In `databricks.yml`:

```yaml
variables:
  data_mode:         { default: custom }
  edfi_base_url:     { default: https://your-ods.example.gov/api }
  edfi_secret_scope: { default: edu_edfi }
  api_year:          { default: "2026" }     # school-year tag stamped on every row
  tenant_code:       { default: "my_agency" } # district or agency tag stamped on every row
```

`api_year` and `tenant_code` are stamped onto every extracted row, which is what lets you
hold multiple years and multiple districts in one catalog without collisions. Set them
deliberately now: changing `tenant_code` later changes every surrogate key.

If you run one ODS instance per school year, deploy once per year with a different
`api_year`, `edfi_base_url`, and secret scope.

```bash
make deploy PROFILE=my-agency
make run    PROFILE=my-agency
```

The first real run does a full historical load and takes considerably longer than the
synthetic run. After that, runs are incremental via Ed-Fi change versions and typically
finish in under 5 minutes even for a state-sized ODS.

---

## Step 8. Reconcile. This is your acceptance step.

A green run proves the pipeline is **structurally** correct: surrogate keys are unique,
foreign keys resolve, business invariants hold. It does **not** prove the numbers match your
reality, and nobody but your agency can establish that.

Before anyone builds a report on this, compare a handful of Gold figures against the
reports you already trust:

- total enrollment by school and by district
- chronic absenteeism rate
- program participation counts (special education, English learner, Title I)
- student counts by race and ethnicity rollup

Investigate every difference until you can explain it. Some will be genuine bugs in the
field mapping for your ODS. Others will be definitional: your state's chronic absenteeism
rule may differ from the default of below 90 percent attendance over at least 20 enrolled
days, in which case set `chronic_absence_threshold` and `chronic_absence_min_days` to your
state's rule rather than editing code. The race and ethnicity labels
(`multiple_races_code`, `hispanic_latino_code`, `race_unknown_code`) are tunable the same
way.

Treat this step as a project milestone with a named owner, not a quick check.

---

## Step 9. Promote to production

The `prod` target already differs from `dev` in the ways that matter: pipelines run in
production mode, the job schedule is unpaused, and the bundle root moves out of your
personal workspace folder.

```bash
databricks bundle deploy -t prod -p my-agency
```

Before you rely on it:

- **Run it as a service principal, not as a person.** A pipeline that breaks when someone
  changes roles or leaves is not in production.
- **Turn on failure notifications.** `resources/ingestion_job.yml` has a commented
  `email_notifications` block. Uncomment it and set your team's address.
- **Grant read access on `edu_gold` to your analysts**, and keep `edu_bronze` and
  `edu_silver` restricted. Analysts should build on the curated marts, not on raw payloads.
- **Decide on a schedule** that lands after your districts' nightly SIS sync completes.
- **Work through** [README §15](../README.md#15-production-readiness-checklist), which is
  the fuller checklist.

---

## Step 10. Customize for your state

Each of these is a documented, small change. [README §14](../README.md#14-customizing-for-your-district--state)
has the full detail and worked examples.

| You want to | Do this |
| --- | --- |
| Pull an Ed-Fi resource that is not included | Add one line to `src/edfi_extract/resources.yml`. That creates the bronze table, the silver base view, and the silver stage table. |
| Surface another field as a typed column | Add an entry to `RECIPES` in `src/silver/recipes.py`. |
| Add a mart | Add one `@dlt.table` function in `src/gold/pipeline.py`, using the surrogate-key helpers in `src/gold/shared/keys.py`, then register it in `notebooks/99_run_e2e_tests.py`. |
| Change a business rule threshold or label | Set the bundle variable. No code change. |
| Add your own quality check | Append a tuple to `qc_business_invariants.checks` in `src/gold/qc.py`. It appears in `qc_scorecard` automatically. |
| Push extracts back out to districts | See the opt-in scaffold in [`extensions/per_org_export/`](../extensions/README.md). The split is built; the external delivery is a seam you complete. |

Run the unit tests after any code change. They need no Databricks connection:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[test]"
make unit
```

---

## Where to go next

- [`ENGINEERING_OVERVIEW.md`](ENGINEERING_OVERVIEW.md) is the fastest full code tour, for
  whoever will maintain this.
- [`MART_PARITY.md`](MART_PARITY.md) is the mart-by-mart inventory and the honest list of
  divergences from upstream EDU. Read it before promising anyone literal parity.
- [`DESCRIPTOR_RESOLUTION.md`](DESCRIPTOR_RESOLUTION.md) explains how Ed-Fi descriptor URIs
  become human-readable labels, which is the first thing people ask about.
- [README §19](../README.md#19-honest-gaps) lists what is genuinely not built yet.
- Questions about adopting this: your Databricks account team.
