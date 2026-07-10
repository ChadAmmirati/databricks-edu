# Descriptor label resolution

**Status:** Implemented and **live-verified** (2026-06-25, sandbox `edu_*_sbx` schemas, synthetic mode) — full ingestion job green, gold labels resolved, zero `uri://` leaks, key parity intact. See Verification. Remaining stronger test: a run against a real Ed-Fi ODS (where codeValue ≠ shortDescription).

Resolves Ed-Fi descriptor references (`uri://ed-fi.org/TermDescriptor#Year Round`) to their
human `shortDescription` from **real descriptor data**, instead of showing the bare code value.
No hand-authored crosswalk / invented labels — labels come from the source's `*Descriptors`
endpoints (a real ODS) or from the synthetic ODS's own self-defined codes.

## Why it was needed
`extract_descriptor()` returned the Ed-Fi `codeValue` (the part after `#`). Synthetic mode masked
this because there the codeValue *is* the label; on a real ODS the codeValue is often an opaque code
whose label lives in `shortDescription`, which we never pulled. (Exactly the "works in synthetic,
breaks on real" gap class — synthetic data masks a real-data failure.)

## Design (additive — existing structure unchanged)
1. **Manifest** (`resources.yml`): added the Ed-Fi `*Descriptors` endpoints (48; `deletes: false`,
   `optional: true`). These land in bronze like any other resource.
2. **Silver `_descriptors` lookup** (`silver/pipeline.py`): unions every `*_descriptors_raw` →
   `descriptor_uri (= namespace # codeValue) -> short_description`. Descriptor resources are
   **skipped** in the base/stage loop (they only feed this lookup).
3. **Resolution in the Silver base layer**: each descriptor recipe column is left-joined to
   `_descriptors` and replaced with its label, falling back to the bare codeValue when a code has no
   descriptor row (`resolve_descriptors` in `silver/transforms.py`). Never NULL, never a raw URI.
4. **`extract_descriptor` is now idempotent** (`silver/transforms.py`): it only strips a value that
   looks like a URI, so an already-resolved label passes through unchanged. **Gold is therefore
   untouched** — its 57 `extract_descriptor(...)` display sites now emit labels automatically.

### Key-parity deny-list (critical)
Three descriptor columns feed surrogate keys and **must stay as their codeValue** so our keys match
EDU's `dbt_utils` keys 1:1 — they are NOT resolved in Silver (`_KEY_DESCRIPTOR_COLS`):
`program_type_descriptor` (→ `k_program`, `k_educator_prep_program`), `graduation_plan_type_descriptor`
(→ `k_graduation_plan`), `grading_period_descriptor` (→ `k_grading_period`). Their codeValues are
already human-readable in Ed-Fi (e.g. "Special Education", "Recommended"), so display quality is fine.
Verified: no other descriptor feeds a key (gold has zero inline `surrogate_key(`; all keys come from
`gold/shared/keys.py`, which takes only IDs/names/codes).

## No fake / no empty / real values
- **Synthetic** (`synthetic.py`): every `_d(kind, value)` call records its use; after generation,
  `_emit_descriptors` emits one descriptor record per `(namespace, codeValue)` actually used, with
  `shortDescription = codeValue`. A synthetic ODS legitimately defines its own codes — honest, not
  invented — and the lookup is guaranteed to cover every code in the data.
- **Real ODS**: the same `*Descriptors` endpoints supply real `shortDescription`s. A code missing
  from the ODS's descriptor tables degrades to its codeValue (a real value), never an empty cell.

## QC (`gold/qc.py`)
- `qc_descriptor_audit` now flags values that are neither a clean non-empty label **nor** a
  well-formed `uri://x#value` (handles resolved-label columns and the deny-list URI columns).
- `qc_business_invariants` adds `descriptor_lookup_present` and `descriptor_lookup_non_hollow` —
  fail if the lookup is empty or has null/blank `short_description` (catches a hollow lookup).

## Verification
**Done locally (no Databricks/Spark needed):**
- `py_compile` clean on all changed files.
- Manifest integrity: no duplicate resource names; the 48 manifest descriptor endpoints exactly
  equal the 48 the generator emits.
- Descriptor coverage: 88 descriptor URIs used in the data, 88 in the lookup, **0 unmatched**.
- Replicated the pure-Python integrity tests against the edits — every-resource-has-rows, key-paths
  resolve, recipe-paths resolve, deletes reference existing — all pass.
- New tests added: `test_transforms.py` (idempotent `extract_descriptor`, `resolve_descriptors`
  label+fallback, no fan-out); `test_synthetic.py` (descriptor coverage, manifest↔generator sync).

**Pending (requires the sandbox):**
- The Spark unit tests in `test_transforms.py` (need `pyspark` + a JRE — unavailable in this env).
- The end-to-end SDP run on Databricks: bronze pull of the descriptor endpoints → `_descriptors`
  builds → resolution on real Spark → QC green. This is the real acceptance gate.

## Known follow-up
On a real ODS, an unmatched code falls back to its codeValue (visible, not a crash) but isn't
flagged. A future `qc_descriptor_coverage` check (distinct descriptor URIs used vs present in
`_descriptors`) would surface real-data gaps — worth adding once we run against a live Ed-Fi ODS.
