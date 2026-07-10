"""
Synthetic-data integrity tests.

These exist because hollow synthetic data once masked a table-collapse bug:
recipes were shaped to match the generator instead of the real API, and the
resources where keys were wrong happened to generate empty lists. The rules:

  1. Every manifest resource that exists on Ed-Fi 7.3 must generate ROWS.
  2. Every manifest key_path must RESOLVE on the generated payloads.
  3. Every silver recipe path must RESOLVE on the generated payloads.
  4. Delete tombstones must reference records that exist in the data.
"""
from __future__ import annotations

import re

import pytest

from edfi_extract.extractor import load_manifest
from edfi_extract.synthetic import generate_synthetic_dataset

# Endpoints removed before / absent on Data Standard 5.x — the generator omits
# them on purpose to exercise the optional-endpoint skip path.
NOT_IN_DS5 = {"parents", "student_parent_associations", "student_discipline_incident_associations"}


@pytest.fixture(scope="module")
def data():
    return generate_synthetic_dataset()


@pytest.fixture(scope="module")
def manifest():
    return load_manifest()


def _resolve(record: dict, path: str):
    """Resolve a dot-notation JSON path (with optional [N] indexing) on a dict."""
    cur = record
    for part in path.split("."):
        m = re.match(r"(\w+)\[(\d+)\]$", part)
        if m:
            cur = cur.get(m.group(1)) if isinstance(cur, dict) else None
            if not isinstance(cur, list) or len(cur) <= int(m.group(2)):
                return None
            cur = cur[int(m.group(2))]
        else:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(part)
        if cur is None:
            return None
    return cur


def test_every_ds5_resource_has_rows(data, manifest):
    hollow = [
        r.name for r in manifest
        if r.name not in NOT_IN_DS5 and len(data.get(r.name) or []) == 0
    ]
    assert not hollow, (
        f"Synthetic generator produced NO rows for: {hollow}. "
        "Hollow synthetic data hides broken code paths — generate real records."
    )


def test_non_ds5_resources_are_omitted_not_empty(data):
    for name in NOT_IN_DS5:
        assert name not in data, (
            f"'{name}' does not exist on a 7.3 ODS; omit it (exercising the 404 "
            "skip path) instead of returning an empty list."
        )


def test_every_record_has_guid(data, manifest):
    for r in manifest:
        for rec in (data.get(r.name) or []):
            assert rec.get("id"), f"{r.name} record missing 'id' GUID: {rec}"


def test_manifest_key_paths_resolve_on_synthetic_payloads(data, manifest):
    """The exact regression that once collapsed silver tables: a key path that
    never resolves hashes all-NULL keys and dedupe collapses the table."""
    broken = []
    for r in manifest:
        records = data.get(r.name) or []
        if not records:
            continue
        for path in r.key_paths:
            resolved = sum(1 for rec in records if _resolve(rec, path) is not None)
            # Some key parts are legitimately sparse (e.g. gradingPeriodName on
            # legacy data) but a path resolving on ZERO records is a bug.
            if resolved == 0:
                broken.append(f"{r.name}: {path}")
    assert not broken, f"Manifest key paths never resolve on synthetic data: {broken}"


def test_silver_recipe_paths_resolve_on_synthetic_payloads(data):
    from silver.recipes import RECIPES

    OPTIONAL_RECIPE_COLS = {
        # legitimately sparse fields — present on some records only
        ("students", "middle_name"),
        ("student_school_associations", "exit_withdraw_date"),
        ("student_school_attendance_events", "attendance_event_reason"),
        ("student_program_associations", "end_date"),
        ("student_special_education_program_associations", "end_date"),
        ("student_homeless_program_associations", "end_date"),
        ("student_language_instruction_program_associations", "end_date"),
        ("student_title_i_part_a_program_associations", "end_date"),
        ("student_cte_program_associations", "end_date"),
        ("student_migrant_education_program_associations", "end_date"),
        ("student_school_food_service_program_association", "end_date"),
        ("sessions", "term_descriptor"),
        ("student_education_organization_associations", "limited_english_proficiency_descriptor"),
        ("student_education_organization_associations", "hispanic_latino_ethnicity"),
    }
    broken = []
    for resource, recipe in RECIPES.items():
        records = data.get(resource) or []
        if not records:
            continue
        for col, json_path in recipe.items():
            path = json_path.lstrip("$.")
            resolved = sum(1 for rec in records if _resolve(rec, path) is not None)
            if resolved == 0 and (resource, col) not in OPTIONAL_RECIPE_COLS:
                broken.append(f"{resource}.{col} ({json_path})")
    assert not broken, (
        f"Recipe paths never resolve on synthetic data: {broken}. Either the "
        "recipe or the generator diverges from the real Ed-Fi shape — check "
        "the 7.3 OpenAPI spec, then fix BOTH to match it."
    )


def test_deletes_reference_existing_records(data):
    deletes = data.get("_deletes")
    assert deletes, "Generator must emit tombstones so delete propagation is exercised."
    for resource, tombstones in deletes.items():
        assert tombstones, f"_deletes['{resource}'] is empty"
        ids = {rec["id"] for rec in data[resource]}
        for t in tombstones:
            assert t["id"] in ids, f"tombstone {t['id']} not in {resource} records"


def test_descriptor_lookup_covers_every_code_used(data):
    """Every descriptor URI used anywhere in the data must have a matching
    descriptor record, so Silver resolution never silently falls back or leaves
    a raw code. Zero invented values: shortDescription is a code the ODS itself
    defines. This is the descriptor analogue of the recipe-resolution test."""
    desc_names = {k for k in data if k.endswith("_descriptors")}
    assert desc_names, "Generator emitted no descriptor resources."

    emitted = set()
    for name in desc_names:
        assert data[name], f"descriptor resource '{name}' is empty"
        for rec in data[name]:
            assert rec.get("id"), f"{name} record missing 'id': {rec}"
            assert rec.get("shortDescription"), f"{name} has a hollow shortDescription: {rec}"
            emitted.add(f"{rec['namespace']}#{rec['codeValue']}")

    referenced = set()

    def walk(o):
        if isinstance(o, str):
            if o.startswith("uri://") and "#" in o:   # descriptor field values ARE the URI
                referenced.add(o)
        elif isinstance(o, dict):
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    for name, recs in data.items():
        if name in desc_names or name == "_deletes":
            continue
        walk(recs)

    missing = sorted(referenced - emitted)
    assert not missing, (
        f"Descriptor URIs used in the data with NO descriptor record: {missing}. "
        "Route every descriptor through `_d` so the lookup covers it."
    )


def test_manifest_descriptors_match_generator(data, manifest):
    """The manifest's descriptor endpoints must equal exactly what the generator
    emits — so every descriptor lands in bronze and feeds the `_descriptors`
    lookup, and none is declared but never produced (which would fail extraction
    in synthetic mode via the not-in-synthetic EdFiNotFound path)."""
    manifest_desc = {r.name for r in manifest if r.name.endswith("_descriptors")}
    generated_desc = {k for k in data if k.endswith("_descriptors")}
    assert manifest_desc == generated_desc, (
        "Manifest vs generator descriptor mismatch. "
        f"In manifest only: {sorted(manifest_desc - generated_desc)}; "
        f"in generator only: {sorted(generated_desc - manifest_desc)}"
    )


def test_referential_consistency(data):
    """Spot-check FK chains so gold joins resolve."""
    student_ids = {s["studentUniqueId"] for s in data["students"]}
    school_ids = {s["schoolId"] for s in data["schools"]}
    for assoc in data["student_school_associations"]:
        assert assoc["studentReference"]["studentUniqueId"] in student_ids
        assert assoc["schoolReference"]["schoolId"] in school_ids
    contact_ids = {c["contactUniqueId"] for c in data["contacts"]}
    for assoc in data["student_contact_associations"]:
        assert assoc["contactReference"]["contactUniqueId"] in contact_ids
    section_keys = {
        (s["courseOfferingReference"]["localCourseCode"], s["courseOfferingReference"]["schoolId"],
         s["sectionIdentifier"]) for s in data["sections"]
    }
    for assoc in data["student_section_associations"]:
        ref = assoc["sectionReference"]
        assert (ref["localCourseCode"], ref["schoolId"], ref["sectionIdentifier"]) in section_keys
