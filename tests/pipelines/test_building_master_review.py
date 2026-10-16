import pandas as pd
import pandas.testing as pdt

from property_scraper.pipelines.building_master.contracts import OVERRIDE_COLUMNS
from property_scraper.pipelines.building_master.review import (
    triage_building_review_queue,
)

EXPECTED_HIGH_PRIORITY_SCORE = 30.0
EXPECTED_REVIEW_COUNT = 2


def _record(record_id: str, **overrides: object) -> dict[str, object]:
    result: dict[str, object] = {
        "source_record_id": record_id,
        "observed_count": 2,
        "address_en": "1 Harbour Road",
        "normalized_address": "1HARBOURROAD",
        "completion_year": 2000,
        "property_type": "residential",
        "native_parent_id": "estate-1",
    }
    result.update(overrides)
    return result


def _queue(
    review_id: str, source_id: str, candidate_id: str, **overrides: object
) -> dict[str, object]:
    result: dict[str, object] = {
        "review_id": review_id,
        "source_record_id": source_id,
        "candidate_source_record_id": candidate_id,
        "match_score": 95.0,
        "runner_up_score": pd.NA,
        "reason": "manual review",
    }
    result.update(overrides)
    return result


def test_triage_classifies_conflicts_from_source_evidence() -> None:
    records = pd.DataFrame(
        [
            _record("source"),
            _record("candidate-year", completion_year=2001),
            _record("candidate-type", property_type="office"),
            _record(
                "candidate-address",
                address_en="2 Other Road",
                normalized_address="2OTHERROAD",
            ),
            _record("candidate-parent", native_parent_id="estate-2"),
        ]
    )
    queue = pd.DataFrame(
        [
            _queue("year", "source", "candidate-year"),
            _queue("type", "source", "candidate-type"),
            _queue("address", "source", "candidate-address"),
            _queue("parent", "source", "candidate-parent"),
            _queue(
                "collision",
                "source",
                "candidate-year",
                reason="ambiguous candidate collision",
            ),
            _queue(
                "margin",
                "source",
                "candidate-year",
                reason="fuzzy runner-up margin not met",
            ),
            _queue(
                "override",
                "source",
                "candidate-year",
                reason="conflicting approved overrides",
            ),
            _queue("other", "source", "missing"),
        ]
    )

    triaged, _, _ = triage_building_review_queue(queue, records)

    assert triaged.set_index("review_id")["review_category"].to_dict() == {
        "year": "completion_year_conflict",
        "type": "property_type_conflict",
        "address": "address_conflict",
        "parent": "parent_conflict",
        "collision": "ambiguous_name_collision",
        "margin": "fuzzy_margin",
        "override": "override_conflict",
        "other": "other",
    }


def test_priority_is_deterministic_and_uses_transaction_and_dual_evidence() -> None:
    records = pd.DataFrame(
        [
            _record("a", observed_count=3),
            _record("b", observed_count=7),
        ]
    )
    queue = pd.DataFrame([_queue("r1", "a", "b")])
    params = {"high_priority_cutoff": 25, "medium_priority_cutoff": 15}

    first = triage_building_review_queue(queue, records, params)
    second = triage_building_review_queue(queue, records, params)

    assert first[0].loc[0, "priority_score"] == EXPECTED_HIGH_PRIORITY_SCORE
    assert first[0].loc[0, "priority_tier"] == "high"
    pdt.assert_frame_equal(first[0], second[0])


def test_template_never_auto_approves_or_proposes_a_canonical_id() -> None:
    records = pd.DataFrame([_record("a"), _record("b")])
    queue = pd.DataFrame([_queue("r1", "a", "b")])

    _, template, _ = triage_building_review_queue(queue, records)

    # The template proposes a canonical id but never a decision. Reviewers cannot
    # invent a usable id themselves, and merging a pair requires both of its rows
    # to name the same one, so the id is pre-filled and review_status left blank.
    assert template.loc[0, "review_id"] == "r1"
    assert template["review_status"].isna().all()
    assert template["canonical_building_id"].notna().all()
    assert template["canonical_building_id"].nunique() == 1
    assert template["source_record_id"].tolist() == ["a", "b"]
    assert "approved" not in template.columns
    # Saving the filled template as overrides must need no column renaming.
    assert list(template.columns)[: len(OVERRIDE_COLUMNS)] == OVERRIDE_COLUMNS


def test_triage_preserves_each_row_and_is_stable_across_input_order() -> None:
    records = pd.DataFrame([_record("a"), _record("b"), _record("c")])
    queue = pd.DataFrame([_queue("r2", "a", "b"), _queue("r1", "a", "c")])

    triaged, template, summary = triage_building_review_queue(queue, records)
    repeated = triage_building_review_queue(queue.copy(), records.iloc[::-1], None)

    assert triaged["review_id"].tolist() == ["r2", "r1"]
    # Two rows per pair, so each queue row appears twice in the template.
    assert template["review_id"].tolist() == ["r2", "r2", "r1", "r1"]
    assert triaged["review_id"].is_unique
    assert (
        summary.loc[summary["review_category"].eq("total"), "review_count"].item()
        == EXPECTED_REVIEW_COUNT
    )
    pdt.assert_frame_equal(triaged, repeated[0])
    pdt.assert_frame_equal(template, repeated[1])
    pdt.assert_frame_equal(summary, repeated[2])


def test_empty_review_queue_returns_empty_contracts() -> None:
    queue = pd.DataFrame(
        columns=["review_id", "source_record_id", "candidate_source_record_id"]
    )
    records = pd.DataFrame(columns=["source_record_id"])

    triaged, template, summary = triage_building_review_queue(queue, records)

    assert triaged.empty
    assert template.empty
    assert summary.empty
    assert "review_status" in template.columns
