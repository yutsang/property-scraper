import pandas as pd

from property_scraper.pipelines.building_master import matching
from property_scraper.pipelines.building_master.contracts import (
    BRIDGE_COLUMNS,
    CANDIDATE_PAIR_COLUMNS,
    CANONICAL_COLUMNS,
    REVIEW_COLUMNS,
    SOURCE_RECORD_COLUMNS,
)
from property_scraper.pipelines.building_master.matching import (
    build_building_master_outputs,
    generate_match_candidates,
    resolve_canonical_buildings,
)


def _record(
    source_system: str,
    native_id: str,
    name: str,
    **overrides: object,
) -> dict[str, object]:
    options: dict[str, object] = {
        "address": "1 Harbour Road",
        "domain": "residential",
        "grain": "building",
        "district": "Wan Chai",
        "completion_year": 2000,
        "property_type": "residential",
    }
    options.update(overrides)
    address = str(options["address"])
    normalized_name = name.upper().replace(" ", "")
    normalized_address = address.upper().replace(" ", "")
    result: dict[str, object] = {column: pd.NA for column in SOURCE_RECORD_COLUMNS}
    result.update(
        {
            "source_record_id": f"{source_system}::{native_id}",
            "source_system": source_system,
            "source_domain": options["domain"],
            "source_dataset": "synthetic",
            "entity_grain": options["grain"],
            "native_id": native_id,
            "native_parent_id": options.get("native_parent_id", pd.NA),
            "name_en": name,
            "name_zh": options.get("name_zh", pd.NA),
            "building_name_en": name,
            "normalized_name": normalized_name,
            "address_en": address,
            "normalized_address": normalized_address,
            "district": options["district"],
            "completion_year": options["completion_year"],
            "property_type": options["property_type"],
            "attribute_provenance": "test",
        }
    )
    return result


def _resolve(records: pd.DataFrame, **params: object) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return resolve_canonical_buildings(
        records, generate_match_candidates(records, params), pd.DataFrame(), params
    )


def test_exact_match_resolves_and_preserves_ordered_contracts() -> None:
    records = pd.DataFrame(
        [_record("centaline", "1", "Harbour View"), _record("midland", "2", "Harbour View")]
    )
    candidates = generate_match_candidates(records)
    canonical, bridge, review = resolve_canonical_buildings(records, candidates, pd.DataFrame())

    assert list(candidates.columns) == CANDIDATE_PAIR_COLUMNS
    assert list(canonical.columns) == CANONICAL_COLUMNS
    assert list(bridge.columns) == BRIDGE_COLUMNS
    assert list(review.columns) == REVIEW_COLUMNS
    assert len(canonical) == 1
    assert canonical.loc[0, "centaline_native_ids"] == "1"
    assert canonical.loc[0, "midland_native_ids"] == "2"
    assert bridge["source_record_id"].is_unique
    assert bridge["canonical_building_id"].nunique() == 1


def test_same_name_collision_is_ambiguous_and_queued() -> None:
    records = pd.DataFrame(
        [
            _record("centaline", "1", "Harbour View"),
            _record("midland", "2", "Harbour View"),
            _record("midland", "3", "Harbour View"),
        ]
    )
    canonical, bridge, review = _resolve(records)

    assert len(canonical) == len(records)
    assert bridge["match_tier"].eq("singleton").all()
    assert not review.empty
    assert review["reason"].str.contains("ambiguous").all()


def test_chinese_exact_name_with_shared_address_is_a_candidate() -> None:
    records = pd.DataFrame(
        [
            _record("centaline", "1", "Centaline Tower", name_zh="海景大廈"),
            _record("midland", "2", "Midland House", name_zh="海景大廈"),
        ]
    )

    candidates = generate_match_candidates(records)

    assert len(candidates) == 1
    assert candidates.loc[0, "match_tier"] == "exact_name_address"


def test_chinese_key_keeps_the_characters_that_distinguish_buildings() -> None:
    """House numbers and block letters are the only distinguishing content in
    Hong Kong commercial names, which are street addresses."""
    assert matching._chinese_key("駱克道382號") != matching._chinese_key("駱克道88號")
    assert matching._chinese_key("力寶中心1座") != matching._chinese_key("力寶中心2座")
    assert matching._chinese_key("嘉力工業中心B座") != matching._chinese_key("嘉力工業中心A座")
    assert matching._chinese_key("太古廣場1期") != matching._chinese_key("太古廣場2期")


def test_chinese_key_ignores_punctuation_and_spacing() -> None:
    assert matching._chinese_key("嘉力工業中心 A 座") == matching._chinese_key("嘉力工業中心(A座)")
    assert matching._chinese_key(" 宏力工業大廈 ") == matching._chinese_key("宏力工業大廈")


def test_chinese_names_differing_only_by_street_number_are_not_candidates() -> None:
    records = pd.DataFrame(
        [
            _record(
                "centaline",
                "1",
                "17 Cheung Fat Street",
                name_zh="長發街17號",
                address="17 Cheung Fat Street",
            ),
            _record(
                "midland",
                "2",
                "20 Cheung Fat Street",
                name_zh="長發街20號",
                address="20 Cheung Fat Street",
            ),
        ]
    )

    assert generate_match_candidates(records).empty


def test_domain_and_grain_boundaries_are_not_candidate_pairs() -> None:
    records = pd.DataFrame(
        [
            _record("centaline", "1", "Harbour View", address="1 Harbour Road"),
            _record(
                "midland", "2", "Harbour View",
                domain="commercial", address="2 Harbour Road", district="Central",
            ),
            _record(
                "midland", "3", "Harbour View",
                grain="estate_phase", address="3 Harbour Road", district="Kowloon City",
            ),
        ]
    )
    candidates = generate_match_candidates(records)
    canonical, bridge, review = resolve_canonical_buildings(records, candidates, pd.DataFrame())

    assert candidates.empty
    assert len(canonical) == len(records)
    assert review.empty
    assert bridge["canonical_building_id"].nunique() == len(records)


def test_fuzzy_candidate_generation_is_geographically_blocked(
    monkeypatch: object,
) -> None:
    records = pd.DataFrame(
        [
            _record(
                source_system,
                f"{source_system}-{index}",
                f"{source_system} building {index}",
                district=f"District {index}",
            )
            for index in range(200)
            for source_system in ("centaline", "midland")
        ]
    )
    calls = 0
    original_score = matching._score

    def count_score(left: str, right: str) -> float:
        nonlocal calls
        calls += 1
        return original_score(left, right)

    monkeypatch.setattr(matching, "_rapidfuzz_extract", None)
    monkeypatch.setattr(matching, "_rapidfuzz_ratio", None)
    monkeypatch.setattr(matching, "_score", count_score)

    candidates = matching.generate_match_candidates(records, {"fuzzy_threshold": 101})

    assert candidates.empty
    assert calls == len(records)


def test_generic_name_exact_blocks_do_not_cross_geographies() -> None:
    block_count = 1_000
    records = pd.DataFrame(
        [
            _record(
                source_system,
                f"{source_system}-{index}",
                "Tower 1",
                address=f"{index} Example Road",
                district=f"District {index}",
            )
            for index in range(block_count)
            for source_system in ("centaline", "midland")
        ]
    )

    candidates = generate_match_candidates(records)

    assert len(candidates) == block_count
    assert candidates["is_ambiguous"].eq(False).all()
    assert candidates["left_source_record_id"].str.split("::").str[1].str.split("-").str[-1].equals(
        candidates["right_source_record_id"].str.split("::").str[1].str.split("-").str[-1]
    )


def test_resolver_indexes_many_pairs_and_singletons_without_lock_scans(
    monkeypatch: object,
) -> None:
    pair_count = 300
    records = pd.DataFrame(
        [
            *[
                _record(
                    source_system,
                    f"pair-{index}",
                    f"Building {index}",
                    address=f"{index} Pair Road",
                    district=f"Pair District {index}",
                )
                for index in range(pair_count)
                for source_system in ("centaline", "midland")
            ],
            *[
                _record(
                    "centaline",
                    f"singleton-{index}",
                    f"Singleton {index}",
                    address=f"{index} Singleton Road",
                    district=f"Singleton District {index}",
                )
                for index in range(pair_count)
            ],
        ]
    )
    candidates = generate_match_candidates(records)

    def fail_on_lock_lookup(_: object, __: str) -> set[str]:
        raise AssertionError("empty override resolution must not inspect component locks")

    monkeypatch.setattr(matching._DisjointSet, "locks_for", fail_on_lock_lookup)
    canonical, bridge, _ = resolve_canonical_buildings(
        records, candidates, pd.DataFrame()
    )

    assert len(candidates) == pair_count
    assert len(bridge) == len(records)
    assert bridge["source_record_id"].is_unique
    assert len(canonical) == pair_count * 2


def test_fuzzy_match_requires_clear_runner_up_margin() -> None:
    records = pd.DataFrame(
        [
            _record("centaline", "1", "Harbour Vew"),
            _record("midland", "2", "Harbour View"),
            _record("midland", "3", "Harbour Veiw"),
        ]
    )
    canonical, bridge, review = _resolve(
        records, fuzzy_threshold=80, fuzzy_runner_up_margin=20
    )

    assert len(canonical) == len(records)
    assert bridge["match_tier"].eq("singleton").all()
    assert review["reason"].str.contains("ambiguous|margin", regex=True).any()


def test_hierarchy_resolves_complex_phase_and_building() -> None:
    records = pd.DataFrame(
        [
            _record("centaline", "complex", "Grand Estate", grain="estate_phase"),
            _record(
                "centaline",
                "phase-1",
                "Grand Estate Phase 1",
                grain="estate_phase",
                native_parent_id="complex",
            ),
            _record(
                "centaline",
                "tower-1",
                "Tower 1",
                native_parent_id="phase-1",
            ),
        ]
    )
    canonical, _, _ = _resolve(records)
    by_name = canonical.set_index("canonical_name_en")
    complex_id = by_name.loc["Grand Estate", "canonical_building_id"]
    phase = by_name.loc["Grand Estate Phase 1"]
    building = by_name.loc["Tower 1"]

    assert phase["canonical_complex_id"] == complex_id
    assert phase["canonical_phase_id"] == phase["canonical_building_id"]
    assert building["canonical_complex_id"] == complex_id
    assert building["canonical_phase_id"] == phase["canonical_building_id"]


def test_building_hierarchy_stays_empty_for_conflicting_parent_aliases() -> None:
    records = pd.DataFrame(
        [
            _record("centaline", "phase-c", "Centaline Phase", grain="estate_phase"),
            _record("midland", "phase-m", "Midland Phase", grain="estate_phase"),
            _record(
                "centaline",
                "tower-c",
                "Shared Tower",
                native_parent_id="phase-c",
            ),
            _record(
                "midland",
                "tower-m",
                "Shared Tower",
                native_parent_id="phase-m",
            ),
        ]
    )
    canonical, _, _ = _resolve(records)
    tower = canonical.loc[canonical["canonical_name_en"].eq("Shared Tower")].iloc[0]

    assert pd.isna(tower["canonical_complex_id"])
    assert pd.isna(tower["canonical_phase_id"])


def test_generic_towers_only_match_inside_safe_linked_parents() -> None:
    records = pd.DataFrame(
        [
            _record("centaline", "phase-a", "Estate A", grain="estate_phase", address="1 A Road"),
            _record("centaline", "phase-b", "Estate B", grain="estate_phase", address="1 B Road"),
            _record("midland", "phase-a", "Estate A", grain="estate_phase", address="1 A Road"),
            _record("midland", "phase-b", "Estate B", grain="estate_phase", address="1 B Road"),
            _record("centaline", "tower-a", "Tower 1", native_parent_id="phase-a", address="Unknown"),
            _record("centaline", "tower-b", "Tower 1", native_parent_id="phase-b", address="Unknown"),
            _record("midland", "tower-a", "Tower 1", native_parent_id="phase-a", address="Elsewhere"),
            _record("midland", "tower-b", "Tower 1", native_parent_id="phase-b", address="Elsewhere"),
        ]
    )

    candidates = generate_match_candidates(records)
    towers = candidates.loc[candidates["match_tier"].eq("exact_name_parent")]

    assert {
        tuple(row)
        for row in towers[["left_source_record_id", "right_source_record_id"]].itertuples(
            index=False, name=None
        )
    } == {
        ("centaline::tower-a", "midland::tower-a"),
        ("centaline::tower-b", "midland::tower-b"),
    }


def test_ambiguous_parent_link_does_not_support_generic_child_merge() -> None:
    records = pd.DataFrame(
        [
            _record("centaline", "phase-c", "Shared Estate", grain="estate_phase"),
            _record("midland", "phase-m1", "Shared Estate", grain="estate_phase"),
            _record("midland", "phase-m2", "Shared Estate", grain="estate_phase"),
            _record("centaline", "tower-c", "Tower 1", native_parent_id="phase-c"),
            _record("midland", "tower-m", "Tower 1", native_parent_id="phase-m1"),
        ]
    )

    canonical, bridge, review = _resolve(records)

    towers = bridge.loc[bridge["source_record_id"].isin(["centaline::tower-c", "midland::tower-m"])]
    assert towers["match_tier"].eq("singleton").all()
    assert towers["canonical_building_id"].nunique() == len(towers)
    assert review["reason"].str.contains("ambiguous").any()
    assert len(canonical) == len(records)


def test_unique_bilingual_parent_links_children_without_geography() -> None:
    records = pd.DataFrame(
        [
            _record(
                "centaline",
                "phase-c",
                "Harbour Estate",
                grain="estate_phase",
                name_zh="海景花園",
                address="1 Centaline Road",
                district=pd.NA,
            ),
            _record(
                "midland",
                "phase-m",
                "Harbour Estate",
                grain="estate_phase",
                name_zh="海景花園",
                address="2 Midland Road",
                district=pd.NA,
            ),
            _record(
                "centaline",
                "tower-c",
                "Tower 1",
                native_parent_id="phase-c",
                address="1 Centaline Road",
                district=pd.NA,
            ),
            _record(
                "midland",
                "tower-m",
                "Tower 1",
                native_parent_id="phase-m",
                address="2 Midland Road",
                district=pd.NA,
            ),
        ]
    )

    candidates = generate_match_candidates(records)

    assert set(candidates["match_tier"]) == {"exact_bilingual", "exact_name_parent"}
    assert (
        candidates.loc[
            candidates["match_tier"].eq("exact_name_parent"), "left_source_record_id"
        ].item()
        == "centaline::tower-c"
    )


def test_bilingual_parent_collision_does_not_support_children() -> None:
    records = pd.DataFrame(
        [
            _record(
                "centaline",
                "phase-c",
                "Harbour Estate",
                grain="estate_phase",
                name_zh="海景花園",
                address="1 Centaline Road",
                district=pd.NA,
            ),
            _record(
                "midland",
                "phase-m1",
                "Harbour Estate",
                grain="estate_phase",
                name_zh="海景花園",
                address="2 Midland Road",
                district=pd.NA,
            ),
            _record(
                "midland",
                "phase-m2",
                "Harbour Estate",
                grain="estate_phase",
                name_zh="海景花園",
                address="3 Midland Road",
                district=pd.NA,
            ),
            _record("centaline", "tower-c", "Tower 1", native_parent_id="phase-c"),
            _record("midland", "tower-m", "Tower 1", native_parent_id="phase-m1"),
        ]
    )

    candidates = generate_match_candidates(records)

    assert candidates["match_tier"].eq("exact_bilingual").all()
    assert not candidates["left_source_record_id"].str.contains("tower").any()
    assert candidates["is_ambiguous"].all()


def test_single_language_name_without_geography_is_not_a_candidate() -> None:
    records = pd.DataFrame(
        [
            _record(
                "centaline",
                "phase-c",
                "Harbour Estate",
                grain="estate_phase",
                name_zh=pd.NA,
                address="1 Centaline Road",
                district=pd.NA,
            ),
            _record(
                "midland",
                "phase-m",
                "Harbour Estate",
                grain="estate_phase",
                name_zh=pd.NA,
                address="2 Midland Road",
                district=pd.NA,
            ),
        ]
    )

    assert generate_match_candidates(records).empty


def test_parent_supported_fuzzy_matching_respects_runner_up_margin() -> None:
    records = pd.DataFrame(
        [
            _record("centaline", "phase-c", "Garden Estate", grain="estate_phase"),
            _record("midland", "phase-m", "Garden Estate", grain="estate_phase"),
            _record(
                "centaline",
                "tower-c",
                "Garden Tower",
                native_parent_id="phase-c",
                district="North",
            ),
            _record(
                "midland",
                "tower-m1",
                "Garden Towe",
                native_parent_id="phase-m",
                district="South",
            ),
            _record(
                "midland",
                "tower-m2",
                "Garden Twer",
                native_parent_id="phase-m",
                district="South",
            ),
        ]
    )
    params = {
        "fuzzy_threshold": 100,
        "parent_fuzzy_threshold": 80,
        "fuzzy_runner_up_margin": 20,
    }

    candidates = generate_match_candidates(records, params)
    _, bridge, review = resolve_canonical_buildings(records, candidates, pd.DataFrame(), params)

    assert set(
        candidates.loc[
            candidates["match_tier"].eq("fuzzy_name_parent"), "right_source_record_id"
        ]
    ) == {"midland::tower-m1", "midland::tower-m2"}
    assert bridge.loc[bridge["entity_grain"].eq("building"), "match_tier"].eq("singleton").all()
    assert review["reason"].str.contains("ambiguous|margin", regex=True).any()


def test_parent_aware_matching_is_idempotent_across_input_order() -> None:
    records = pd.DataFrame(
        [
            _record("centaline", "phase-c", "Harbour Estate", grain="estate_phase"),
            _record("midland", "phase-m", "Harbour Estate", grain="estate_phase"),
            _record("centaline", "tower-c", "Tower 1", native_parent_id="phase-c"),
            _record("midland", "tower-m", "Tower 1", native_parent_id="phase-m"),
        ]
    )

    first_candidates = generate_match_candidates(records)
    second_candidates = generate_match_candidates(records.iloc[::-1].reset_index(drop=True))
    first = resolve_canonical_buildings(records, first_candidates, pd.DataFrame())
    second = resolve_canonical_buildings(
        records.iloc[::-1].reset_index(drop=True), second_candidates, pd.DataFrame()
    )

    assert first_candidates.equals(second_candidates)
    assert all(left.equals(right) for left, right in zip(first, second))


def test_approved_override_precedes_automatic_match_and_locks_id() -> None:
    records = pd.DataFrame(
        [_record("centaline", "1", "Harbour View"), _record("midland", "2", "Harbour View")]
    )
    overrides = pd.DataFrame(
        [
            {
                "source_record_id": "centaline::1",
                "canonical_building_id": "locked-building-7",
                "review_status": "approved",
            }
        ]
    )
    canonical, bridge, _ = resolve_canonical_buildings(
        records, generate_match_candidates(records), overrides
    )

    assert canonical.loc[0, "canonical_building_id"] == "locked-building-7"
    assert set(bridge["canonical_building_id"]) == {"locked-building-7"}
    assert bridge.loc[bridge["source_record_id"].eq("centaline::1"), "match_tier"].item() == "override"


def test_singletons_and_ids_are_stable_across_input_order() -> None:
    records = pd.DataFrame(
        [
            _record("centaline", "1", "Harbour View"),
            _record("midland", "2", "Different Building", address="2 Other Road"),
        ]
    )
    first = _resolve(records)
    second = _resolve(records.iloc[::-1].reset_index(drop=True))

    assert len(first[0]) == len(records)
    assert first[1]["match_tier"].eq("singleton").all()
    assert first[0].equals(second[0])
    assert first[1].equals(second[1])


def test_output_builder_returns_workbook_sheets_and_audit() -> None:
    records = pd.DataFrame(
        [_record("centaline", "1", "Harbour View"), _record("midland", "2", "Harbour View")]
    )
    candidates = generate_match_candidates(records)
    canonical, bridge, review = resolve_canonical_buildings(records, candidates, pd.DataFrame())
    sheets, audit = build_building_master_outputs(canonical, bridge, review, candidates)

    assert set(sheets) == {
        "Canonical Buildings",
        "Source Aliases",
        "Review Queue",
        "Candidate Pairs",
    }
    assert audit.set_index("metric").loc["source_records", "value"] == len(records)


def test_fuzzy_matching_requires_agreeing_house_numbers() -> None:
    """Edit distance rates a one-digit street number change as 94% similar."""
    records = pd.DataFrame(
        [
            _record("centaline", "1", "92 Sung Wong Toi Road", address="92 Sung Wong Toi Road"),
            _record("midland", "2", "90 Sung Wong Toi Road", address="90 Sung Wong Toi Road"),
        ]
    )

    assert generate_match_candidates(records).empty


def test_composite_district_labels_still_block_together() -> None:
    """Centaline stamps 61% of estates with a composite locality label."""
    records = pd.DataFrame(
        [
            _record("centaline", "1", "Belvedere Garden", district="Tsuen Wan | Belvedere Garden"),
            _record("midland", "2", "Belvedere Garden", district="Tsuen Wan"),
        ]
    )

    candidates = generate_match_candidates(records)

    assert len(candidates) == 1
    assert candidates.loc[0, "match_tier"].startswith("exact_")


def test_canonical_record_merges_fields_from_both_sources() -> None:
    """Matching exists so the two sources complete each other: Centaline carries
    the address and no Chinese name, Midland the reverse."""
    records = pd.DataFrame(
        [
            _record("centaline", "1", "Harbour View", name_zh=pd.NA, address="1 Harbour Road"),
            _record("midland", "2", "Harbour View", name_zh="海景大廈", address=pd.NA),
        ]
    )
    canonical, _, _ = _resolve(records)

    assert len(canonical) == 1
    row = canonical.iloc[0]
    assert row["canonical_name_zh"] == "海景大廈"
    assert row["address_en"] == "1 Harbour Road"


def test_one_building_recorded_at_two_grains_becomes_one_row() -> None:
    """Centaline lists 1005 Canton Road as an estate_phase and as a building."""
    records = pd.DataFrame(
        [
            _record("centaline", "1", "1005 Canton Road", address="1005 Canton Road"),
            _record(
                "centaline", "2", "1005 Canton Road",
                address="1005 Canton Road", grain="estate_phase",
            ),
        ]
    )
    canonical, bridge, _ = _resolve(records)

    assert len(canonical) == 1
    assert bridge["canonical_building_id"].nunique() == 1


def test_estate_and_its_differently_named_block_stay_separate() -> None:
    """A multi-block estate names its buildings "Tower 1", not after itself."""
    records = pd.DataFrame(
        [
            _record("centaline", "1", "Tower 1", address="1 Harbour Road"),
            _record(
                "centaline", "2", "Harbour Gardens",
                address="1 Harbour Road", grain="estate_phase",
            ),
        ]
    )
    canonical, _, _ = _resolve(records)

    assert len(canonical) == 2


def test_generic_names_in_one_district_are_not_merged_without_an_address() -> None:
    """"Tower 1" repeated across a district is a label, not an identity."""
    records = pd.DataFrame(
        [
            _record("centaline", str(index), "Tower 1", address=pd.NA, district="Sha Tin")
            for index in range(5)
        ]
        + [
            _record(
                "centaline", "e1", "Tower 1",
                address=pd.NA, district="Sha Tin", grain="estate_phase",
            )
        ]
    )
    canonical, _, _ = _resolve(records)

    assert len(canonical) == len(records)


def _override(record_id: str, canonical_id: str, status: str) -> dict[str, object]:
    return {
        "source_record_id": record_id,
        "canonical_building_id": canonical_id,
        "review_status": status,
        "reviewed_by": "tester",
        "reviewed_at": "2026-09-11",
        "notes": pd.NA,
    }


def test_rejected_override_keeps_a_pair_apart() -> None:
    """Two buildings sharing a name in one district: the reviewer's "no" has to
    survive the next run, or the queue asks again forever."""
    records = pd.DataFrame(
        [
            _record("centaline", "1", "Po Wing Building", address="1 Bowrington Road"),
            _record("midland", "2", "Po Wing Building", address="1 Bowrington Road"),
        ]
    )
    overrides = pd.DataFrame(
        [
            _override("centaline::1", "building-review-x", "rejected"),
            _override("midland::2", "building-review-x", "rejected"),
        ]
    )

    canonical, bridge, review = resolve_canonical_buildings(
        records, generate_match_candidates(records), overrides
    )

    assert len(canonical) == 2
    assert bridge["match_tier"].eq("singleton").all()
    assert review["reason"].str.contains("rejected").any()


def test_approved_override_still_merges_the_pair() -> None:
    records = pd.DataFrame(
        [
            _record("centaline", "1", "Harbour View", address="1 Harbour Road"),
            _record("midland", "2", "Harbour View", address="1 Harbour Road"),
        ]
    )
    overrides = pd.DataFrame(
        [
            _override("centaline::1", "building-review-y", "approved"),
            _override("midland::2", "building-review-y", "approved"),
        ]
    )

    canonical, bridge, _ = resolve_canonical_buildings(
        records, generate_match_candidates(records), overrides
    )

    assert len(canonical) == 1
    assert bridge["canonical_building_id"].nunique() == 1
    assert bridge["canonical_building_id"].iloc[0] == "building-review-y"


def test_block_label_resolves_an_apparent_collision() -> None:
    """"Block 41" meets every block of the estate. Only one carries the same
    number, so the crowd is not real ambiguity."""
    records = pd.DataFrame(
        [_record("centaline", "c41", "Block 41")]
        + [
            _record("midland", f"m{number}", f"Block {number}")
            for number in (39, 40, 41, 42)
        ]
    )
    canonical, bridge, _ = _resolve(records)

    merged = canonical[canonical["source_count"].eq(2)]
    assert len(merged) == 1
    assert merged.iloc[0]["centaline_native_ids"] == "c41"
    assert merged.iloc[0]["midland_native_ids"] == "m41"


def test_two_candidates_sharing_the_label_stay_ambiguous() -> None:
    """Genuine ambiguity survives: two candidates both labelled 41."""
    records = pd.DataFrame(
        [
            _record("centaline", "c41", "Block 41"),
            _record("midland", "m41a", "Block 41"),
            _record("midland", "m41b", "Block 41"),
        ]
    )
    canonical, bridge, review = _resolve(records)

    assert bridge["match_tier"].eq("singleton").all()
    assert len(canonical) == len(records)
    assert not review.empty
