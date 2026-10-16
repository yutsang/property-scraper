import pandas as pd
import pytest

import property_scraper.pipelines.building_master.nodes as adapter_nodes
from property_scraper.pipelines.building_master.contracts import SOURCE_RECORD_COLUMNS
from property_scraper.pipelines.building_master.nodes import (
    adapt_centaline_commercial,
    adapt_centaline_residential,
    adapt_midland_commercial,
    adapt_midland_residential,
    combine_source_records,
)

FUTURE_COMPLETION_YEAR = 2200
TWO_OBSERVATIONS = 2
THREE_OBSERVATIONS = 3
MODAL_GEO_CONFIDENCE = 0.667
HK_LATITUDE = 22.44
HK_LONGITUDE = 114.03


def test_centaline_residential_preserves_estate_phase_and_aggregates_buildings() -> None:
    estates = pd.DataFrame(
        [{
            "estate_code": "E1",
            "Name": "Harbour View",
            "chinese_name": "海景花園",
            "estate_detailed_address": "1 Harbour Road",
            "Region": "Hong Kong",
            "District": "Wan Chai",
            "occupation_permit": str(FUTURE_COMPLETION_YEAR),
        }]
    )
    transactions = pd.DataFrame(
        [
            {
                "building_code": "B1",
                "estate_code": "E1",
                "building_name": "Tower 1",
                "street_address": "1, Harbour Road",
                "region": "Hong Kong",
                "district": "Wan Chai",
            },
            {
                "building_code": "B1",
                "estate_code": "E1",
                "building_name": "Tower 1",
                "street_address": "9 Alternate Road",
                "region": "Hong Kong",
                "district": "Wan Chai",
            },
            {
                "building_code": "B1",
                "estate_code": "E1",
                "building_name": "Tower 1",
                "street_address": "1, Harbour Road",
                "region": "Hong Kong",
                "district": "Wan Chai",
            },
        ]
    )

    result = adapt_centaline_residential(estates, transactions)

    assert list(result.columns) == SOURCE_RECORD_COLUMNS
    assert result["source_record_id"].is_unique
    estate, building = result.iloc
    assert estate["entity_grain"] == "estate_phase"
    assert building["entity_grain"] == "building"
    assert building["native_parent_id"] == "E1"
    assert building["observed_count"] == THREE_OBSERVATIONS
    assert building["address_en"] == "1, Harbour Road"
    assert building["normalized_address"] == "1 HARBOUR ROAD"
    assert estate["completion_year"] == FUTURE_COMPLETION_YEAR
    assert estate["age"] == 0
    assert estate["name_zh"] == "海景花園"
    assert estate["observed_count"] == THREE_OBSERVATIONS
    assert estate["geo_confidence"] == 1.0


def test_centaline_residential_recovers_only_unique_estate_name_parents() -> None:
    estates = pd.DataFrame(
        [
            {"estate_code": "EST-UNIQUE", "Name": "Harbour View"},
            {"estate_code": "EST-DUPLICATE-1", "Name": "Duplicate Court"},
            {"estate_code": "EST-DUPLICATE-2", "Name": "Duplicate Court"},
        ]
    )
    transactions = pd.DataFrame(
        [
            {
                "building_code": "BLD-EXPLICIT",
                "building_name": "Explicit Tower",
                "estate_code": "EST-EXPLICIT",
                "estate_name": "Harbour View",
            },
            {
                "building_code": "BLD-UNIQUE",
                "building_name": "Unique Tower",
                "estate_code": pd.NA,
                "estate_name": "harbour view",
            },
            {
                "building_code": "BLD-AMBIGUOUS",
                "building_name": "Ambiguous Tower",
                "estate_code": pd.NA,
                "estate_name": "Duplicate Court",
            },
            {
                "building_code": "BLD-BLANK",
                "building_name": "Blank Tower",
                "estate_code": pd.NA,
                "estate_name": pd.NA,
            },
        ]
    )

    result = adapt_centaline_residential(estates, transactions)
    repeated_result = adapt_centaline_residential(estates, transactions)
    parents = result.set_index("native_id")["native_parent_id"]

    assert parents["BLD-EXPLICIT"] == "EST-EXPLICIT"
    assert parents["BLD-UNIQUE"] == "EST-UNIQUE"
    assert pd.isna(parents["BLD-AMBIGUOUS"])
    assert pd.isna(parents["BLD-BLANK"])
    assert {"BLD-EXPLICIT", "BLD-UNIQUE", "BLD-AMBIGUOUS", "BLD-BLANK"}.issubset(
        result["native_id"]
    )
    pd.testing.assert_frame_equal(result, repeated_result)


def test_midland_residential_retains_parent_and_estate_ids() -> None:
    estates = pd.DataFrame(
        [{
            "id": "EST-1",
            "parent_estate_id": "PARENT-1",
            "name": "Garden Phase 1",
            "news_name": "花園一期",
            # Midland's subregion is the 18-district level, its finer columns
            # are the locality — the same levels Centaline calls district and
            # subdistrict.
            "subregion_name": "Sha Tin",
            "sm_district_name": "Ma On Shan",
        }]
    )
    transactions = pd.DataFrame(
        [
            {
                "building_id": "BLD-1",
                "estate_id": "EST-1",
                "building": "Block A",
                "location": "2 Garden Lane",
                "district": "Ma On Shan",
                "news_name": "花園一期",
                "subregion_name": "Sha Tin",
            }
        ]
    )

    result = adapt_midland_residential(estates, transactions)

    assert result.loc[0, "native_parent_id"] == "PARENT-1"
    assert result.loc[0, "name_zh"] == "花園一期"
    assert result.loc[0, "district"] == "Sha Tin"
    assert result.loc[0, "subdistrict"] == "Ma On Shan"
    assert result.loc[0, "observed_count"] == 1
    candidate = result.loc[result["entity_grain"].eq("building")].iloc[0]
    assert candidate["native_id"] == "BLD-1"
    assert candidate["native_parent_id"] == "EST-1"
    assert candidate["address_en"] == "2 Garden Lane"


def test_midland_residential_rejects_coordinate_only_locations() -> None:
    estates = pd.DataFrame(
        [
            {
                "id": "EST-DICT",
                "name": "Coordinate Estate",
                "location": {"lat": 22.44, "lon": 114.03},
                "location_lat": HK_LATITUDE,
                "location_lon": HK_LONGITUDE,
            },
            {
                "id": "EST-PAIR",
                "name": "Pair Estate",
                "location": "22.44, 114.03",
            },
        ]
    )
    transactions = pd.DataFrame(
        [
            {
                "estate_id": "EST-DICT",
                "building_id": "BLD-DICT",
                "building": "Coordinate Building",
                "location": "{'lat': 22.44, 'lon': 114.03}",
            },
            {
                "estate_id": "EST-PAIR",
                "building_id": "BLD-PAIR",
                "building": "Pair Building",
                "location": "(22.44, 114.03)",
            },
            {
                "estate_id": "EST-TEXT",
                "building_id": "BLD-TEXT",
                "building": "Tai Nan Building",
                "location": "Tai Nan Street",
            },
            {
                "estate_id": "EST-TEXT",
                "building_id": "BLD-CJK",
                "building": "大南大廈",
                "location": "大南街",
            },
        ]
    )

    result = adapt_midland_residential(estates, transactions)

    dict_estate = result.loc[result["native_id"].eq("EST-DICT")].iloc[0]
    pair_estate = result.loc[result["native_id"].eq("EST-PAIR")].iloc[0]
    assert pd.isna(dict_estate["address_en"])
    assert dict_estate["latitude"] == HK_LATITUDE
    assert dict_estate["longitude"] == HK_LONGITUDE
    assert pd.isna(pair_estate["address_en"])
    assert pd.isna(result.loc[result["native_id"].eq("BLD-DICT"), "address_en"].iloc[0])
    assert pd.isna(result.loc[result["native_id"].eq("BLD-PAIR"), "address_en"].iloc[0])
    assert result.loc[result["native_id"].eq("BLD-TEXT"), "address_en"].iloc[0] == "Tai Nan Street"
    assert result.loc[result["native_id"].eq("BLD-CJK"), "address_en"].iloc[0] == "大南街"


def test_centaline_commercial_creates_only_unmatched_transaction_aliases() -> None:
    buildings = pd.DataFrame(
        [{"property_id": "CAT-1", "building_name": "Central Plaza", "completion_year": "1999"}]
    )
    transactions = pd.DataFrame(
        [
            {
                "propertyId": "CAT-1",
                "propertyNameEn": "Central Plaza",
                "propertyNameCn": "中環廣場",
                "full_address": "3 Queen's Road, Central",
                "zoneEn": "Hong Kong",
                "districtNameEn": "Central",
            },
            {
                "property_id": "TX-2",
                "propertyNameEn": "New Tower",
                "propertyNameCn": "新大廈",
                "addressDisplayName": "4 Canton Road",
                "zoneEn": "Kowloon",
            },
            {
                "property_id": "TX-2",
                "propertyNameEn": "New Tower",
                "propertyNameCn": "新大廈",
                "addressDisplayName": "4 Canton Road",
                "zoneEn": "Kowloon",
            },
        ]
    )

    result = adapt_centaline_commercial(buildings, transactions)

    assert len(result) == TWO_OBSERVATIONS
    catalog = result.loc[result["native_id"].eq("CAT-1")].iloc[0]
    assert catalog["name_zh"] == "中環廣場"
    assert catalog["observed_count"] == 1
    assert catalog["geo_confidence"] == 1.0
    assert catalog["address_en"] == "3 Queen's Road, Central"
    alias = result.loc[result["native_id"].eq("TX-2")].iloc[0]
    assert alias["source_dataset"] == "transaction_building_aliases"
    assert alias["observed_count"] == TWO_OBSERVATIONS
    assert alias["name_zh"] == "新大廈"
    # addressDisplayName is the listing headline ("... Mid"), not an address, and
    # alias rows carry no full_address, so the record is left without one rather
    # than with a value whose floor token shifts its fingerprint id every run.
    assert pd.isna(alias["address_en"])


def test_midland_commercial_uses_modal_transaction_geography() -> None:
    buildings = pd.DataFrame([{"id": "ICI-1", "Building Name": "Trade Centre"}])
    transactions = pd.DataFrame(
        [
            {
                "id": "ICI-1",
                "zoneEn": "Kowloon",
                "dist_name_en": "Kwun Tong",
                "chi_name": "貿易中心",
                "streetno": "1",
                "street_name_en": "Trade Street",
                "street_name_zh": "貿易街",
            },
            {
                "id": "ICI-1",
                "zoneEn": "Kowloon",
                "dist_name_en": "Kwun Tong",
                "chi_name": "貿易中心",
                "streetno": "1",
                "street_name_en": "Trade Street",
                "street_name_zh": "貿易街",
            },
            {"id": "ICI-1", "zoneEn": "Hong Kong", "dist_name_en": "Central", "chi_name": "貿易中心"},
            {
                "building_id": "ICI-2",
                "eng_name": "Alias Building",
                "chi_name": "別名大廈",
                "street_name_zh": "別名街",
                "zoneEn": "Kowloon",
            },
        ]
    )

    result = adapt_midland_commercial(buildings, transactions)

    catalog = result.loc[result["native_id"].eq("ICI-1")].iloc[0]
    assert catalog["region"] == "Kowloon"
    assert catalog["district"] == "Kwun Tong"
    assert catalog["observed_count"] == THREE_OBSERVATIONS
    assert catalog["geo_confidence"] == MODAL_GEO_CONFIDENCE
    assert catalog["name_zh"] == "貿易中心"
    assert catalog["address_en"] == "1 Trade Street"
    assert catalog["address_zh"] == "1 貿易街"
    assert catalog["normalized_address"] == "1 TRADE STREET"
    alias = result.loc[result["native_id"].eq("ICI-2")].iloc[0]
    assert pd.isna(alias["address_en"])
    assert alias["address_zh"] == "別名街"
    assert alias["normalized_address"] == "別名街"
    assert "nan" not in alias["address_zh"].lower()
    assert result["source_record_id"].is_unique


def test_combined_source_records_has_ordered_unique_ids() -> None:
    centaline_residential = adapt_centaline_residential(pd.DataFrame(), pd.DataFrame())
    midland_residential = adapt_midland_residential(pd.DataFrame(), pd.DataFrame())
    centaline_commercial = adapt_centaline_commercial(
        pd.DataFrame([{"property_id": "one", "building_name": "One"}]), pd.DataFrame()
    )
    midland_commercial = adapt_midland_commercial(
        pd.DataFrame([{"id": "one", "Building Name": "One"}]), pd.DataFrame()
    )

    result = combine_source_records(
        centaline_residential,
        midland_residential,
        centaline_commercial,
        midland_commercial,
    )

    assert list(result.columns) == SOURCE_RECORD_COLUMNS
    assert result["source_record_id"].is_unique


def test_commercial_evidence_is_aggregated_once_per_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []
    aggregate = adapter_nodes._aggregate_transaction_evidence

    def record_aggregate(*args: object, **kwargs: object) -> dict[str, dict[str, object]]:
        calls.append(kwargs["id_columns"])  # type: ignore[index]
        return aggregate(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(adapter_nodes, "_aggregate_transaction_evidence", record_aggregate)
    catalog_ids = [f"ID-{index}" for index in range(100)]
    centaline_catalog = pd.DataFrame(
        {"property_id": catalog_ids, "building_name": catalog_ids}
    )
    centaline_transactions = pd.DataFrame(
        {
            "propertyId": [catalog_ids[index % len(catalog_ids)] for index in range(5_000)],
            "propertyNameEn": ["Building"] * 5_000,
        }
    )
    midland_catalog = pd.DataFrame({"id": catalog_ids, "Building Name": catalog_ids})
    midland_transactions = pd.DataFrame(
        {
            "building_id": [catalog_ids[index % len(catalog_ids)] for index in range(5_000)],
            "eng_name": ["Building"] * 5_000,
        }
    )

    adapt_centaline_commercial(centaline_catalog, centaline_transactions)
    adapt_midland_commercial(midland_catalog, midland_transactions)

    assert calls == [("propertyId", "property_id"), ("building_id", "id")]


def test_intra_source_duplicates_collapse_into_one_record() -> None:
    """Centaline emits the same estate under two native ids differing by prefix."""
    from property_scraper.pipelines.building_master.nodes import _finalize

    def row(native_id: str, observed: int) -> dict[str, object]:
        return {
            "source_record_id": f"centaline::{native_id}",
            "source_system": "centaline",
            "source_domain": "residential",
            "source_dataset": "estate_catalog",
            "entity_grain": "estate_phase",
            "native_id": native_id,
            "normalized_name": "CHINGNGACOURT",
            "normalized_address": "1EXAMPLEROAD",
            "district": "Sha Tin",
            "observed_count": observed,
        }

    result = _finalize([row("1-ABBEGPWYPP", 4), row("2-ABBEGPWYPP", 7)])

    assert len(result) == 1
    assert result.iloc[0]["native_id"] == "2-ABBEGPWYPP"
    assert result.iloc[0]["observed_count"] == 11


def test_same_name_at_different_addresses_is_not_collapsed() -> None:
    from property_scraper.pipelines.building_master.nodes import _finalize

    def row(native_id: str, address: str) -> dict[str, object]:
        return {
            "source_record_id": f"centaline::{native_id}",
            "source_system": "centaline",
            "source_domain": "residential",
            "source_dataset": "estate_catalog",
            "entity_grain": "estate_phase",
            "native_id": native_id,
            "normalized_name": "WINGFAIBUILDING",
            "normalized_address": address,
            "district": "Sha Tin",
            "observed_count": 3,
        }

    result = _finalize([row("A", "1EXAMPLEROAD"), row("B", "99OTHERROAD")])

    assert len(result) == 2


def test_same_generic_name_under_different_parents_is_not_collapsed() -> None:
    """Midland files 56 different Yuen Long estates' "Block 2" under that name
    with no address, so name plus district cannot identify a building."""
    from property_scraper.pipelines.building_master.nodes import _finalize

    def row(native_id: str, parent: str) -> dict[str, object]:
        return {
            "source_record_id": f"midland::{native_id}",
            "source_system": "midland",
            "source_domain": "residential",
            "source_dataset": "transaction_building_candidates",
            "entity_grain": "building",
            "native_id": native_id,
            "native_parent_id": parent,
            "normalized_name": "BLOCK2",
            "normalized_address": pd.NA,
            "district": "Yuen Long",
            "observed_count": 5,
        }

    result = _finalize([row("B1", "EST-1"), row("B2", "EST-2"), row("B3", "EST-1")])

    assert len(result) == 2
    assert set(result["native_parent_id"]) == {"EST-1", "EST-2"}


def test_name_without_address_or_parent_is_never_collapsed() -> None:
    from property_scraper.pipelines.building_master.nodes import _finalize

    def row(native_id: str) -> dict[str, object]:
        return {
            "source_record_id": f"midland::{native_id}",
            "source_system": "midland",
            "source_domain": "residential",
            "source_dataset": "transaction_building_candidates",
            "entity_grain": "building",
            "native_id": native_id,
            "native_parent_id": pd.NA,
            "normalized_name": "BLOCKA",
            "normalized_address": pd.NA,
            "district": "Tuen Mun",
            "observed_count": 2,
        }

    assert len(_finalize([row("X"), row("Y")])) == 2
