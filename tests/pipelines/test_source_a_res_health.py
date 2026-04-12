import pandas as pd

from property_scraper.pipelines.source_a_res import health
from property_scraper.pipelines.source_a_res.pipeline import create_pipeline


def test_check_source_a_health_node_runs_check_without_dataset_output(
    monkeypatch,
) -> None:
    calls: list[dict] = []

    def fake_check(params, max_stale_days):
        calls.append(
            {
                "params": params,
                "max_stale_days": max_stale_days,
            }
        )
        return {"ok": True, "latest_date": "2026-03-26"}

    monkeypatch.setattr(health, "check_source_a_health", fake_check)

    result = health.check_source_a_health_node(
        {
            "source_a_res": {
                "health_check_max_stale_days": 7,
            }
        }
    )

    assert result is None
    assert calls == [
        {
            "params": {"source_a_res": {"health_check_max_stale_days": 7}},
            "max_stale_days": 7,
        }
    ]


def test_source_a_res_pipeline_builds_with_health_check_side_effect_node() -> None:
    pipeline = create_pipeline()

    health_node = next(
        node for node in pipeline.nodes if node.name == "check_api_health"
    )

    assert health_node.outputs == []
