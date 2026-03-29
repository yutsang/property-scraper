import pandas as pd

from property_scraper.pipelines.centaline_res import health


def test_check_centaline_api_health_node_runs_check_without_dataset_output(
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

    monkeypatch.setattr(health, "check_centaline_api_health", fake_check)

    result = health.check_centaline_api_health_node(
        {
            "centaline_res": {
                "health_check_max_stale_days": 7,
            }
        }
    )

    assert result is None
    assert calls == [
        {
            "params": {"centaline_res": {"health_check_max_stale_days": 7}},
            "max_stale_days": 7,
        }
    ]
