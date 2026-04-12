from __future__ import annotations

from typing import Any


def get_source_settings(params: dict[str, Any], source_key: str) -> dict[str, Any]:
    section = params.get(source_key, {})
    if not isinstance(section, dict):
        raise ValueError(f"Expected mapping config for '{source_key}' in webscraper parameters.")
    return section


def get_required_setting(params: dict[str, Any], source_key: str, *keys: str) -> Any:
    current: Any = get_source_settings(params, source_key)
    path = [source_key, *keys]
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            joined = ".".join(path)
            raise ValueError(
                f"Missing required private config '{joined}'. "
                "Define it in conf/local/parameters.yml."
            )
        current = current[key]

    if current in ("", None, {}, []):
        joined = ".".join(path)
        raise ValueError(
            f"Blank private config '{joined}'. Define it in conf/local/parameters.yml."
        )
    return current


def get_optional_setting(
    params: dict[str, Any], source_key: str, *keys: str, default: Any = None
) -> Any:
    current: Any = get_source_settings(params, source_key)
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current
