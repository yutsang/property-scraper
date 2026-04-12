import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class NodeExecutionTracker:
    """
    Track node executions and lightweight live-state comparisons.

    The tracker can use:
    - local dataset freshness (e.g. max transaction date)
    - live website state (e.g. sitemap lastmod, page counts, probe fingerprints)
    - stored execution metadata for later diagnostics
    """

    def __init__(self, tracking_file: str = "data/node_execution_tracker.json"):
        self.tracking_file = tracking_file
        self.execution_data = self._load_execution_data()

    def _load_execution_data(self) -> Dict[str, Any]:
        if os.path.exists(self.tracking_file):
            try:
                with open(self.tracking_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning("Failed to load execution tracker: %s", e)
        return {}

    def _save_execution_data(self):
        try:
            os.makedirs(os.path.dirname(self.tracking_file), exist_ok=True)
            # Write atomically to prevent corruption from concurrent access
            tmp_file = self.tracking_file + ".tmp"
            with open(tmp_file, "w") as f:
                json.dump(self.execution_data, f, indent=2, default=str)
            os.replace(tmp_file, self.tracking_file)
        except Exception as e:
            logger.error("Failed to save execution tracker: %s", e)

    def _safe_value(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {str(k): self._safe_value(v) for k, v in sorted(value.items(), key=lambda item: str(item[0]))}
        if isinstance(value, list):
            return [self._safe_value(v) for v in value]
        if isinstance(value, tuple):
            return [self._safe_value(v) for v in value]
        if isinstance(value, set):
            return sorted(self._safe_value(v) for v in value)
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    def _hash_payload(self, payload: Any) -> Optional[str]:
        if payload is None:
            return None
        normalized = self._safe_value(payload)
        raw = json.dumps(normalized, sort_keys=True, ensure_ascii=True, default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _parse_date_string(self, date_str: Any):
        if not date_str or pd.isna(date_str):
            return None

        date_str = str(date_str).strip()
        is_iso_format = "T" in date_str or (
            len(date_str) == 10 and date_str[4] == "-" and date_str[7] == "-"
        )
        is_slash_format = "/" in date_str

        if is_iso_format:
            date_formats = [
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d",
            ]
        elif is_slash_format:
            date_formats = ["%d/%m/%Y", "%m/%d/%Y"]
        else:
            date_formats = ["%Y%m%d", "%d-%m-%Y", "%Y-%m-%d"]

        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except (ValueError, TypeError):
                continue

        try:
            parsed = pd.to_datetime(
                date_str,
                errors="coerce",
                dayfirst=is_slash_format and not is_iso_format,
            )
            if pd.notna(parsed):
                return parsed.date()
        except Exception:
            pass

        return None

    def build_data_file_state(self, data_file_path: Optional[str]) -> Optional[Dict[str, Any]]:
        """Summarize dataset state without mutating tracker state."""
        if not data_file_path:
            return None

        path = Path(data_file_path)
        state: Dict[str, Any] = {
            "path": str(path),
            "exists": path.exists(),
            "is_valid": False,
            "row_count": 0,
            "size_bytes": path.stat().st_size if path.exists() else 0,
            "max_date": None,
            "date_column": None,
            "modified_at": datetime.fromtimestamp(path.stat().st_mtime).isoformat() if path.exists() else None,
            "error": None,
        }

        if not path.exists():
            state["error"] = "file_missing"
            return state

        if state["size_bytes"] == 0:
            state["error"] = "file_empty"
            return state

        try:
            if path.suffix == ".parquet":
                df = pd.read_parquet(path)
            elif path.suffix == ".csv":
                df = pd.read_csv(path)
            else:
                state["error"] = f"unsupported_format:{path.suffix}"
                return state

            state["row_count"] = len(df)
            if df.empty:
                state["error"] = "dataset_empty"
                return state

            date_columns = ["date", "transaction_date", "tx_date", "Date", "transactionDate"]
            date_col = next((col for col in date_columns if col in df.columns), None)
            if date_col:
                parsed_dates = df[date_col].apply(self._parse_date_string).dropna()
                if not parsed_dates.empty:
                    state["max_date"] = parsed_dates.max().isoformat()
                    state["date_column"] = date_col

            state["is_valid"] = True
            return state
        except Exception as exc:
            state["error"] = str(exc)
            return state

    def should_run_node(
        self,
        node_name: str,
        node_type: str = "default",
        tracking_params: Optional[Dict[str, Any]] = None,
        data_file_path: Optional[str] = None,
        live_state: Optional[Dict[str, Any]] = None,
    ) -> bool:
        decision = self.evaluate_run(
            node_name=node_name,
            node_type=node_type,
            tracking_params=tracking_params,
            data_file_path=data_file_path,
            live_state=live_state,
        )
        return decision["should_run"]

    def evaluate_run(
        self,
        node_name: str,
        node_type: str = "default",
        tracking_params: Optional[Dict[str, Any]] = None,
        data_file_path: Optional[str] = None,
        live_state: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Return a detailed run/skip decision."""
        tracking_params = tracking_params or {}
        current_time = datetime.now().isoformat()
        previous = self.execution_data.get(node_name, {})
        dataset_state = self.build_data_file_state(data_file_path)

        if live_state is not None:
            current_live_hash = self._hash_payload(live_state)
            previous_live_hash = previous.get("last_live_state_hash")
            live_probe_failed = bool(live_state.get("probe_failed")) if isinstance(live_state, dict) else False

            if live_probe_failed:
                if dataset_state and dataset_state.get("is_valid") and dataset_state.get("max_date"):
                    max_date = self._parse_date_string(dataset_state["max_date"])
                    if max_date and max_date >= datetime.now().date():
                        return {
                            "should_run": False,
                            "reason": "live_probe_failed_but_local_dataset_current",
                            "live_state": live_state,
                            "dataset_state": dataset_state,
                            "evaluated_at": current_time,
                        }
                return {
                    "should_run": True,
                    "reason": "live_probe_failed_fallback_to_safe_run",
                    "live_state": live_state,
                    "dataset_state": dataset_state,
                    "evaluated_at": current_time,
                }

            if previous_live_hash and current_live_hash == previous_live_hash:
                if dataset_state and dataset_state.get("is_valid"):
                    return {
                        "should_run": False,
                        "reason": "live_state_unchanged",
                        "live_state": live_state,
                        "dataset_state": dataset_state,
                        "evaluated_at": current_time,
                    }

            return {
                "should_run": True,
                "reason": "live_state_changed_or_untracked",
                "live_state": live_state,
                "dataset_state": dataset_state,
                "evaluated_at": current_time,
            }

        if node_type == "transaction" and dataset_state:
            if not dataset_state.get("exists"):
                return {
                    "should_run": True,
                    "reason": "transaction_file_missing",
                    "dataset_state": dataset_state,
                    "evaluated_at": current_time,
                }
            if not dataset_state.get("is_valid"):
                return {
                    "should_run": True,
                    "reason": f"transaction_dataset_invalid:{dataset_state.get('error')}",
                    "dataset_state": dataset_state,
                    "evaluated_at": current_time,
                }

            max_date = self._parse_date_string(dataset_state.get("max_date"))
            if max_date and max_date >= datetime.now().date():
                return {
                    "should_run": False,
                    "reason": f"transaction_data_current:{max_date}",
                    "dataset_state": dataset_state,
                    "evaluated_at": current_time,
                }
            return {
                "should_run": True,
                "reason": f"transaction_data_stale:{dataset_state.get('max_date')}",
                "dataset_state": dataset_state,
                "evaluated_at": current_time,
            }

        return {
            "should_run": True,
            "reason": "default_run",
            "dataset_state": dataset_state,
            "evaluated_at": current_time,
        }

    def record_node_decision(
        self,
        node_name: str,
        node_type: str = "default",
        should_run: bool = True,
        reason: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        live_state: Optional[Dict[str, Any]] = None,
        dataset_state: Optional[Dict[str, Any]] = None,
    ):
        entry = self.execution_data.get(node_name, {})
        entry.update(
            {
                "node_type": node_type,
                "last_decision_at": datetime.now().isoformat(),
                "last_should_run": should_run,
                "last_reason": reason,
            }
        )
        if metadata is not None:
            entry["metadata"] = metadata
        if live_state is not None:
            entry["last_live_state"] = self._safe_value(live_state)
            entry["last_live_state_hash"] = self._hash_payload(live_state)
        if dataset_state is not None:
            entry["last_dataset_state"] = self._safe_value(dataset_state)
        self.execution_data[node_name] = entry
        self._save_execution_data()

    def record_node_execution(
        self,
        node_name: str,
        node_type: str = "default",
        metadata: Optional[Dict[str, Any]] = None,
        live_state: Optional[Dict[str, Any]] = None,
        dataset_state: Optional[Dict[str, Any]] = None,
    ):
        current_time = datetime.now().isoformat()
        existing = self.execution_data.get(node_name, {})

        entry = {
            "last_run": current_time,
            "node_type": node_type,
            "execution_count": existing.get("execution_count", 0) + 1,
            "metadata": self._safe_value(metadata or {}),
            "last_decision_at": current_time,
            "last_should_run": True,
            "last_reason": "executed",
        }
        if live_state is not None:
            entry["last_live_state"] = self._safe_value(live_state)
            entry["last_live_state_hash"] = self._hash_payload(live_state)
        else:
            entry["last_live_state"] = existing.get("last_live_state")
            entry["last_live_state_hash"] = existing.get("last_live_state_hash")

        if dataset_state is not None:
            entry["last_dataset_state"] = self._safe_value(dataset_state)
        elif existing.get("last_dataset_state") is not None:
            entry["last_dataset_state"] = existing.get("last_dataset_state")

        self.execution_data[node_name] = entry
        self._save_execution_data()
        logger.info("Recorded execution for node '%s' at %s", node_name, current_time)

    def get_node_status(self, node_name: str) -> Dict[str, Any]:
        if node_name not in self.execution_data:
            return {
                "node_name": node_name,
                "last_run": None,
                "node_type": None,
                "execution_count": 0,
                "should_run": True,
                "days_since_last_run": None,
                "last_reason": None,
            }

        data = self.execution_data[node_name]
        last_run = data.get("last_run")
        days_since_last_run = None
        if last_run:
            try:
                days_since_last_run = (datetime.now() - datetime.fromisoformat(last_run)).days
            except Exception:
                days_since_last_run = None

        return {
            "node_name": node_name,
            "last_run": last_run,
            "node_type": data.get("node_type"),
            "execution_count": data.get("execution_count", 0),
            "should_run": data.get("last_should_run", True),
            "days_since_last_run": days_since_last_run,
            "metadata": data.get("metadata", {}),
            "last_reason": data.get("last_reason"),
            "last_live_state": data.get("last_live_state"),
            "last_dataset_state": data.get("last_dataset_state"),
        }

    def get_all_node_statuses(self) -> Dict[str, Dict[str, Any]]:
        return {
            node_name: self.get_node_status(node_name)
            for node_name in self.execution_data.keys()
        }

    def reset_node(self, node_name: str):
        if node_name in self.execution_data:
            del self.execution_data[node_name]
            self._save_execution_data()
            logger.info("Reset execution history for node '%s'", node_name)

    def reset_all_nodes(self):
        self.execution_data = {}
        self._save_execution_data()
        logger.info("Reset all node execution history")


# Global tracker instance
_node_tracker = None

def get_node_tracker() -> NodeExecutionTracker:
    """Get the global node tracker instance."""
    global _node_tracker
    if _node_tracker is None:
        _node_tracker = NodeExecutionTracker()
    return _node_tracker

def should_run_node(
    node_name: str,
    node_type: str = "default",
    tracking_params: Optional[Dict[str, Any]] = None,
    data_file_path: Optional[str] = None,
    live_state: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Convenience function to check if a node should be run.
    
    Args:
        node_name: Name of the node
        node_type: Type of node ('transaction', 'estate', 'building', 'default')
        tracking_params: Optional parameters for configuration
        data_file_path: Path to the data file to check max date (for transaction nodes)
        
    Returns:
        bool: True if node should be run, False otherwise
    """
    return get_node_tracker().should_run_node(
        node_name,
        node_type,
        tracking_params,
        data_file_path,
        live_state,
    )


def evaluate_node_run(
    node_name: str,
    node_type: str = "default",
    tracking_params: Optional[Dict[str, Any]] = None,
    data_file_path: Optional[str] = None,
    live_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Convenience wrapper for detailed run/skip decisions."""
    return get_node_tracker().evaluate_run(
        node_name=node_name,
        node_type=node_type,
        tracking_params=tracking_params,
        data_file_path=data_file_path,
        live_state=live_state,
    )


def record_node_execution(
    node_name: str,
    node_type: str = "default",
    metadata: Optional[Dict[str, Any]] = None,
    live_state: Optional[Dict[str, Any]] = None,
    dataset_state: Optional[Dict[str, Any]] = None,
):
    """
    Convenience function to record node execution.
    
    Args:
        node_name: Name of the node
        node_type: Type of node ('transaction', 'estate', 'default')
        metadata: Additional metadata to store
    """
    get_node_tracker().record_node_execution(
        node_name,
        node_type,
        metadata,
        live_state=live_state,
        dataset_state=dataset_state,
    )


def record_node_decision(
    node_name: str,
    node_type: str = "default",
    should_run: bool = True,
    reason: str = "",
    metadata: Optional[Dict[str, Any]] = None,
    live_state: Optional[Dict[str, Any]] = None,
    dataset_state: Optional[Dict[str, Any]] = None,
):
    """Convenience wrapper for storing skip/run decisions before execution."""
    get_node_tracker().record_node_decision(
        node_name=node_name,
        node_type=node_type,
        should_run=should_run,
        reason=reason,
        metadata=metadata,
        live_state=live_state,
        dataset_state=dataset_state,
    )

def get_node_status(node_name: str) -> Dict[str, Any]:
    """
    Convenience function to get node status.
    
    Args:
        node_name: Name of the node
        
    Returns:
        Dict containing node status information
    """
    return get_node_tracker().get_node_status(node_name) 