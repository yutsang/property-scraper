import os
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class NodeExecutionTracker:
    """
    Utility class to track node execution dates and determine if nodes should be re-run
    based on anti-scraping requirements.
    """
    
    def __init__(self, tracking_file: str = "data/node_execution_tracker.json"):
        """
        Initialize the node execution tracker.
        
        Args:
            tracking_file: Path to the JSON file storing execution timestamps
        """
        self.tracking_file = tracking_file
        self.execution_data = self._load_execution_data()
    
    def _load_execution_data(self) -> Dict[str, Any]:
        """Load execution data from JSON file."""
        if os.path.exists(self.tracking_file):
            try:
                with open(self.tracking_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load execution tracker: {e}")
                return {}
        return {}
    
    def _save_execution_data(self):
        """Save execution data to JSON file."""
        try:
            os.makedirs(os.path.dirname(self.tracking_file), exist_ok=True)
            with open(self.tracking_file, 'w') as f:
                json.dump(self.execution_data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save execution tracker: {e}")
    
    def should_run_node(self, node_name: str, node_type: str = "default", tracking_params: Optional[Dict[str, Any]] = None, data_file_path: Optional[str] = None) -> bool:
        """
        Determine if a node should be run based on data freshness (max date in dataset).
        
        NOTE: This method now ALWAYS returns True for non-transaction nodes.
        Only transaction nodes perform date-based checks using actual data.
        
        Args:
            node_name: Name of the node
            node_type: Type of node ('transaction', 'estate', 'building', 'default')
            tracking_params: Optional parameters for configuration (unused)
            data_file_path: Path to the data file to check max date (transaction nodes only)
            
        Returns:
            bool: True if node should be run (always True except for up-to-date transaction nodes)
        """
        # For transaction nodes, check max date in dataset vs current date
        if node_type == "transaction" and data_file_path:
            return self._should_run_transaction_node(node_name, data_file_path, tracking_params)
        
        # For all other nodes (estate, building, default): ALWAYS RUN
        # No more "skip based on days since last run" logic
        return True
    
    def _should_run_transaction_node(self, node_name: str, data_file_path: str, tracking_params: Optional[Dict[str, Any]] = None) -> bool:
        """
        Check if transaction node should run by comparing max date in dataset with current date.
        
        Logic:
        - If max_date >= today: SKIP (dataset is up to date, no new data available)
        - If max_date < today: RUN (scrape from max_date+1 to today)
        """
        if not os.path.exists(data_file_path):
            logger.info(f"📊 Node '{node_name}': No existing data file - initial scrape required")
            return True
        
        try:
            # Load existing data to check max date
            if data_file_path.endswith('.parquet'):
                df = pd.read_parquet(data_file_path)
            elif data_file_path.endswith('.csv'):
                df = pd.read_csv(data_file_path)
            else:
                logger.warning(f"⚠️  Node '{node_name}': Unsupported file format - will execute")
                return True
            
            if df.empty:
                logger.info(f"📊 Node '{node_name}': Empty dataset - initial scrape required")
                return True
            
            # Find date column
            date_columns = ['date', 'transaction_date', 'tx_date', 'Date', 'transactionDate']
            date_col = None
            
            for col in date_columns:
                if col in df.columns:
                    date_col = col
                    break
            
            if not date_col:
                logger.warning(f"⚠️  Node '{node_name}': No date column found - will execute")
                return True
            
            # Parse dates and find max
            df_temp = df.copy()
            df_temp['parsed_date'] = df_temp[date_col].apply(self._parse_date_string)
            valid_dates = df_temp['parsed_date'].dropna()
            
            if valid_dates.empty:
                logger.info(f"📊 Node '{node_name}': No valid dates found - will execute")
                return True
            
            # Calculate max date
            max_date = valid_dates.max()
            current_date = datetime.now().date()
            
            # Check if we need to scrape
            if max_date >= current_date:
                logger.info(f"✅ Node '{node_name}': Data up-to-date (max: {max_date}) - skipping")
                return False
            else:
                days_behind = (current_date - max_date).days
                logger.info(f"📊 Node '{node_name}': Data {days_behind} days behind (max: {max_date}) - will scrape")
                return True
                
        except Exception as e:
            logger.warning(f"⚠️  Error checking data for '{node_name}': {e}")
            logger.info(f"📊 Will execute node for safety")
            return True
    
    def _parse_date_string(self, date_str):
        """
        Parse date string using format detection.
        Handles both ISO format (yyyy-mm-dd) and Hong Kong format (dd/mm/yyyy).
        """
        if not date_str or pd.isna(date_str):
            return None

        date_str = str(date_str).strip()
        
        # Quick format detection
        is_iso_format = (
            'T' in date_str or  # ISO timestamp with time
            (len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-')  # yyyy-mm-dd
        )
        is_slash_format = '/' in date_str  # dd/mm/yyyy or mm/dd/yyyy
        
        # Try explicit format parsing first (faster and cleaner)
        date_formats = []
        
        if is_iso_format:
            # ISO formats - try without dayfirst
            date_formats = [
                '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO with milliseconds and Z
                '%Y-%m-%dT%H:%M:%S.%f',   # ISO with milliseconds
                '%Y-%m-%dT%H:%M:%SZ',     # ISO without milliseconds
                '%Y-%m-%dT%H:%M:%S',      # ISO without timezone
                '%Y-%m-%d',               # Simple yyyy-mm-dd
            ]
        elif is_slash_format:
            # Slash formats - try dd/mm/yyyy first (Hong Kong standard)
            date_formats = [
                '%d/%m/%Y',   # dd/mm/yyyy (Hong Kong)
                '%m/%d/%Y',   # mm/dd/yyyy (US)
            ]
        else:
            # Other formats
            date_formats = [
                '%Y%m%d',     # yyyymmdd
                '%d-%m-%Y',   # dd-mm-yyyy
                '%Y-%m-%d',   # yyyy-mm-dd
            ]
        
        # Try each format
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except (ValueError, TypeError):
                continue
        
        # Fallback to pandas auto-detection (only if explicit formats failed)
        try:
            # Use dayfirst only for slash formats that aren't ISO
            use_dayfirst = is_slash_format and not is_iso_format
            parsed = pd.to_datetime(date_str, errors='coerce', dayfirst=use_dayfirst)
            if pd.notna(parsed):
                return parsed.date()
        except:
            pass
        
        return None
    
    def record_node_execution(self, node_name: str, node_type: str = "default", 
                            metadata: Optional[Dict[str, Any]] = None):
        """
        Record that a node has been executed.
        
        Args:
            node_name: Name of the node
            node_type: Type of node ('transaction', 'estate', 'default')
            metadata: Additional metadata to store
        """
        current_time = datetime.now().isoformat()
        
        self.execution_data[node_name] = {
            'last_run': current_time,
            'node_type': node_type,
            'execution_count': self.execution_data.get(node_name, {}).get('execution_count', 0) + 1,
            'metadata': metadata or {}
        }
        
        self._save_execution_data()
        logger.info(f"Recorded execution for node '{node_name}' at {current_time}")
    
    def get_node_status(self, node_name: str) -> Dict[str, Any]:
        """
        Get the current status of a node.
        
        Args:
            node_name: Name of the node
            
        Returns:
            Dict containing node status information
        """
        if node_name not in self.execution_data:
            return {
                'node_name': node_name,
                'last_run': None,
                'node_type': None,
                'execution_count': 0,
                'should_run': True,
                'days_since_last_run': None
            }
        
        data = self.execution_data[node_name]
        last_run = data.get('last_run')
        
        if last_run:
            try:
                last_run_date = datetime.fromisoformat(last_run)
                current_date = datetime.now()
                days_since_last_run = (current_date - last_run_date).days
            except:
                days_since_last_run = None
        else:
            days_since_last_run = None
        
        return {
            'node_name': node_name,
            'last_run': last_run,
            'node_type': data.get('node_type'),
            'execution_count': data.get('execution_count', 0),
            'should_run': self.should_run_node(node_name, data.get('node_type')),
            'days_since_last_run': days_since_last_run,
            'metadata': data.get('metadata', {})
        }
    
    def get_all_node_statuses(self) -> Dict[str, Dict[str, Any]]:
        """
        Get status of all tracked nodes.
        
        Returns:
            Dict mapping node names to their status
        """
        return {node_name: self.get_node_status(node_name) 
                for node_name in self.execution_data.keys()}
    
    def reset_node(self, node_name: str):
        """
        Reset a node's execution history.
        
        Args:
            node_name: Name of the node to reset
        """
        if node_name in self.execution_data:
            del self.execution_data[node_name]
            self._save_execution_data()
            logger.info(f"Reset execution history for node '{node_name}'")
    
    def reset_all_nodes(self):
        """Reset all node execution history."""
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

def should_run_node(node_name: str, node_type: str = "default", tracking_params: Optional[Dict[str, Any]] = None, data_file_path: Optional[str] = None) -> bool:
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
    return get_node_tracker().should_run_node(node_name, node_type, tracking_params, data_file_path)

def record_node_execution(node_name: str, node_type: str = "default", 
                         metadata: Optional[Dict[str, Any]] = None):
    """
    Convenience function to record node execution.
    
    Args:
        node_name: Name of the node
        node_type: Type of node ('transaction', 'estate', 'default')
        metadata: Additional metadata to store
    """
    get_node_tracker().record_node_execution(node_name, node_type, metadata)

def get_node_status(node_name: str) -> Dict[str, Any]:
    """
    Convenience function to get node status.
    
    Args:
        node_name: Name of the node
        
    Returns:
        Dict containing node status information
    """
    return get_node_tracker().get_node_status(node_name) 