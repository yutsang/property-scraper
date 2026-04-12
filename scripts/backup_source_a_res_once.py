#!/usr/bin/env python
"""
One-time backup of Source A Res data before rerun.
Run once before: kedro run --pipeline source_a_res

Usage: python scripts/backup_source_a_res_once.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from property_scraper.utils.source_a_res_backup import backup_source_a_res_data

if __name__ == "__main__":
    result = backup_source_a_res_data(base_dir=".")
    print(f"Backed up {len(result['files_copied'])} files to {result['backup_path']}")
