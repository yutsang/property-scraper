#!/usr/bin/env python
"""
Backup current Source A Res transaction data and run a full rescrape using Playwright.
- Backs up centaline_res_trans_lv_0.parquet to data/backups/
- Runs kedro with transaction_full_rerun=true to rescrape all transactions

Usage: python scripts/backup_and_rescrape_centaline_transactions.py
Then:   kedro run --pipeline source_a_res --params source_a_res.transaction_full_rerun=true
"""
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

TRANS_FILE = "data/01_raw/centaline_res_trans_lv_0.parquet"
BACKUP_DIR = "data/backups"


def main():
    base = Path(".")
    src = base / TRANS_FILE

    if not src.exists():
        print(f"⚠️  No existing transaction file at {src}")
        print("   Proceeding without backup (first run).")
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = base / BACKUP_DIR / f"source_a_res_trans_lv_0_{timestamp}.parquet"
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, backup_path)
        print(f"✅ Backed up transactions to {backup_path}")

    print("\n🚀 Run full transaction rescrape with:")
    print("   kedro run --pipeline source_a_res --params webscraper.source_a_res.transaction_full_rerun=true")
    print("\n   Or run the full pipeline now? (y/n): ", end="")
    try:
        if input().strip().lower() == "y":
            result = subprocess.run(
                ["kedro", "run", "--pipeline", "source_a_res", "--params", "webscraper.source_a_res.transaction_full_rerun=true"],
                cwd=base,
            )
            sys.exit(result.returncode)
    except EOFError:
        pass


if __name__ == "__main__":
    main()
