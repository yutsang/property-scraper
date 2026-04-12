"""
Backup Source A Res pipeline data before run.
Copies parquet/CSV files to data/backups/source_a_res_<timestamp>/.
"""
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

CENTALINE_FILES = [
    "data/01_raw/Centanet_Res_Area_Code.csv",
    "data/01_raw/centaline_estate_lv_1.parquet",
    "data/01_raw/centaline_estate_lv_2.parquet",
    "data/01_raw/centaline_res_trans_lv_0.parquet",
    "data/02_intermediate/centaline_res_trans_lv_1.parquet",
    "data/02_intermediate/centaline_res_base.parquet",
    "data/03_primary/centaline_res.parquet",
]
META_FILES = [
    "data/01_raw/centaline_estate_lv_1_meta.json",
]


def backup_source_a_res_data(
    base_dir: str = ".",
    backup_subdir: str = "data/backups",
) -> dict:
    """
    Backup Source A Res pipeline outputs to timestamped folder.
    Returns dict with backup_path, files_copied, files_skipped.
    """
    base = Path(base_dir)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = base / backup_subdir / f"source_a_res_{timestamp}"
    backup_path.mkdir(parents=True, exist_ok=True)

    copied = []
    skipped = []

    for rel in CENTALINE_FILES + META_FILES:
        src = base / rel
        if not src.exists():
            skipped.append(rel)
            continue
        dst = backup_path / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(src, dst)
            copied.append(rel)
        except Exception as e:
            logger.warning(f"Could not backup {rel}: {e}")
            skipped.append(rel)

    logger.info(f"Backed up {len(copied)} Source A Res files to {backup_path}")
    return {
        "backup_path": str(backup_path),
        "files_copied": copied,
        "files_skipped": skipped,
    }


def find_latest_backup(
    target_filename: str,
    base_dir: str = ".",
    backup_subdir: str = "data/backups",
) -> Optional[Path]:
    """Return the newest backup file matching a target filename."""
    backup_root = Path(base_dir) / backup_subdir
    if not backup_root.exists():
        return None

    candidates = [
        path for path in backup_root.rglob(target_filename)
        if path.is_file()
    ]
    if not candidates:
        return None

    return max(candidates, key=lambda path: path.stat().st_mtime)


def restore_latest_backup(
    target_rel_path: str,
    base_dir: str = ".",
    backup_subdir: str = "data/backups",
) -> dict:
    """
    Restore the newest backup that matches the target file name.

    Returns a dict with keys:
    - restored (bool)
    - backup_path (str | None)
    - target_path (str)
    - message (str)
    """
    base = Path(base_dir)
    target_path = base / target_rel_path
    latest_backup = find_latest_backup(
        target_path.name,
        base_dir=base_dir,
        backup_subdir=backup_subdir,
    )

    if latest_backup is None:
        return {
            "restored": False,
            "backup_path": None,
            "target_path": str(target_path),
            "message": f"No backup found for {target_path.name}",
        }

    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(latest_backup, target_path)
    logger.warning("Restored %s from backup %s", target_path, latest_backup)
    return {
        "restored": True,
        "backup_path": str(latest_backup),
        "target_path": str(target_path),
        "message": f"Restored {target_path.name} from {latest_backup}",
    }
