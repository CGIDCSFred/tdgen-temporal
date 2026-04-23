"""
Full snapshot writer — dumps all TS2 entity tables to CSV/JSON.
"""

import json
from datetime import date
from pathlib import Path

import pandas as pd

from tdgen_temporal.db.state_store import StateStore

_SNAPSHOT_TABLES = [
    "CLIENT",
    "PROVIDER",
    "PRODUCT_DEFINITION",
    "MERCHANT",
    "ACCOUNT",
    "CUSTOMER",
    "CARD",
    "AUTHORIZATION",
    "TRANSACTION",
    "STATEMENT",
    "DISPUTE",
    "CHARGEBACK",
    "FRAUD_ALERT",
    "SCORE_RECORD",
    "COLLECTION_CASE",
]


class SnapshotWriter:
    def __init__(self, output_root: Path, formats: list[str]) -> None:
        self._root = output_root
        self._formats = formats

    def write(self, run_date: date, store: StateStore) -> list[str]:
        snap_dir = self._root / "snapshots" / run_date.isoformat()
        snap_dir.mkdir(parents=True, exist_ok=True)
        paths: list[str] = []

        for table in _SNAPSHOT_TABLES:
            rows = store.snapshot_table(table)
            if not rows:
                continue
            if "csv" in self._formats:
                p = snap_dir / f"{table}.csv"
                pd.DataFrame(rows).to_csv(p, index=False)
                paths.append(str(p))
            if "json" in self._formats:
                p = snap_dir / f"{table}.json"
                p.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
                paths.append(str(p))

        return paths
