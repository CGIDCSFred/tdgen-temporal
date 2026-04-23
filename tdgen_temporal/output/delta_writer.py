"""
Writes INSERT and UPDATE delta files for each day's run.

Output layout:
    output/deltas/YYYY-MM-DD/
        inserts/TRANSACTION.csv
        inserts/TRANSACTION.json
        updates/ACCOUNT.csv
        updates/ACCOUNT.json
"""

import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd


@dataclass
class DeltaSet:
    run_date: date
    inserts: dict[str, list[dict]] = field(default_factory=dict)
    updates: dict[str, list[dict]] = field(default_factory=dict)


class DeltaWriter:
    def __init__(self, output_root: Path, formats: list[str]) -> None:
        self._root = output_root
        self._formats = formats

    def write(self, delta: DeltaSet) -> dict[str, list[str]]:
        written: dict[str, list[str]] = {"inserts": [], "updates": []}
        day_dir = self._root / "deltas" / delta.run_date.isoformat()

        for category, table_rows in (("inserts", delta.inserts), ("updates", delta.updates)):
            for table_name, rows in table_rows.items():
                if not rows:
                    continue
                # Strip internal-only keys (prefixed with _)
                clean = [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows]
                paths = self._write_table(clean, table_name, category, day_dir)
                written[category].extend(str(p) for p in paths)

        return written

    def _write_table(
        self,
        rows: list[dict],
        table_name: str,
        category: str,
        day_dir: Path,
    ) -> list[Path]:
        out_dir = day_dir / category
        out_dir.mkdir(parents=True, exist_ok=True)
        paths: list[Path] = []

        if "csv" in self._formats:
            p = out_dir / f"{table_name}.csv"
            pd.DataFrame(rows).to_csv(p, index=False)
            paths.append(p)

        if "json" in self._formats:
            p = out_dir / f"{table_name}.json"
            p.write_text(json.dumps(rows, indent=2, default=str), encoding="utf-8")
            paths.append(p)

        return paths
