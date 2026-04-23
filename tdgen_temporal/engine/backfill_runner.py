"""
Backfill runner — calls DailyRunner in a loop over a date range.
"""

from datetime import date, timedelta
from pathlib import Path

import yaml
from faker import Faker

from tdgen_temporal.db.state_store import StateStore
from tdgen_temporal.engine.daily_runner import DailyRunner
from tdgen_temporal.generators.field_generators import make_faker
import random


def run_backfill(
    db_path: Path,
    config_path: Path,
    from_date: date,
    to_date: date,
    output_root: Path,
) -> dict:
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    seed   = config.get("simulation", {}).get("seed", 42)
    fake   = make_faker(seed)
    rng    = random.Random(seed)

    store  = StateStore(db_path)
    runner = DailyRunner(store, config, output_root, fake, rng)

    total_days   = 0
    total_inserts: dict[str, int] = {}
    total_updates: dict[str, int] = {}

    current = from_date
    while current <= to_date:
        result = runner.run(current)
        total_days += 1
        for k, v in result.inserts.items():
            total_inserts[k] = total_inserts.get(k, 0) + v
        for k, v in result.updates.items():
            total_updates[k] = total_updates.get(k, 0) + v

        _print_day(current, result)
        current += timedelta(days=1)

    store.close()
    return {
        "from_date":     from_date.isoformat(),
        "to_date":       to_date.isoformat(),
        "days_processed": total_days,
        "total_inserts": total_inserts,
        "total_updates": total_updates,
    }


def _print_day(run_date: date, result) -> None:
    ins_total = sum(result.inserts.values())
    upd_total = sum(result.updates.values())
    txn_count = result.inserts.get("TRANSACTION", 0)
    print(
        f"  {run_date}  |  {ins_total:5,} inserts  "
        f"({txn_count:5,} txns)  |  {upd_total:5,} updates  "
        f"|  {result.duration:.1f}s"
    )
