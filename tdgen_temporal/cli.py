"""
CLI entry point for tdgen-temporal.

Usage:
    python -m tdgen_temporal.cli init [--db PATH] [--date YYYY-MM-DD] [--config PATH]
    python -m tdgen_temporal.cli advance [--db PATH] [--days N] [--config PATH] [--output PATH]
    python -m tdgen_temporal.cli backfill --from YYYY-MM-DD --to YYYY-MM-DD [--db PATH] [--config PATH] [--output PATH]
    python -m tdgen_temporal.cli status [--db PATH]
"""

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path


DEFAULT_DB     = Path("output/state.db")
DEFAULT_CONFIG = Path("config/scenario.yaml")
DEFAULT_OUTPUT = Path("output")


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="tdgen-temporal",
        description="Temporal synthetic test data generation for TSYS TS2",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # -- init --
    p_init = sub.add_parser("init", help="Seed Day 0 population")
    p_init.add_argument("--db",     type=Path, default=DEFAULT_DB)
    p_init.add_argument("--date",   type=str,  default=None, help="Simulation start date YYYY-MM-DD")
    p_init.add_argument("--config", type=Path, default=DEFAULT_CONFIG)

    # -- advance --
    p_adv = sub.add_parser("advance", help="Advance simulation by N days")
    p_adv.add_argument("--db",     type=Path, default=DEFAULT_DB)
    p_adv.add_argument("--days",   type=int,  default=1)
    p_adv.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    p_adv.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)

    # -- backfill --
    p_bf = sub.add_parser("backfill", help="Generate data for a historical date range")
    p_bf.add_argument("--from",   dest="from_date", type=str, required=True)
    p_bf.add_argument("--to",     dest="to_date",   type=str, required=True)
    p_bf.add_argument("--db",     type=Path, default=DEFAULT_DB)
    p_bf.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    p_bf.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)

    # -- status --
    p_st = sub.add_parser("status", help="Show current simulation state")
    p_st.add_argument("--db", type=Path, default=DEFAULT_DB)

    # -- validate --
    p_val = sub.add_parser("validate", help="Check referential, temporal, and state integrity")
    p_val.add_argument("--db",      type=Path, default=DEFAULT_DB)
    p_val.add_argument("--verbose", action="store_true", help="Show example PKs for each finding")
    p_val.add_argument("--errors-only", action="store_true", dest="errors_only",
                       help="Only report ERROR severity findings")
    p_val.add_argument("--output", type=Path, default=None,
                       help="Write an HTML report to this file (e.g. report.html)")

    args = parser.parse_args()

    if args.command == "init":
        return _cmd_init(args)
    if args.command == "advance":
        return _cmd_advance(args)
    if args.command == "backfill":
        return _cmd_backfill(args)
    if args.command == "status":
        return _cmd_status(args)
    if args.command == "validate":
        return _cmd_validate(args)
    return 1


def _cmd_init(args) -> int:
    from tdgen_temporal.engine.init_runner import run_init
    run_date = date.fromisoformat(args.date) if args.date else date.today()
    run_init(db_path=args.db, config_path=args.config, run_date=run_date)
    return 0


def _cmd_advance(args) -> int:
    import random
    import yaml
    from tdgen_temporal.db.state_store import StateStore
    from tdgen_temporal.engine.daily_runner import DailyRunner
    from tdgen_temporal.generators.field_generators import make_faker

    config = yaml.safe_load(args.config.read_text(encoding="utf-8"))
    seed   = config.get("simulation", {}).get("seed", 42)
    fake   = make_faker(seed)
    rng    = random.Random(seed)

    store  = StateStore(args.db)
    runner = DailyRunner(store, config, args.output, fake, rng)

    current_date = store.get_current_run_date()
    if current_date is None:
        print("ERROR: Database not initialised. Run `init` first.")
        store.close()
        return 1

    for i in range(args.days):
        next_date = current_date + timedelta(days=1)
        result    = runner.run(next_date)
        current_date = next_date
        ins_total = sum(result.inserts.values())
        upd_total = sum(result.updates.values())
        txn_count = result.inserts.get("TRANSACTION", 0)
        print(
            f"  {next_date}  |  {ins_total:5,} inserts  "
            f"({txn_count:5,} txns)  |  {upd_total:5,} updates  "
            f"|  {result.duration:.1f}s"
        )
        if result.errors:
            for e in result.errors:
                print(f"  [WARN] {e}")

    store.close()
    return 0


def _cmd_backfill(args) -> int:
    from tdgen_temporal.engine.backfill_runner import run_backfill

    from_date = date.fromisoformat(args.from_date)
    to_date   = date.fromisoformat(args.to_date)

    if to_date < from_date:
        print("ERROR: --to date must be >= --from date")
        return 1

    print(f"\nBackfilling {from_date} -> {to_date} ({(to_date - from_date).days + 1} days)")
    summary = run_backfill(
        db_path=args.db,
        config_path=args.config,
        from_date=from_date,
        to_date=to_date,
        output_root=args.output,
    )
    print(f"\nBackfill complete — {summary['days_processed']} days processed")
    for k, v in summary["total_inserts"].items():
        print(f"  {k:<30} {v:>8,} inserts")
    return 0


def _cmd_status(args) -> int:
    from tdgen_temporal.db.state_store import StateStore

    if not args.db.exists():
        print(f"Database not found: {args.db}")
        return 1

    store = StateStore(args.db)
    meta  = store.get_simulation_meta()

    if meta is None:
        print("Database exists but has not been initialised.")
        store.close()
        return 0

    print(f"\nSimulation Status")
    print(f"{'=' * 50}")
    print(f"  Current date : {meta['current_run_date']}")
    print(f"  Total runs   : {meta['total_runs']}")
    print(f"  Last run ID  : {meta['last_run_id']}")
    print(f"  DB path      : {args.db}")
    print()

    W = 36
    print(f"  {'Table':<{W}} {'Rows':>8}")
    print(f"  {'-'*W}  {'--------'}")
    for tbl in ["CLIENT", "PROVIDER", "PRODUCT_DEFINITION", "MERCHANT",
                "ACCOUNT", "CUSTOMER", "CARD", "AUTHORIZATION", "TRANSACTION",
                "STATEMENT", "DISPUTE", "CHARGEBACK", "FRAUD_ALERT",
                "SCORE_RECORD", "COLLECTION_CASE"]:
        n = store.count(tbl)
        print(f"  {tbl:<{W}} {n:>8,}")
    store.close()
    return 0


def _cmd_validate(args) -> int:
    from tdgen_temporal.validators import run_all, print_report
    from tdgen_temporal.validators.results import Severity, generate_html_report

    if not args.db.exists():
        print(f"Database not found: {args.db}")
        return 1

    print(f"\nValidating {args.db}...")
    checks, findings = run_all(args.db)

    if args.errors_only:
        checks   = [c for c in checks   if c.severity == Severity.ERROR]
        findings = [f for f in findings if f.severity == Severity.ERROR]

    print_report(checks, findings, verbose=args.verbose)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        generate_html_report(checks, findings, db_path=args.db, output_path=args.output)

    errors = sum(1 for f in findings if f.severity == Severity.ERROR)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
