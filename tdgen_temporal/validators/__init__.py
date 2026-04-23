"""
Integrity validators for tdgen-temporal.

Usage:
    from tdgen_temporal.validators import run_all
    checks, findings = run_all(db_path)
"""

import sqlite3
from pathlib import Path

from tdgen_temporal.validators import referential, temporal, state
from tdgen_temporal.validators.results import Check, Finding, Severity, print_report

ALL_CHECKS: list[Check] = referential.CHECKS + temporal.CHECKS + state.CHECKS


def run_all(db_path: Path) -> tuple[list[Check], list[Finding]]:
    """
    Open a read-only connection to db_path and run all three check suites.
    Returns (all_checks, findings).
    """
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        findings: list[Finding] = []
        findings += referential.run(conn)
        findings += temporal.run(conn)
        findings += state.run(conn)
        return ALL_CHECKS, findings
    finally:
        conn.close()


__all__ = ["run_all", "ALL_CHECKS", "Check", "Finding", "Severity", "print_report"]
