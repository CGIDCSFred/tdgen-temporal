"""
StateStore — all reads and writes to state.db go through this class.
Single SQLite connection; WAL mode; all writes are parameterised.
"""

import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any


class StateStore:
    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=OFF")

    # ── Simulation meta ────────────────────────────────────────────────────

    def get_simulation_meta(self) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM simulation_meta WHERE id = 1"
        ).fetchone()
        return dict(row) if row else None

    def set_simulation_meta(self, run_date: date, run_id: str, total_runs: int) -> None:
        now = datetime.now().isoformat()
        self._conn.execute(
            """
            INSERT INTO simulation_meta (id, current_run_date, last_run_id, total_runs, created_at, updated_at)
            VALUES (1, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                current_run_date = excluded.current_run_date,
                last_run_id      = excluded.last_run_id,
                total_runs       = excluded.total_runs,
                updated_at       = excluded.updated_at
            """,
            (run_date.isoformat(), run_id, total_runs, now, now),
        )
        self._conn.commit()

    def get_current_run_date(self) -> date | None:
        meta = self.get_simulation_meta()
        if meta is None:
            return None
        return date.fromisoformat(meta["current_run_date"])

    def get_total_runs(self) -> int:
        meta = self.get_simulation_meta()
        return meta["total_runs"] if meta else 0

    # ── PK sequences ───────────────────────────────────────────────────────

    def next_id(self, table_name: str) -> int:
        """Return next PK for table_name and increment the counter."""
        row = self._conn.execute(
            "SELECT next_id FROM pk_sequences WHERE table_name = ?", (table_name,)
        ).fetchone()
        if row is None:
            # Initialise from current max or 1
            try:
                max_row = self._conn.execute(
                    f'SELECT MAX(rowid) FROM "{table_name}"'
                ).fetchone()
                next_id = (max_row[0] or 0) + 1
            except Exception:
                next_id = 1
            self._conn.execute(
                "INSERT INTO pk_sequences (table_name, next_id) VALUES (?, ?)",
                (table_name, next_id + 1),
            )
            self._conn.commit()
            return next_id
        nid = row[0]
        self._conn.execute(
            "UPDATE pk_sequences SET next_id = next_id + 1 WHERE table_name = ?",
            (table_name,),
        )
        return nid

    def init_sequence(self, table_name: str, start: int = 1) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO pk_sequences (table_name, next_id) VALUES (?, ?)",
            (table_name, start),
        )

    # ── Generic read/write ─────────────────────────────────────────────────

    def fetch_all(self, table_name: str, where: str = "", params: tuple = ()) -> list[dict]:
        sql = f'SELECT * FROM "{table_name}"'
        if where:
            sql += f" WHERE {where}"
        rows = self._conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def fetch_one(self, table_name: str, where: str, params: tuple = ()) -> dict | None:
        sql = f'SELECT * FROM "{table_name}" WHERE {where}'
        row = self._conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def count(self, table_name: str) -> int:
        row = self._conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
        return row[0]

    def bulk_insert(self, table_name: str, rows: list[dict]) -> int:
        if not rows:
            return 0
        cols = list(rows[0].keys())
        placeholders = ", ".join("?" for _ in cols)
        col_list = ", ".join(f'"{c}"' for c in cols)
        sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})'
        values = [tuple(r[c] for c in cols) for r in rows]
        self._conn.executemany(sql, values)
        self._conn.commit()
        return len(rows)

    def bulk_upsert(self, table_name: str, rows: list[dict]) -> int:
        if not rows:
            return 0
        cols = list(rows[0].keys())
        placeholders = ", ".join("?" for _ in cols)
        col_list = ", ".join(f'"{c}"' for c in cols)
        sql = f'INSERT OR REPLACE INTO "{table_name}" ({col_list}) VALUES ({placeholders})'
        values = [tuple(r[c] for c in cols) for r in rows]
        self._conn.executemany(sql, values)
        self._conn.commit()
        return len(rows)

    def update_row(self, table_name: str, pk_col: str, pk_val: Any, updates: dict) -> None:
        set_clause = ", ".join(f'"{k}" = ?' for k in updates)
        sql = f'UPDATE "{table_name}" SET {set_clause} WHERE "{pk_col}" = ?'
        self._conn.execute(sql, (*updates.values(), pk_val))
        self._conn.commit()

    # ── Domain-specific queries ────────────────────────────────────────────

    def get_all_accounts(self) -> list[dict]:
        rows = self._conn.execute(
            """
            SELECT a.*, t.current_state, t.days_delinquent,
                   t.consecutive_missed_payments, t.last_payment_date,
                   t.last_statement_date, t.payment_due_date, t.cycle_day
            FROM ACCOUNT a
            LEFT JOIN account_temporal_state t ON a.account_id = t.account_id
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def get_active_accounts(self) -> list[dict]:
        rows = self._conn.execute(
            """
            SELECT a.*, t.current_state, t.days_delinquent,
                   t.consecutive_missed_payments, t.last_payment_date,
                   t.last_statement_date, t.payment_due_date, t.cycle_day
            FROM ACCOUNT a
            LEFT JOIN account_temporal_state t ON a.account_id = t.account_id
            WHERE a.account_status != 'CLOSED'
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def get_accounts_needing_statement(self, run_date: date) -> list[dict]:
        rows = self._conn.execute(
            """
            SELECT a.*, t.current_state, t.days_delinquent,
                   t.cycle_day, t.last_statement_date
            FROM ACCOUNT a
            JOIN account_temporal_state t ON a.account_id = t.account_id
            WHERE t.cycle_day = ?
              AND a.account_status != 'CLOSED'
            """,
            (run_date.day,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_active_cards_for_account(self, account_id: int) -> list[dict]:
        rows = self._conn.execute(
            "SELECT * FROM CARD WHERE account_id = ? AND card_status != 'CANCELLED'",
            (account_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_active_disputes(self) -> list[dict]:
        rows = self._conn.execute(
            """
            SELECT d.*, t.current_state AS temporal_state, t.days_open
            FROM DISPUTE d
            JOIN dispute_temporal_state t ON d.dispute_id = t.dispute_id
            WHERE t.current_state NOT IN ('CLOSED', 'RESOLVED', 'WITHDRAWN')
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def get_active_fraud_alerts(self) -> list[dict]:
        rows = self._conn.execute(
            """
            SELECT f.*, t.current_state AS temporal_state, t.days_open
            FROM FRAUD_ALERT f
            JOIN fraud_alert_temporal_state t ON f.alert_id = t.alert_id
            WHERE t.current_state NOT IN ('CLOSED', 'FALSE_POSITIVE', 'CONFIRMED')
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def get_active_chargebacks(self) -> list[dict]:
        rows = self._conn.execute(
            """
            SELECT c.*, t.current_state AS temporal_state, t.days_open
            FROM CHARGEBACK c
            JOIN chargeback_temporal_state t ON c.chargeback_id = t.chargeback_id
            WHERE t.current_state NOT IN ('WON', 'LOST')
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def get_active_collection_cases(self) -> list[dict]:
        rows = self._conn.execute(
            """
            SELECT c.*, t.current_state AS temporal_state, t.current_bucket, t.days_in_bucket
            FROM COLLECTION_CASE c
            JOIN collection_case_temporal_state t ON c.case_id = t.case_id
            WHERE t.current_state NOT IN ('CLOSED', 'CHARGEOFF')
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def get_merchants(self) -> list[dict]:
        return self.fetch_all("MERCHANT")

    def get_products(self) -> list[dict]:
        return self.fetch_all("PRODUCT_DEFINITION")

    def get_providers(self) -> list[dict]:
        return self.fetch_all("PROVIDER")

    def get_cards_expiring_within(self, run_date: date, days: int = 30) -> list[dict]:
        from datetime import timedelta
        cutoff = (run_date + timedelta(days=days)).isoformat()[:7]  # YYYY-MM
        rows = self._conn.execute(
            "SELECT * FROM CARD WHERE expiry_date <= ? AND card_status = 'ACTIVE'",
            (cutoff,),
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Run log ────────────────────────────────────────────────────────────

    def record_run(self, run_id: str, run_date: date, run_mode: str,
                   accounts_processed: int, inserts: dict, updates: dict,
                   duration: float) -> None:
        self._conn.execute(
            """
            INSERT INTO run_log
                (run_id, run_date, run_mode, accounts_processed,
                 inserts_json, updates_json, duration_seconds, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, run_date.isoformat(), run_mode, accounts_processed,
                json.dumps(inserts), json.dumps(updates),
                duration, datetime.now().isoformat(),
            ),
        )
        self._conn.commit()

    # ── Snapshot ───────────────────────────────────────────────────────────

    def snapshot_table(self, table_name: str) -> list[dict]:
        return self.fetch_all(table_name)

    # ── Transaction wrapper ────────────────────────────────────────────────

    def begin(self) -> None:
        self._conn.execute("BEGIN")

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()
