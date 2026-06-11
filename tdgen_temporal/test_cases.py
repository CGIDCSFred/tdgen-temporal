"""
Test case mining — queries the simulation database for records that satisfy
named business scenarios, returning labelled bundles of related record IDs.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass
class TestCaseDefinition:
    key: str
    name: str
    category: str
    description: str
    sql: str


ALL_CASES: list[TestCaseDefinition] = [
    # ── Baseline ──────────────────────────────────────────────────────────────
    TestCaseDefinition(
        key="happy_path",
        name="Happy path — active account, clean history",
        category="Baseline",
        description=(
            "Active account with multiple transactions, no disputes, no fraud alerts, "
            "and no delinquency. Good for testing the normal purchase-and-payment flow."
        ),
        sql="""
            SELECT
                a.account_id,
                a.account_status,
                a.credit_limit,
                a.current_balance,
                a.available_credit,
                COUNT(t.transaction_id) AS transaction_count
            FROM ACCOUNT a
            JOIN "TRANSACTION" t ON t.account_id = a.account_id
            WHERE a.account_status = 'ACTIVE'
              AND a.days_delinquent = 0
              AND NOT EXISTS (
                  SELECT 1 FROM DISPUTE d WHERE d.account_id = a.account_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM FRAUD_ALERT f WHERE f.account_id = a.account_id
              )
            GROUP BY a.account_id
            HAVING transaction_count >= 5
            ORDER BY transaction_count DESC
            LIMIT 10
        """,
    ),
    # ── Lifecycle ─────────────────────────────────────────────────────────────
    TestCaseDefinition(
        key="delinquent_with_collection",
        name="Delinquency — active collection case",
        category="Lifecycle",
        description=(
            "Account that missed payments, entered delinquency, and has an open "
            "collection case. Tests the missed-payment → delinquency → collections path."
        ),
        sql="""
            SELECT
                a.account_id,
                a.account_status,
                a.days_delinquent,
                a.current_balance,
                cc.case_id,
                cc.delinquency_bucket,
                cc.case_status,
                cc.amount_past_due,
                cc.total_owed
            FROM ACCOUNT a
            JOIN COLLECTION_CASE cc ON cc.account_id = a.account_id
            WHERE a.account_status = 'DELINQUENT'
            ORDER BY a.days_delinquent DESC
            LIMIT 10
        """,
    ),
    TestCaseDefinition(
        key="chargeoff",
        name="Full lifecycle — charged-off account",
        category="Lifecycle",
        description=(
            "Account that progressed through ACTIVE → DELINQUENT → CHARGEOFF. "
            "Tests the full bad-debt lifecycle including card block side effects."
        ),
        sql="""
            SELECT
                a.account_id,
                a.account_status,
                a.days_delinquent,
                a.credit_limit,
                a.current_balance,
                card.card_id,
                card.card_status,
                cc.case_id,
                cc.case_status,
                cc.chargeoff_date
            FROM ACCOUNT a
            LEFT JOIN CARD card ON card.account_id = a.account_id
            LEFT JOIN COLLECTION_CASE cc ON cc.account_id = a.account_id
            WHERE a.account_status = 'CHARGEOFF'
            ORDER BY a.days_delinquent DESC
            LIMIT 10
        """,
    ),
    TestCaseDefinition(
        key="card_expired_replaced",
        name="Card lifecycle — expired and replaced",
        category="Lifecycle",
        description=(
            "Account where the original card expired and a replacement was issued. "
            "Tests card expiry detection and reissuance."
        ),
        sql="""
            SELECT
                c_old.account_id,
                c_old.card_id       AS expired_card_id,
                c_old.card_status   AS expired_status,
                c_old.expiry_date,
                c_new.card_id       AS replacement_card_id,
                c_new.card_status   AS replacement_status,
                c_new.issue_date    AS replacement_issued
            FROM CARD c_old
            JOIN CARD c_new
              ON  c_new.account_id = c_old.account_id
              AND c_new.card_id    > c_old.card_id
            WHERE c_old.card_status = 'EXPIRED'
              AND c_new.card_status = 'ACTIVE'
            LIMIT 10
        """,
    ),
    TestCaseDefinition(
        key="score_history",
        name="Credit scoring — account with score movement",
        category="Lifecycle",
        description=(
            "Account with multiple credit score records showing score drift over time. "
            "Tests the monthly score refresh and band assignment logic."
        ),
        sql="""
            SELECT
                s.account_id,
                COUNT(*)                        AS score_count,
                MIN(s.score_value)              AS score_min,
                MAX(s.score_value)              AS score_max,
                MAX(s.score_value)
                  - MIN(s.score_value)          AS score_range,
                MIN(s.score_date)               AS first_score_date,
                MAX(s.score_date)               AS latest_score_date,
                MAX(s.score_band)               AS latest_band
            FROM SCORE_RECORD s
            GROUP BY s.account_id
            HAVING score_count >= 2
            ORDER BY score_range DESC
            LIMIT 10
        """,
    ),
    # ── Dispute ───────────────────────────────────────────────────────────────
    TestCaseDefinition(
        key="dispute_escalated_chargeback",
        name="Dispute — escalated to chargeback",
        category="Dispute",
        description=(
            "Transaction that was disputed and the dispute resolution triggered a "
            "network chargeback. Tests the full dispute → chargeback escalation path."
        ),
        sql="""
            SELECT
                d.dispute_id,
                d.account_id,
                d.transaction_id,
                d.dispute_type,
                d.dispute_status,
                d.disputed_amount,
                d.resolution,
                c.chargeback_id,
                c.chargeback_stage,
                c.chargeback_amount,
                c.representment_status
            FROM DISPUTE d
            JOIN CHARGEBACK c ON c.dispute_id = d.dispute_id
            ORDER BY d.disputed_amount DESC
            LIMIT 10
        """,
    ),
    TestCaseDefinition(
        key="dispute_approved",
        name="Dispute — resolved in cardholder's favour",
        category="Dispute",
        description=(
            "Dispute where the resolution was APPROVED (found in favour of the cardholder). "
            "Tests the investigation → approved resolution path."
        ),
        sql="""
            SELECT
                d.dispute_id,
                d.account_id,
                d.transaction_id,
                d.dispute_type,
                d.disputed_amount,
                d.dispute_opened_date,
                d.resolved_date,
                d.resolution
            FROM DISPUTE d
            WHERE d.resolution = 'APPROVED'
            ORDER BY d.disputed_amount DESC
            LIMIT 10
        """,
    ),
    TestCaseDefinition(
        key="dispute_denied",
        name="Dispute — resolved in merchant's favour",
        category="Dispute",
        description=(
            "Dispute where the resolution was DENIED (found in favour of the merchant). "
            "Tests the investigation → denied resolution path."
        ),
        sql="""
            SELECT
                d.dispute_id,
                d.account_id,
                d.transaction_id,
                d.dispute_type,
                d.disputed_amount,
                d.dispute_opened_date,
                d.resolved_date,
                d.resolution
            FROM DISPUTE d
            WHERE d.resolution = 'DENIED'
            ORDER BY d.disputed_amount DESC
            LIMIT 10
        """,
    ),
    TestCaseDefinition(
        key="dispute_withdrawn",
        name="Dispute — withdrawn by cardholder",
        category="Dispute",
        description=(
            "Dispute that was withdrawn by the cardholder within the early withdrawal "
            "window. Tests the early-exit path of the dispute lifecycle."
        ),
        sql="""
            SELECT
                d.dispute_id,
                d.account_id,
                d.transaction_id,
                d.dispute_type,
                d.disputed_amount,
                d.dispute_opened_date,
                d.dispute_status
            FROM DISPUTE d
            WHERE d.dispute_status = 'WITHDRAWN'
            ORDER BY d.dispute_opened_date DESC
            LIMIT 10
        """,
    ),
    # ── Fraud ─────────────────────────────────────────────────────────────────
    TestCaseDefinition(
        key="fraud_confirmed_card_blocked",
        name="Fraud — confirmed alert, card blocked",
        category="Fraud",
        description=(
            "Fraud alert that was confirmed after review, triggering a card block "
            "side effect. Tests the OPEN → UNDER_REVIEW → CONFIRMED → card blocked path."
        ),
        sql="""
            SELECT
                f.alert_id,
                f.account_id,
                f.transaction_id,
                f.alert_type,
                f.alert_status,
                f.action_taken,
                card.card_id,
                card.card_status
            FROM FRAUD_ALERT f
            JOIN CARD card ON card.account_id = f.account_id
            WHERE f.alert_status = 'CONFIRMED'
              AND card.card_status = 'BLOCKED'
            LIMIT 10
        """,
    ),
    TestCaseDefinition(
        key="fraud_false_positive",
        name="Fraud — false positive (cleared)",
        category="Fraud",
        description=(
            "Fraud alert that was reviewed and determined to be a false positive. "
            "Tests the OPEN → UNDER_REVIEW → FALSE_POSITIVE path."
        ),
        sql="""
            SELECT
                f.alert_id,
                f.account_id,
                f.transaction_id,
                f.alert_type,
                f.alert_status,
                f.action_taken
            FROM FRAUD_ALERT f
            WHERE f.alert_status = 'FALSE_POSITIVE'
            LIMIT 10
        """,
    ),
    TestCaseDefinition(
        key="fraud_and_dispute_same_txn",
        name="Fraud + dispute — same transaction",
        category="Fraud",
        description=(
            "Transaction that generated both a fraud alert AND a dispute. Tests "
            "concurrent fraud and dispute processing on the same underlying transaction."
        ),
        sql="""
            SELECT
                t.transaction_id,
                t.account_id,
                t.transaction_amount,
                t.transaction_date,
                f.alert_id,
                f.alert_type,
                f.alert_status,
                d.dispute_id,
                d.dispute_type,
                d.dispute_status,
                d.disputed_amount
            FROM "TRANSACTION" t
            JOIN FRAUD_ALERT f ON f.transaction_id = t.transaction_id
            JOIN DISPUTE     d ON d.transaction_id = t.transaction_id
            ORDER BY t.transaction_amount DESC
            LIMIT 10
        """,
    ),
]


def run_cases(db_path: Path) -> dict[str, pd.DataFrame]:
    """
    Run all test case queries against the database.
    Returns a dict of {case_key: DataFrame}.
    Empty DataFrame means no matching records (not enough simulation history).
    """
    conn = sqlite3.connect(str(db_path))
    results: dict[str, pd.DataFrame] = {}
    for case in ALL_CASES:
        try:
            results[case.key] = pd.read_sql_query(case.sql, conn)
        except Exception:
            results[case.key] = pd.DataFrame()
    conn.close()
    return results
