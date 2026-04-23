"""
Temporal integrity checks.
"""

import sqlite3

from tdgen_temporal.validators.results import Check, Finding, Severity

_E = Severity.ERROR
_W = Severity.WARNING

CHECKS: list[Check] = [
    Check(
        "account.open_date > closed_date",
        "temporal",
        _E,
        "ACCOUNT",
        "Account closed_date must not precede open_date",
    ),
    Check(
        "account.last_payment_date < open_date",
        "temporal",
        _E,
        "ACCOUNT",
        "Account last_payment_date must not precede open_date",
    ),
    Check(
        "card.issue_date >= expiry_date",
        "temporal",
        _E,
        "CARD",
        "Card expiry_date must be after issue_date",
    ),
    Check(
        "transaction.post_date < transaction_date",
        "temporal",
        _E,
        "TRANSACTION",
        "Transaction post_date must not precede transaction_date",
    ),
    Check(
        "transaction.post_date > transaction_date + 3 days",
        "temporal",
        _W,
        "TRANSACTION",
        "Transaction post_date should be within 3 days of transaction_date",
    ),
    Check(
        "authorization.auth_timestamp > transaction.transaction_date",
        "temporal",
        _E,
        "TRANSACTION",
        "Authorization timestamp must not be later than its transaction date",
    ),
    Check(
        "transaction after account closed_date",
        "temporal",
        _E,
        "TRANSACTION",
        "No transactions should exist after an account's closed_date",
    ),
    Check(
        "transaction before account open_date",
        "temporal",
        _E,
        "TRANSACTION",
        "No transactions should exist before an account's open_date",
    ),
    Check(
        "dispute.opened_date < transaction.transaction_date",
        "temporal",
        _E,
        "DISPUTE",
        "Dispute must be opened on or after its underlying transaction date",
    ),
    Check(
        "dispute.resolved_date < dispute_opened_date",
        "temporal",
        _E,
        "DISPUTE",
        "Dispute resolved_date must not precede dispute_opened_date",
    ),
    Check(
        "chargeback.chargeback_date < dispute.dispute_opened_date",
        "temporal",
        _E,
        "CHARGEBACK",
        "Chargeback date must not precede the parent dispute's opened date",
    ),
    Check(
        "chargeback.chargeback_date < dispute.resolved_date",
        "temporal",
        _W,
        "CHARGEBACK",
        "Chargeback date should not precede the dispute's resolved date",
    ),
    Check(
        "statement.payment_due_date <= statement_date",
        "temporal",
        _E,
        "STATEMENT",
        "Statement payment_due_date must be after the statement_date",
    ),
    Check(
        "score_record.score_date < account.open_date",
        "temporal",
        _E,
        "SCORE_RECORD",
        "Score record score_date must not precede the account's open_date",
    ),
]

_SQLS: dict[str, str] = {
    "account.open_date > closed_date": """
        SELECT account_id FROM ACCOUNT
        WHERE closed_date IS NOT NULL AND open_date IS NOT NULL
          AND open_date > closed_date""",
    "account.last_payment_date < open_date": """
        SELECT account_id FROM ACCOUNT
        WHERE last_payment_date IS NOT NULL AND open_date IS NOT NULL
          AND last_payment_date < open_date""",
    "card.issue_date >= expiry_date": """
        SELECT card_id FROM CARD
        WHERE issue_date IS NOT NULL AND expiry_date IS NOT NULL
          AND issue_date >= expiry_date""",
    "transaction.post_date < transaction_date": """
        SELECT transaction_id FROM "TRANSACTION"
        WHERE post_date IS NOT NULL AND transaction_date IS NOT NULL
          AND DATE(post_date) < DATE(transaction_date)""",
    "transaction.post_date > transaction_date + 3 days": """
        SELECT transaction_id FROM "TRANSACTION"
        WHERE post_date IS NOT NULL AND transaction_date IS NOT NULL
          AND JULIANDAY(post_date) - JULIANDAY(transaction_date) > 3""",
    "authorization.auth_timestamp > transaction.transaction_date": """
        SELECT t.transaction_id FROM "TRANSACTION" t
        JOIN AUTHORIZATION a ON t.auth_id = a.auth_id
        WHERE DATE(a.auth_timestamp) > DATE(t.transaction_date)""",
    "transaction after account closed_date": """
        SELECT t.transaction_id FROM "TRANSACTION" t
        JOIN ACCOUNT a ON t.account_id = a.account_id
        WHERE a.account_status = 'CLOSED' AND a.closed_date IS NOT NULL
          AND DATE(t.transaction_date) > DATE(a.closed_date)""",
    "transaction before account open_date": """
        SELECT t.transaction_id FROM "TRANSACTION" t
        JOIN ACCOUNT a ON t.account_id = a.account_id
        WHERE a.open_date IS NOT NULL
          AND DATE(t.transaction_date) < DATE(a.open_date)""",
    "dispute.opened_date < transaction.transaction_date": """
        SELECT d.dispute_id FROM DISPUTE d
        JOIN "TRANSACTION" t ON d.transaction_id = t.transaction_id
        WHERE DATE(d.dispute_opened_date) < DATE(t.transaction_date)""",
    "dispute.resolved_date < dispute_opened_date": """
        SELECT dispute_id FROM DISPUTE
        WHERE resolved_date IS NOT NULL AND dispute_opened_date IS NOT NULL
          AND DATE(resolved_date) < DATE(dispute_opened_date)""",
    "chargeback.chargeback_date < dispute.dispute_opened_date": """
        SELECT c.chargeback_id FROM CHARGEBACK c
        JOIN DISPUTE d ON c.dispute_id = d.dispute_id
        WHERE d.dispute_opened_date IS NOT NULL AND c.chargeback_date IS NOT NULL
          AND DATE(c.chargeback_date) < DATE(d.dispute_opened_date)""",
    "chargeback.chargeback_date < dispute.resolved_date": """
        SELECT c.chargeback_id FROM CHARGEBACK c
        JOIN DISPUTE d ON c.dispute_id = d.dispute_id
        WHERE d.resolved_date IS NOT NULL AND c.chargeback_date IS NOT NULL
          AND DATE(c.chargeback_date) < DATE(d.resolved_date)""",
    "statement.payment_due_date <= statement_date": """
        SELECT statement_id FROM STATEMENT
        WHERE payment_due_date IS NOT NULL AND statement_date IS NOT NULL
          AND DATE(payment_due_date) <= DATE(statement_date)""",
    "score_record.score_date < account.open_date": """
        SELECT s.score_id FROM SCORE_RECORD s
        JOIN ACCOUNT a ON s.account_id = a.account_id
        WHERE a.open_date IS NOT NULL
          AND DATE(s.score_date) < DATE(a.open_date)""",
}


def run(conn: sqlite3.Connection) -> list[Finding]:
    findings: list[Finding] = []
    check_map = {c.name: c for c in CHECKS}
    for name, sql in _SQLS.items():
        rows = conn.execute(sql).fetchall()
        if rows:
            findings.append(
                Finding(
                    check=check_map[name],
                    count=len(rows),
                    examples=[r[0] for r in rows[:5]],
                )
            )
    return findings
