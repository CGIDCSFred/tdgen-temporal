"""
Referential integrity checks.
"""

import sqlite3

from tdgen_temporal.validators.results import Check, Finding, Severity

_E = Severity.ERROR

CHECKS: list[Check] = [
    Check(
        "customer.account_id -> ACCOUNT",
        "referential",
        _E,
        "CUSTOMER",
        "Every CUSTOMER row references a valid ACCOUNT",
    ),
    Check(
        "card.account_id -> ACCOUNT",
        "referential",
        _E,
        "CARD",
        "Every CARD row references a valid ACCOUNT",
    ),
    Check(
        "account.provider_id -> PROVIDER",
        "referential",
        _E,
        "ACCOUNT",
        "Every ACCOUNT references a valid PROVIDER",
    ),
    Check(
        "account.product -> PRODUCT_DEFINITION",
        "referential",
        _E,
        "ACCOUNT",
        "Every ACCOUNT references a valid PRODUCT_DEFINITION",
    ),
    Check(
        "provider.client_id -> CLIENT",
        "referential",
        _E,
        "PROVIDER",
        "Every PROVIDER references a valid CLIENT",
    ),
    Check(
        "transaction.account_id -> ACCOUNT",
        "referential",
        _E,
        "TRANSACTION",
        "Every TRANSACTION references a valid ACCOUNT",
    ),
    Check(
        "transaction.card_id -> CARD",
        "referential",
        _E,
        "TRANSACTION",
        "Every non-null TRANSACTION.card_id references a valid CARD",
    ),
    Check(
        "transaction.merchant_id -> MERCHANT",
        "referential",
        _E,
        "TRANSACTION",
        "Every non-null TRANSACTION.merchant_id references a valid MERCHANT",
    ),
    Check(
        "transaction.auth_id -> AUTHORIZATION",
        "referential",
        _E,
        "TRANSACTION",
        "Every non-null TRANSACTION.auth_id references a valid AUTHORIZATION",
    ),
    Check(
        "authorization.account_id -> ACCOUNT",
        "referential",
        _E,
        "AUTHORIZATION",
        "Every AUTHORIZATION references a valid ACCOUNT",
    ),
    Check(
        "statement.account_id -> ACCOUNT",
        "referential",
        _E,
        "STATEMENT",
        "Every STATEMENT references a valid ACCOUNT",
    ),
    Check(
        "dispute.transaction_id -> TRANSACTION",
        "referential",
        _E,
        "DISPUTE",
        "Every DISPUTE references a valid TRANSACTION",
    ),
    Check(
        "dispute.account_id -> ACCOUNT",
        "referential",
        _E,
        "DISPUTE",
        "Every DISPUTE references a valid ACCOUNT",
    ),
    Check(
        "chargeback.dispute_id -> DISPUTE",
        "referential",
        _E,
        "CHARGEBACK",
        "Every CHARGEBACK references a valid DISPUTE",
    ),
    Check(
        "chargeback.transaction_id -> TRANSACTION",
        "referential",
        _E,
        "CHARGEBACK",
        "Every CHARGEBACK references a valid TRANSACTION",
    ),
    Check(
        "fraud_alert.account_id -> ACCOUNT",
        "referential",
        _E,
        "FRAUD_ALERT",
        "Every FRAUD_ALERT references a valid ACCOUNT",
    ),
    Check(
        "fraud_alert.transaction_id -> TRANSACTION",
        "referential",
        _E,
        "FRAUD_ALERT",
        "Every non-null FRAUD_ALERT.transaction_id references a valid TRANSACTION",
    ),
    Check(
        "score_record.account_id -> ACCOUNT",
        "referential",
        _E,
        "SCORE_RECORD",
        "Every SCORE_RECORD references a valid ACCOUNT",
    ),
    Check(
        "collection_case.account_id -> ACCOUNT",
        "referential",
        _E,
        "COLLECTION_CASE",
        "Every COLLECTION_CASE references a valid ACCOUNT",
    ),
    Check(
        "account_temporal_state.account_id -> ACCOUNT",
        "referential",
        _E,
        "account_temporal_state",
        "Every account_temporal_state row references a valid ACCOUNT",
    ),
    Check(
        "dispute_temporal_state.dispute_id -> DISPUTE",
        "referential",
        _E,
        "dispute_temporal_state",
        "Every dispute_temporal_state row references a valid DISPUTE",
    ),
    Check(
        "chargeback_temporal_state.chargeback_id -> CHARGEBACK",
        "referential",
        _E,
        "chargeback_temporal_state",
        "Every chargeback_temporal_state row references a valid CHARGEBACK",
    ),
    Check(
        "account has no customer",
        "referential",
        _E,
        "ACCOUNT",
        "Every ACCOUNT has at least one matching CUSTOMER row",
    ),
    Check(
        "account has no temporal state",
        "referential",
        _E,
        "ACCOUNT",
        "Every ACCOUNT has a matching account_temporal_state row",
    ),
]

_SQLS: dict[str, str] = {
    "customer.account_id -> ACCOUNT": """
        SELECT c.customer_id FROM CUSTOMER c
        LEFT JOIN ACCOUNT a ON c.account_id = a.account_id
        WHERE a.account_id IS NULL""",
    "card.account_id -> ACCOUNT": """
        SELECT c.card_id FROM CARD c
        LEFT JOIN ACCOUNT a ON c.account_id = a.account_id
        WHERE a.account_id IS NULL""",
    "account.provider_id -> PROVIDER": """
        SELECT a.account_id FROM ACCOUNT a
        LEFT JOIN PROVIDER p ON a.provider_id = p.provider_id
        WHERE p.provider_id IS NULL""",
    "account.product -> PRODUCT_DEFINITION": """
        SELECT a.account_id FROM ACCOUNT a
        LEFT JOIN PRODUCT_DEFINITION p
            ON a.tsys_product_code = p.tsys_product_code
           AND a.client_product_code = p.client_product_code
        WHERE p.tsys_product_code IS NULL""",
    "provider.client_id -> CLIENT": """
        SELECT p.provider_id FROM PROVIDER p
        LEFT JOIN CLIENT c ON p.client_id = c.client_id
        WHERE c.client_id IS NULL""",
    "transaction.account_id -> ACCOUNT": """
        SELECT t.transaction_id FROM "TRANSACTION" t
        LEFT JOIN ACCOUNT a ON t.account_id = a.account_id
        WHERE a.account_id IS NULL""",
    "transaction.card_id -> CARD": """
        SELECT t.transaction_id FROM "TRANSACTION" t
        LEFT JOIN CARD c ON t.card_id = c.card_id
        WHERE t.card_id IS NOT NULL AND c.card_id IS NULL""",
    "transaction.merchant_id -> MERCHANT": """
        SELECT t.transaction_id FROM "TRANSACTION" t
        LEFT JOIN MERCHANT m ON t.merchant_id = m.merchant_id
        WHERE t.merchant_id IS NOT NULL AND m.merchant_id IS NULL""",
    "transaction.auth_id -> AUTHORIZATION": """
        SELECT t.transaction_id FROM "TRANSACTION" t
        LEFT JOIN AUTHORIZATION a ON t.auth_id = a.auth_id
        WHERE t.auth_id IS NOT NULL AND a.auth_id IS NULL""",
    "authorization.account_id -> ACCOUNT": """
        SELECT a.auth_id FROM AUTHORIZATION a
        LEFT JOIN ACCOUNT acc ON a.account_id = acc.account_id
        WHERE acc.account_id IS NULL""",
    "statement.account_id -> ACCOUNT": """
        SELECT s.statement_id FROM STATEMENT s
        LEFT JOIN ACCOUNT a ON s.account_id = a.account_id
        WHERE a.account_id IS NULL""",
    "dispute.transaction_id -> TRANSACTION": """
        SELECT d.dispute_id FROM DISPUTE d
        LEFT JOIN "TRANSACTION" t ON d.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL""",
    "dispute.account_id -> ACCOUNT": """
        SELECT d.dispute_id FROM DISPUTE d
        LEFT JOIN ACCOUNT a ON d.account_id = a.account_id
        WHERE a.account_id IS NULL""",
    "chargeback.dispute_id -> DISPUTE": """
        SELECT c.chargeback_id FROM CHARGEBACK c
        LEFT JOIN DISPUTE d ON c.dispute_id = d.dispute_id
        WHERE d.dispute_id IS NULL""",
    "chargeback.transaction_id -> TRANSACTION": """
        SELECT c.chargeback_id FROM CHARGEBACK c
        LEFT JOIN "TRANSACTION" t ON c.transaction_id = t.transaction_id
        WHERE t.transaction_id IS NULL""",
    "fraud_alert.account_id -> ACCOUNT": """
        SELECT f.alert_id FROM FRAUD_ALERT f
        LEFT JOIN ACCOUNT a ON f.account_id = a.account_id
        WHERE a.account_id IS NULL""",
    "fraud_alert.transaction_id -> TRANSACTION": """
        SELECT f.alert_id FROM FRAUD_ALERT f
        LEFT JOIN "TRANSACTION" t ON f.transaction_id = t.transaction_id
        WHERE f.transaction_id IS NOT NULL AND t.transaction_id IS NULL""",
    "score_record.account_id -> ACCOUNT": """
        SELECT s.score_id FROM SCORE_RECORD s
        LEFT JOIN ACCOUNT a ON s.account_id = a.account_id
        WHERE a.account_id IS NULL""",
    "collection_case.account_id -> ACCOUNT": """
        SELECT c.case_id FROM COLLECTION_CASE c
        LEFT JOIN ACCOUNT a ON c.account_id = a.account_id
        WHERE a.account_id IS NULL""",
    "account_temporal_state.account_id -> ACCOUNT": """
        SELECT t.account_id FROM account_temporal_state t
        LEFT JOIN ACCOUNT a ON t.account_id = a.account_id
        WHERE a.account_id IS NULL""",
    "dispute_temporal_state.dispute_id -> DISPUTE": """
        SELECT t.dispute_id FROM dispute_temporal_state t
        LEFT JOIN DISPUTE d ON t.dispute_id = d.dispute_id
        WHERE d.dispute_id IS NULL""",
    "chargeback_temporal_state.chargeback_id -> CHARGEBACK": """
        SELECT t.chargeback_id FROM chargeback_temporal_state t
        LEFT JOIN CHARGEBACK c ON t.chargeback_id = c.chargeback_id
        WHERE c.chargeback_id IS NULL""",
    "account has no customer": """
        SELECT a.account_id FROM ACCOUNT a
        LEFT JOIN CUSTOMER c ON a.account_id = c.account_id
        WHERE c.account_id IS NULL""",
    "account has no temporal state": """
        SELECT a.account_id FROM ACCOUNT a
        LEFT JOIN account_temporal_state t ON a.account_id = t.account_id
        WHERE t.account_id IS NULL""",
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
