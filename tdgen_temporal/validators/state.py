"""
State machine consistency checks.
"""

import sqlite3

from tdgen_temporal.validators.results import Check, Finding, Severity

_E = Severity.ERROR
_W = Severity.WARNING

_VALID_ACCOUNT_STATES = {"ACTIVE", "DELINQUENT", "SUSPENDED", "CHARGEOFF", "CLOSED"}
_VALID_CARD_STATES = {"ACTIVE", "BLOCKED", "EXPIRED", "CANCELLED", "LOST", "STOLEN"}
_VALID_DISPUTE_STATES = {"OPEN", "INVESTIGATING", "RESOLVED", "CLOSED", "WITHDRAWN"}
_VALID_ALERT_STATES = {"OPEN", "UNDER_REVIEW", "CONFIRMED", "FALSE_POSITIVE", "CLOSED"}
_VALID_CB_STAGES = {
    "FIRST_CHARGEBACK",
    "REPRESENTMENT",
    "PRE_ARBITRATION",
    "ARBITRATION",
    "WON",
    "LOST",
}
_VALID_CASE_STATES = {"ACTIVE", "AGENCY_REFERRAL", "PROMISE_TO_PAY", "CHARGEOFF", "CLOSED"}

CHECKS: list[Check] = [
    # Status/state sync
    Check(
        "account_status != temporal_state.current_state",
        "state",
        _E,
        "ACCOUNT",
        "ACCOUNT.account_status must match account_temporal_state.current_state",
    ),
    Check(
        "dispute_status != temporal_state.current_state",
        "state",
        _E,
        "DISPUTE",
        "DISPUTE.dispute_status must match dispute_temporal_state.current_state",
    ),
    Check(
        "alert_status != temporal_state.current_state",
        "state",
        _E,
        "FRAUD_ALERT",
        "FRAUD_ALERT.alert_status must match fraud_alert_temporal_state.current_state",
    ),
    # Invalid status values
    Check(
        "account.account_status invalid value",
        "state",
        _E,
        "ACCOUNT",
        f"account_status must be one of {sorted(_VALID_ACCOUNT_STATES)}",
    ),
    Check(
        "card.card_status invalid value",
        "state",
        _E,
        "CARD",
        f"card_status must be one of {sorted(_VALID_CARD_STATES)}",
    ),
    Check(
        "dispute.dispute_status invalid value",
        "state",
        _E,
        "DISPUTE",
        f"dispute_status must be one of {sorted(_VALID_DISPUTE_STATES)}",
    ),
    Check(
        "chargeback.chargeback_stage invalid value",
        "state",
        _E,
        "CHARGEBACK",
        f"chargeback_stage must be one of {sorted(_VALID_CB_STAGES)}",
    ),
    # Expected side effects
    Check(
        "chargeoff account has usable card",
        "state",
        _W,
        "CARD",
        "CHARGEOFF accounts must not have ACTIVE cards",
    ),
    Check(
        "closed account missing closed_date",
        "state",
        _E,
        "ACCOUNT",
        "CLOSED accounts must have closed_date set",
    ),
    Check(
        "delinquent account has days_delinquent <= 0",
        "state",
        _W,
        "ACCOUNT",
        "DELINQUENT accounts must have days_delinquent > 0",
    ),
    Check(
        "chargeoff account has days_delinquent < 180",
        "state",
        _W,
        "ACCOUNT",
        "CHARGEOFF accounts should have days_delinquent >= 180",
    ),
    Check(
        "chargeback parent dispute not resolved",
        "state",
        _W,
        "CHARGEBACK",
        "Chargebacks should only exist for RESOLVED or CLOSED disputes",
    ),
    Check(
        "open collection case on non-delinquent account",
        "state",
        _W,
        "COLLECTION_CASE",
        "Open COLLECTION_CASEs should be linked to DELINQUENT or CHARGEOFF accounts",
    ),
    # Balance plausibility
    Check(
        "available_credit inconsistent with balance",
        "state",
        _W,
        "ACCOUNT",
        "available_credit should equal credit_limit minus current_balance (within $1)",
    ),
    Check(
        "current_balance > 110% of credit_limit",
        "state",
        _W,
        "ACCOUNT",
        "current_balance should not exceed credit_limit by more than 10%",
    ),
    Check(
        "active account has negative balance",
        "state",
        _W,
        "ACCOUNT",
        "ACTIVE accounts should not have a current_balance below -$1",
    ),
]

_SQLS: dict[str, str] = {
    "account_status != temporal_state.current_state": """
        SELECT a.account_id FROM ACCOUNT a
        JOIN account_temporal_state t ON a.account_id = t.account_id
        WHERE a.account_status != t.current_state""",
    "dispute_status != temporal_state.current_state": """
        SELECT d.dispute_id FROM DISPUTE d
        JOIN dispute_temporal_state t ON d.dispute_id = t.dispute_id
        WHERE d.dispute_status != t.current_state""",
    "alert_status != temporal_state.current_state": """
        SELECT f.alert_id FROM FRAUD_ALERT f
        JOIN fraud_alert_temporal_state t ON f.alert_id = t.alert_id
        WHERE f.alert_status != t.current_state""",
    "account.account_status invalid value": f"SELECT account_id FROM ACCOUNT WHERE account_status NOT IN ({','.join(repr(s) for s in _VALID_ACCOUNT_STATES)})",
    "card.card_status invalid value": f"SELECT card_id FROM CARD WHERE card_status NOT IN ({','.join(repr(s) for s in _VALID_CARD_STATES)})",
    "dispute.dispute_status invalid value": f"SELECT dispute_id FROM DISPUTE WHERE dispute_status NOT IN ({','.join(repr(s) for s in _VALID_DISPUTE_STATES)})",
    "chargeback.chargeback_stage invalid value": f"SELECT chargeback_id FROM CHARGEBACK WHERE chargeback_stage NOT IN ({','.join(repr(s) for s in _VALID_CB_STAGES)})",
    "chargeoff account has usable card": """
        SELECT c.card_id FROM CARD c
        JOIN ACCOUNT a ON c.account_id = a.account_id
        WHERE a.account_status = 'CHARGEOFF'
          AND c.card_status NOT IN ('BLOCKED','CANCELLED','EXPIRED')""",
    "closed account missing closed_date": """
        SELECT account_id FROM ACCOUNT
        WHERE account_status = 'CLOSED' AND closed_date IS NULL""",
    "delinquent account has days_delinquent <= 0": """
        SELECT account_id FROM ACCOUNT
        WHERE account_status = 'DELINQUENT'
          AND (days_delinquent IS NULL OR days_delinquent <= 0)""",
    "chargeoff account has days_delinquent < 180": """
        SELECT account_id FROM ACCOUNT
        WHERE account_status = 'CHARGEOFF'
          AND days_delinquent IS NOT NULL AND days_delinquent < 180""",
    "chargeback parent dispute not resolved": """
        SELECT c.chargeback_id FROM CHARGEBACK c
        JOIN DISPUTE d ON c.dispute_id = d.dispute_id
        WHERE d.dispute_status NOT IN ('RESOLVED','CLOSED')""",
    "open collection case on non-delinquent account": """
        SELECT cc.case_id FROM COLLECTION_CASE cc
        JOIN ACCOUNT a ON cc.account_id = a.account_id
        WHERE a.account_status NOT IN ('DELINQUENT','CHARGEOFF')
          AND cc.case_status NOT IN ('CLOSED','CHARGEOFF')""",
    "available_credit inconsistent with balance": """
        SELECT account_id FROM ACCOUNT
        WHERE credit_limit IS NOT NULL AND current_balance IS NOT NULL
          AND available_credit IS NOT NULL
          AND ABS(available_credit - (credit_limit - current_balance)) > 1.0
          AND account_status NOT IN ('CLOSED','CHARGEOFF')""",
    "current_balance > 110% of credit_limit": """
        SELECT account_id FROM ACCOUNT
        WHERE current_balance IS NOT NULL AND credit_limit IS NOT NULL
          AND current_balance > credit_limit * 1.10
          AND account_status NOT IN ('CLOSED','CHARGEOFF')""",
    "active account has negative balance": """
        SELECT account_id FROM ACCOUNT
        WHERE current_balance < -1.0 AND account_status = 'ACTIVE'""",
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
