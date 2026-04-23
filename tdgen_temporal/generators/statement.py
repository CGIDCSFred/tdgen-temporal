"""
Statement generation — triggered when run_date.day == account.cycle_day.
"""

import random
from datetime import date, timedelta

from tdgen_temporal.db.state_store import StateStore


def generate_statements(
    accounts: list[dict],
    run_date: date,
    store: StateStore,
    rng: random.Random,
) -> list[dict]:
    """
    For each account whose cycle_day matches run_date.day, generate a STATEMENT.
    """
    statements = []

    for account in accounts:
        cycle_day = int(account.get("cycle_day", 0) or 0)
        if cycle_day != run_date.day:
            continue
        if account.get("account_status") == "CLOSED":
            continue

        balance = float(account.get("current_balance", 0) or 0)
        prev_bal = balance * rng.uniform(0.85, 1.05)  # approximate opening balance
        credits = round(rng.uniform(0, balance * 0.3), 2)
        debits = round(rng.uniform(balance * 0.1, balance * 0.5), 2)
        interest = round(balance * 0.015, 2) if balance > 0 else 0.0
        fees = rng.choice([0, 0, 0, 39.0, 79.0])
        min_pmt = round(max(10.0, balance * 0.02), 2)
        tx_count = rng.randint(1, 50)

        payment_due = (run_date + timedelta(days=21)).isoformat()

        statements.append(
            {
                "statement_id": store.next_id("STATEMENT"),
                "account_id": account["account_id"],
                "statement_date": run_date.isoformat(),
                "payment_due_date": payment_due,
                "opening_balance": round(prev_bal, 2),
                "closing_balance": round(balance, 2),
                "total_credits": credits,
                "total_debits": debits,
                "minimum_payment": min_pmt,
                "interest_charged": interest,
                "fees_charged": fees,
                "transaction_count": tx_count,
                "available_credit": round(
                    max(0.0, float(account.get("credit_limit", 0) or 0) - balance), 2
                ),
                "cycle_id": f"CYC-{run_date.strftime('%Y%m')}-{account['account_id']}",
            }
        )

    return statements
