"""
Daily transaction and authorization generation.
"""

import random
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING

from faker import Faker

from tdgen_temporal.generators.field_generators import (
    _CHANNELS,
    _MCC_CODES,
    _NETWORKS,
    _POS_ENTRY,
    random_narrative,
)

if TYPE_CHECKING:
    from tdgen_temporal.db.state_store import StateStore


def _rand_time(run_date: date, rng: random.Random) -> str:
    h = rng.randint(0, 23)
    m = rng.randint(0, 59)
    s = rng.randint(0, 59)
    return datetime(run_date.year, run_date.month, run_date.day, h, m, s).isoformat(sep=" ")


def generate_daily_transactions(
    accounts: list[dict],
    merchants: list[dict],
    run_date: date,
    config: dict,
    store: "StateStore",
    fake: Faker,
    rng: random.Random,
) -> tuple[list[dict], list[dict]]:
    """
    Generate new TRANSACTION and AUTHORIZATION rows for the day.
    Returns (transactions, authorizations).
    """
    transactions = []
    authorizations = []
    rates = config.get("rates", {})
    mean = float(rates.get("transactions_per_account_per_day_mean", 1.8))
    std = float(rates.get("transactions_per_account_per_day_stddev", 1.2))
    fraud_rate = float(rates.get("fraud_rate", 0.002))

    tx_types_weights = {
        "PURCHASE": 60,
        "CASH_ADVANCE": 5,
        "PAYMENT": 20,
        "REFUND": 8,
        "FEE": 4,
        "INTEREST": 3,
    }
    tx_types = list(tx_types_weights.keys())
    tx_weights = list(tx_types_weights.values())

    active_merchants = [m for m in merchants if m] or merchants

    for account in accounts:
        status = account.get("account_status", "ACTIVE")
        if status in ("CLOSED", "CHARGEOFF"):
            continue
        open_date_str = account.get("open_date")
        if open_date_str and run_date < date.fromisoformat(open_date_str):
            continue

        # Number of transactions today for this account
        n_tx = max(0, int(rng.gauss(mean, std)))

        for _ in range(n_tx):
            tx_type = rng.choices(tx_types, weights=tx_weights)[0]
            merchant = rng.choice(active_merchants) if active_merchants else None
            merchant_id = merchant["merchant_id"] if merchant else None
            mcc = merchant["mcc_code"] if merchant else rng.choice(_MCC_CODES)
            channel = rng.choice(_CHANNELS)
            pos_mode = rng.choice(_POS_ENTRY)
            network = rng.choice(_NETWORKS)
            ts = _rand_time(run_date, rng)
            post_date = (run_date + timedelta(days=rng.randint(0, 2))).isoformat()

            if tx_type == "PAYMENT":
                amount = round(
                    float(account.get("payment_due_amount") or 25) * rng.uniform(0.5, 2.0), 2
                )
                amount = max(10.0, amount)
            elif tx_type in ("FEE", "INTEREST"):
                amount = round(rng.uniform(2, 50), 2)
            elif tx_type == "CASH_ADVANCE":
                amount = round(rng.uniform(20, 500), 2)
            else:
                amount = round(rng.uniform(1, 800), 2)

            is_fraud = tx_type == "PURCHASE" and rng.random() < fraud_rate

            # Authorization
            is_approved = not is_fraud and rng.random() < 0.92
            auth_code = "00" if is_approved else rng.choice(["05", "51", "54", "57"])
            approval_no = (
                "".join([str(rng.randint(0, 9)) for _ in range(6)]) if is_approved else None
            )
            decline_rsn = None
            if not is_approved:
                decline_rsn = rng.choice(
                    ["insufficient_funds", "expired_card", "do_not_honour", "exceeds_limit"]
                )

            auth_id = store.next_id("AUTHORIZATION")
            authorizations.append(
                {
                    "auth_id": auth_id,
                    "account_id": account["account_id"],
                    "card_id": account.get("card_id") or _get_card_id(account, store),
                    "merchant_id": merchant_id,
                    "auth_timestamp": ts,
                    "auth_amount": amount,
                    "currency_code": "CAD",
                    "auth_response_code": auth_code,
                    "auth_approval_code": approval_no,
                    "decline_reason": decline_rsn,
                    "pos_entry_mode": pos_mode,
                    "pos_condition_code": None,
                    "channel": channel,
                    "terminal_id": merchant.get("terminal_id") if merchant else None,
                    "network": network,
                    "avs_response": rng.choice(["Y", "N", "U"]) if is_approved else None,
                    "cvv_response": rng.choice(["M", "N"]) if is_approved else None,
                    "risk_score": round(rng.uniform(0, 100), 1),
                    "three_ds_result": None,
                    "ip_address": fake.ipv4() if channel == "online" else None,
                    "device_fingerprint": None,
                    "available_after_auth": round(
                        float(account.get("available_credit", 0) or 0) - amount, 2
                    ),
                    "auth_type": "standard",
                    "auth_hold_days": rng.randint(1, 3),
                }
            )

            if not is_approved:
                continue

            tx_id = store.next_id("TRANSACTION")
            transactions.append(
                {
                    "transaction_id": tx_id,
                    "account_id": account["account_id"],
                    "card_id": account.get("card_id") or _get_card_id(account, store),
                    "merchant_id": merchant_id,
                    "auth_id": auth_id,
                    "transaction_date": ts,
                    "post_date": post_date,
                    "transaction_amount": amount,
                    "billing_amount": amount,
                    "transaction_currency": "CAD",
                    "conversion_rate": 1.0,
                    "transaction_type": tx_type,
                    "transaction_status": "posted",
                    "description": random_narrative(fake),
                    "mcc_code": mcc,
                    "pos_entry_mode": pos_mode,
                    "channel": channel,
                    "reference_number": fake.bothify("REF-########"),
                    "batch_id": fake.bothify("BATCH-####"),
                    "is_recurring": 0,
                    "is_international": 1 if channel == "online" and rng.random() < 0.05 else 0,
                    "interchange_qualifier": rng.choice(["CPS", "EIRF", "STD"]),
                    "interchange_fee": round(amount * 0.015, 4),
                    "statement_id": None,
                    "_is_fraud": is_fraud,
                }
            )

    return transactions, authorizations


def _get_card_id(account: dict, store: "StateStore") -> int | None:
    cards = store.get_active_cards_for_account(account["account_id"])
    return cards[0]["card_id"] if cards else None


def generate_new_disputes(
    transactions: list[dict],
    run_date: date,
    config: dict,
    store: "StateStore",
    fake: Faker,
    rng: random.Random,
) -> list[dict]:
    """Generate DISPUTE rows for a fraction of today's transactions."""
    dispute_rate = float(config.get("rates", {}).get("dispute_rate", 0.003))
    dispute_types = [
        "FRAUD",
        "NOT_RECEIVED",
        "DUPLICATE",
        "WRONG_AMOUNT",
        "QUALITY",
        "SUBSCRIPTION",
    ]
    disputes = []

    for tx in transactions:
        if tx.get("transaction_type") not in ("PURCHASE", "CASH_ADVANCE"):
            continue
        if rng.random() > dispute_rate:
            continue
        d_type = "FRAUD" if tx.get("_is_fraud") else rng.choice(dispute_types)
        disputes.append(
            {
                "dispute_id": store.next_id("DISPUTE"),
                "transaction_id": tx["transaction_id"],
                "account_id": tx["account_id"],
                "dispute_opened_date": run_date.isoformat(),
                "dispute_type": d_type,
                "dispute_status": "OPEN",
                "dispute_reason_code": fake.bothify("RC-###"),
                "disputed_amount": tx["transaction_amount"],
                "cardholder_explanation": rng.choice(
                    [
                        "I did not make this purchase",
                        "Item never arrived",
                        "Wrong amount charged",
                        "Duplicate charge",
                        "Cancelled subscription",
                    ]
                ),
                "assigned_analyst": None,
                "response_due_date": (run_date + timedelta(days=30)).isoformat(),
                "resolution": None,
                "resolved_date": None,
            }
        )

    return disputes


def generate_new_fraud_alerts(
    transactions: list[dict],
    run_date: date,
    config: dict,
    store: "StateStore",
    rng: random.Random,
) -> list[dict]:
    """Generate FRAUD_ALERT rows for flagged transactions."""
    alert_types = ["VELOCITY", "GEO_ANOMALY", "CARD_TESTING", "HIGH_RISK_MCC", "LARGE_TXN"]
    alert_sources = ["rule_engine", "RTD", "CardGuard", "3DS", "manual"]
    alerts = []

    for tx in transactions:
        if not tx.get("_is_fraud") and rng.random() > 0.001:
            continue
        alerts.append(
            {
                "alert_id": store.next_id("FRAUD_ALERT"),
                "account_id": tx["account_id"],
                "transaction_id": tx["transaction_id"],
                "alert_timestamp": tx["transaction_date"],
                "alert_source": rng.choice(alert_sources),
                "alert_type": rng.choice(alert_types),
                "risk_score": rng.randint(200, 999),
                "alert_status": "OPEN",
                "action_taken": "none",
                "analyst_id": None,
                "resolved_date": None,
                "case_link": None,
            }
        )

    return alerts
