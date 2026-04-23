"""
ACCOUNT state machine.

States: ACTIVE -> DELINQUENT -> CHARGEOFF -> CLOSED

Evaluated once per account per day by DailyRunner.
The entity_row passed in is ACCOUNT JOIN account_temporal_state.
"""

import random
from datetime import date, timedelta

from tdgen_temporal.state_machines.base import AdvanceResult, SideEffect, StateMachine


class AccountStateMachine(StateMachine):
    def advance(
        self,
        entity_row: dict,
        run_date: date,
        config: dict,
        rng: random.Random,
    ) -> AdvanceResult:
        row = dict(entity_row)
        changed = []
        side_effects: list[SideEffect] = []
        rates = config.get("rates", {})
        lc = config.get("lifecycle", {})

        state = row.get("current_state", "ACTIVE")
        days_del = row.get("days_delinquent", 0) or 0
        missed = row.get("consecutive_missed_payments", 0) or 0
        balance = float(row.get("current_balance", 0) or 0)
        credit_limit = float(row.get("credit_limit", 5000) or 5000)
        payment_due = row.get("payment_due_date")

        # Payment due check
        if payment_due:
            due_date = date.fromisoformat(payment_due)
            if run_date >= due_date and state != "CLOSED":
                # Check if payment was received on or before due date
                last_pmt = row.get("last_payment_date")
                payment_received = (
                    last_pmt is not None
                    and date.fromisoformat(last_pmt) >= due_date - timedelta(days=1)
                ) or rng.random() < float(rates.get("payment_probability", 0.65))

                if payment_received:
                    # Payment made — apply and reset delinquency if caught up
                    pmt_amount = round(
                        max(balance * 0.05, float(row.get("payment_due_amount", 25) or 25)), 2
                    )
                    balance = max(0.0, balance - pmt_amount)
                    row["current_balance"] = balance
                    row["last_payment_date"] = run_date.isoformat()
                    row["last_payment_amount"] = pmt_amount
                    changed += ["current_balance", "last_payment_date", "last_payment_amount"]
                    if state == "DELINQUENT" and balance < float(
                        row.get("payment_due_amount", 25) or 25
                    ):
                        days_del = 0
                        missed = 0
                        state = "ACTIVE"
                        changed += [
                            "days_delinquent",
                            "consecutive_missed_payments",
                            "current_state",
                            "account_status",
                        ]
                else:
                    # Missed payment
                    if state != "CHARGEOFF" and state != "CLOSED":
                        days_del += 1
                        missed += 1
                        changed += ["days_delinquent", "consecutive_missed_payments"]
                        if state == "ACTIVE":
                            state = "DELINQUENT"
                            changed.append("current_state")
                            changed.append("account_status")
                # Advance payment due date by cycle length (~30 days)
                new_due = due_date + timedelta(days=30)
                row["payment_due_date"] = new_due.isoformat()
                changed.append("payment_due_date")

        # Daily delinquency aging for already-delinquent accounts
        elif state == "DELINQUENT":
            days_del += 1
            changed.append("days_delinquent")

        # DELINQUENT -> CHARGEOFF
        chargeoff_threshold = lc.get("collection_bucket_thresholds", {}).get("CHARGEOFF", 180)
        if state == "DELINQUENT" and days_del >= chargeoff_threshold:
            if rng.random() < float(rates.get("chargeoff_rate", 0.001)):
                state = "CHARGEOFF"
                changed += ["current_state", "account_status"]
                side_effects.append(
                    SideEffect(
                        table="CARD",
                        pk_col="account_id",
                        pk_val=row["account_id"],
                        updates={"card_status": "BLOCKED"},
                    )
                )

        # Update derived fields
        row["available_credit"] = round(max(0.0, credit_limit - balance), 2)
        row["days_delinquent"] = days_del
        row["current_state"] = state
        row["account_status"] = state
        row["consecutive_missed_payments"] = missed
        row["last_monetary_date"] = run_date.isoformat()

        if "available_credit" not in changed:
            changed.append("available_credit")

        return AdvanceResult(
            updated_row=row,
            changed_fields=list(set(changed)),
            side_effects=side_effects,
        )
