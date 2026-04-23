"""
FRAUD_ALERT state machine.
States: OPEN -> UNDER_REVIEW -> CONFIRMED | FALSE_POSITIVE -> CLOSED
"""

import random
from datetime import date, timedelta

from tdgen_temporal.state_machines.base import AdvanceResult, SideEffect, StateMachine


class FraudAlertStateMachine(StateMachine):

    def advance(
        self,
        entity_row: dict,
        run_date: date,
        config: dict,
        rng: random.Random,
    ) -> AdvanceResult:
        row     = dict(entity_row)
        changed = []
        side_effects: list[SideEffect] = []
        rates = config.get("rates", {})
        lc    = config.get("lifecycle", {})

        state     = row.get("temporal_state") or row.get("alert_status", "OPEN")
        days_open = int(row.get("days_open", 0) or 0)
        days_open += 1
        row["days_open"] = days_open
        changed.append("days_open")

        review_days       = int(lc.get("fraud_alert_review_days", 2))
        confirm_rate      = float(rates.get("fraud_confirmation_rate", 0.30))

        if state == "OPEN":
            if days_open >= 7:
                # Auto-expire
                state = "CLOSED"
                row["alert_status"] = "CLOSED"
                row["resolved_date"] = run_date.isoformat()
                changed += ["alert_status", "current_state", "resolved_date"]
            elif rng.random() < 0.95:
                state = "UNDER_REVIEW"
                row["alert_status"] = "UNDER_REVIEW"
                changed += ["alert_status", "current_state"]

        elif state == "UNDER_REVIEW":
            if days_open >= review_days:
                if rng.random() < confirm_rate:
                    state = "CONFIRMED"
                    row["alert_status"] = "CONFIRMED"
                    row["action_taken"] = "block_card"
                    row["resolved_date"] = run_date.isoformat()
                    changed += ["alert_status", "current_state", "action_taken", "resolved_date"]
                    # Block the card
                    if row.get("transaction_id"):
                        side_effects.append(SideEffect(
                            table="CARD", pk_col="account_id", pk_val=row["account_id"],
                            updates={"card_status": "BLOCKED"},
                        ))
                else:
                    state = "FALSE_POSITIVE"
                    row["alert_status"] = "FALSE_POSITIVE"
                    row["action_taken"] = "none"
                    row["resolved_date"] = run_date.isoformat()
                    changed += ["alert_status", "current_state", "action_taken", "resolved_date"]

        elif state == "CONFIRMED":
            reviewed = row.get("reviewed_date") or row.get("resolved_date")
            if reviewed:
                if (run_date - date.fromisoformat(reviewed)).days >= 30:
                    state = "CLOSED"
                    row["alert_status"] = "CLOSED"
                    changed += ["alert_status", "current_state"]

        row["temporal_state"] = state

        return AdvanceResult(
            updated_row=row,
            changed_fields=list(set(changed)),
            side_effects=side_effects,
        )
