"""
CARD state machine — handles expiry and replacement.
States: ACTIVE -> BLOCKED -> CANCELLED | EXPIRED
"""

import random
from datetime import date

from tdgen_temporal.state_machines.base import AdvanceResult, StateMachine


class CardStateMachine(StateMachine):
    def advance(
        self,
        entity_row: dict,
        run_date: date,
        config: dict,
        rng: random.Random,
    ) -> AdvanceResult:
        row = dict(entity_row)
        changed = []
        new_rows: dict[str, list[dict]] = {}

        state = row.get("current_state") or row.get("card_status", "ACTIVE")

        expiry = row.get("expiry_date")
        if expiry and state == "ACTIVE":
            exp_date = date.fromisoformat(expiry[:10])
            days_to_exp = (exp_date - run_date).days
            row["days_to_expiry"] = days_to_exp
            changed.append("days_to_expiry")

            if days_to_exp <= 0:
                state = "EXPIRED"
                row["card_status"] = "EXPIRED"
                changed += ["card_status", "current_state"]

                # Issue replacement card if not already done
                if not row.get("replacement_issued"):
                    row["replacement_issued"] = 1
                    changed.append("replacement_issued")
                    # New card row will be created by DailyRunner using this signal
                    new_rows["CARD_REPLACEMENT"] = [
                        {
                            "_account_id": row["account_id"],
                            "_cardholder_name": row.get("cardholder_name", ""),
                            "_replaced_card_id": row["card_id"],
                        }
                    ]

        row["current_state"] = state
        row["as_of_date"] = run_date.isoformat()

        return AdvanceResult(
            updated_row=row,
            changed_fields=list(set(changed)),
            new_rows=new_rows,
        )
