"""
CHARGEBACK state machine.
States: FIRST_CHARGEBACK -> REPRESENTMENT -> PRE_ARBITRATION -> ARBITRATION -> WON | LOST
"""

import random
from datetime import date, timedelta

from tdgen_temporal.state_machines.base import AdvanceResult, StateMachine


class ChargebackStateMachine(StateMachine):

    def advance(
        self,
        entity_row: dict,
        run_date: date,
        config: dict,
        rng: random.Random,
    ) -> AdvanceResult:
        row     = dict(entity_row)
        changed = []
        lc      = config.get("lifecycle", {})

        state     = row.get("temporal_state") or row.get("chargeback_stage", "FIRST_CHARGEBACK")
        days_open = int(row.get("days_open", 0) or 0)
        days_open += 1
        row["days_open"] = days_open
        changed.append("days_open")

        rep_days = int(lc.get("chargeback_representment_days", 10))

        if state == "FIRST_CHARGEBACK" and days_open >= rep_days:
            if rng.random() < 0.60:
                state = "REPRESENTMENT"
                row["chargeback_stage"]       = "REPRESENTMENT"
                row["representment_status"]   = "sent"
                row["representment_date"]     = run_date.isoformat()
                changed += ["chargeback_stage", "representment_status", "representment_date"]
            else:
                state = "WON"
                row["chargeback_stage"]     = "FIRST_CHARGEBACK"
                row["representment_status"] = "none"
                row["recovered_amount"]     = row.get("chargeback_amount", 0)
                changed += ["chargeback_stage", "representment_status", "recovered_amount"]

        elif state == "REPRESENTMENT" and days_open >= rep_days * 2:
            outcome = rng.choices(["WON", "LOST", "PRE_ARBITRATION"], weights=[50, 30, 20])[0]
            state = outcome
            row["chargeback_stage"] = outcome
            if outcome == "WON":
                row["representment_status"] = "accepted"
                row["recovered_amount"]     = row.get("chargeback_amount", 0)
            elif outcome == "LOST":
                row["representment_status"] = "rejected"
                row["recovered_amount"]     = 0
            changed += ["chargeback_stage", "representment_status", "recovered_amount"]

        elif state == "PRE_ARBITRATION" and days_open >= rep_days * 3:
            outcome = rng.choices(["WON", "LOST"], weights=[40, 60])[0]
            state = outcome
            row["chargeback_stage"]     = outcome
            row["representment_status"] = "accepted" if outcome == "WON" else "rejected"
            row["recovered_amount"]     = row.get("chargeback_amount", 0) if outcome == "WON" else 0
            changed += ["chargeback_stage", "representment_status", "recovered_amount"]

        row["temporal_state"] = state

        return AdvanceResult(
            updated_row=row,
            changed_fields=list(set(changed)),
        )
