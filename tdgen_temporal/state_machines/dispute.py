"""
DISPUTE state machine.
States: OPEN -> INVESTIGATING -> RESOLVED -> CLOSED | WITHDRAWN
"""

import random
from datetime import date, timedelta

from tdgen_temporal.state_machines.base import AdvanceResult, SideEffect, StateMachine


class DisputeStateMachine(StateMachine):

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
        new_rows: dict[str, list[dict]] = {}
        rates = config.get("rates", {})
        lc    = config.get("lifecycle", {})

        state     = row.get("temporal_state") or row.get("dispute_status", "OPEN")
        days_open = int(row.get("days_open", 0) or 0)
        days_open += 1
        row["days_open"] = days_open
        changed.append("days_open")

        investigating_days = int(lc.get("dispute_investigating_days", 7))
        resolution_days    = int(lc.get("dispute_resolution_days", 30))
        withdrawal_rate    = float(rates.get("dispute_withdrawal_rate", 0.05))
        chargeback_rate    = float(rates.get("chargeback_rate", 0.40))

        if state == "OPEN":
            if days_open <= 3 and rng.random() < withdrawal_rate:
                state = "WITHDRAWN"
                row["dispute_status"] = "WITHDRAWN"
                changed += ["dispute_status", "current_state"]
            elif rng.random() < 0.90:
                state = "INVESTIGATING"
                row["dispute_status"] = "INVESTIGATING"
                changed += ["dispute_status", "current_state"]

        elif state == "INVESTIGATING":
            if days_open >= resolution_days:
                # Force close
                state = "CLOSED"
                row["dispute_status"] = "CLOSED"
                row["resolved_date"] = run_date.isoformat()
                row["resolution"] = "WRITTEN_OFF"
                changed += ["dispute_status", "current_state", "resolved_date", "resolution"]
            elif days_open >= investigating_days and rng.random() < 0.70:
                state = "RESOLVED"
                resolution = rng.choice(["APPROVED", "DENIED", "PARTIAL"])
                row["dispute_status"] = "RESOLVED"
                row["resolution"]    = resolution
                row["resolved_date"] = run_date.isoformat()
                changed += ["dispute_status", "current_state", "resolution", "resolved_date"]

                # Credit back if approved
                if resolution in ("APPROVED", "PARTIAL"):
                    credit_pct = 1.0 if resolution == "APPROVED" else 0.5
                    credit_amt = round(float(row.get("disputed_amount", 0) or 0) * credit_pct, 2)
                    if credit_amt > 0:
                        side_effects.append(SideEffect(
                            table="ACCOUNT", pk_col="account_id", pk_val=row["account_id"],
                            updates={"current_balance": None},  # handled in DailyRunner with arithmetic
                        ))

                # Possibly trigger chargeback
                if resolution in ("APPROVED", "PARTIAL") and rng.random() < chargeback_rate:
                    new_rows["CHARGEBACK"] = [{
                        "_dispute_id":          row["dispute_id"],
                        "_transaction_id":       row["transaction_id"],
                        "_chargeback_amount":    float(row.get("disputed_amount", 0) or 0),
                        "_chargeback_date":      run_date.isoformat(),
                        "_initial_stage":        "FIRST_CHARGEBACK",
                    }]

        elif state == "RESOLVED":
            resolved_date = row.get("resolved_date")
            if resolved_date:
                rd = date.fromisoformat(resolved_date)
                if (run_date - rd).days >= 5:
                    state = "CLOSED"
                    row["dispute_status"] = "CLOSED"
                    changed += ["dispute_status", "current_state"]

        row["temporal_state"] = state

        return AdvanceResult(
            updated_row=row,
            changed_fields=list(set(changed)),
            side_effects=side_effects,
            new_rows=new_rows,
        )
