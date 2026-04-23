"""
COLLECTION_CASE state machine — bucket progression as delinquency ages.
States: ACTIVE -> AGENCY_REFERRAL -> PROMISE_TO_PAY -> CHARGEOFF -> CLOSED
"""

import random
from datetime import date, timedelta

from tdgen_temporal.generators.field_generators import delinquency_bucket
from tdgen_temporal.state_machines.base import AdvanceResult, StateMachine


class CollectionCaseStateMachine(StateMachine):
    def advance(
        self,
        entity_row: dict,
        run_date: date,
        config: dict,
        rng: random.Random,
    ) -> AdvanceResult:
        row = dict(entity_row)
        changed = []
        lc = config.get("lifecycle", {})
        thresholds = lc.get(
            "collection_bucket_thresholds",
            {
                "B1": 30,
                "B2": 60,
                "B3": 90,
                "B4": 120,
                "CHARGEOFF": 180,
            },
        )

        state = row.get("temporal_state") or row.get("case_status", "ACTIVE")
        days_del = int(row.get("days_delinquent", 0) or 0)
        days_in_bk = int(row.get("days_in_bucket", 0) or 0) + 1
        row["days_in_bucket"] = days_in_bk
        changed.append("days_in_bucket")

        new_bucket = delinquency_bucket(days_del)
        if new_bucket != row.get("current_bucket"):
            row["current_bucket"] = new_bucket
            row["delinquency_bucket"] = new_bucket
            row["days_in_bucket"] = 0
            changed += ["current_bucket", "delinquency_bucket", "days_in_bucket"]

        if state == "ACTIVE":
            if days_del >= thresholds.get("CHARGEOFF", 180):
                state = "CHARGEOFF"
                row["chargeoff_date"] = run_date.isoformat()
                row["chargeoff_reason"] = "excessive_delinquency"
                changed += ["case_status", "chargeoff_date", "chargeoff_reason"]
            elif days_del >= thresholds.get("B4", 120) and rng.random() < 0.15:
                state = "AGENCY_REFERRAL"
                changed.append("case_status")
            elif rng.random() < 0.05:
                state = "PROMISE_TO_PAY"
                row["next_action"] = "follow_up"
                row["next_action_date"] = (run_date + timedelta(days=7)).isoformat()
                changed += ["case_status", "next_action", "next_action_date"]

        elif state == "PROMISE_TO_PAY":
            if days_del >= thresholds.get("CHARGEOFF", 180):
                state = "CHARGEOFF"
                row["chargeoff_date"] = run_date.isoformat()
                row["chargeoff_reason"] = "broken_promise"
                changed += ["case_status", "chargeoff_date", "chargeoff_reason"]

        row["case_status"] = state
        row["current_state"] = state
        row["as_of_date"] = run_date.isoformat()

        return AdvanceResult(
            updated_row=row,
            changed_fields=list(set(changed)),
        )
