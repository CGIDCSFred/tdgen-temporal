"""
SCORE_RECORD state machine — monthly refresh per account.
"""

import random
from datetime import date

from tdgen_temporal.generators.field_generators import score_band
from tdgen_temporal.state_machines.base import AdvanceResult, StateMachine


class ScoreRecordStateMachine(StateMachine):
    """
    Generates a new SCORE_RECORD row on the configured score_refresh_day.
    Scores drift based on account health: delinquent accounts trend down,
    healthy accounts trend slightly up with noise.
    """

    def advance(
        self,
        entity_row: dict,
        run_date: date,
        config: dict,
        rng: random.Random,
    ) -> AdvanceResult:
        row = dict(entity_row)
        rates = config.get("rates", {})
        refresh_day = int(rates.get("score_refresh_day", 1))

        new_rows: dict[str, list[dict]] = {}

        open_date_str = row.get("open_date")
        if open_date_str and run_date < date.fromisoformat(open_date_str):
            return AdvanceResult(updated_row=row, changed_fields=[], new_rows={})

        if run_date.day == refresh_day:
            current_score = int(row.get("score_value") or row.get("risk_score") or 650)
            state = row.get("account_status", "ACTIVE")

            # Drift logic
            if state in ("DELINQUENT", "CHARGEOFF"):
                drift = rng.randint(-50, -5)
            else:
                drift = rng.randint(-10, 15)

            new_score = max(300, min(850, current_score + drift))
            band = score_band(new_score)

            score_types = ["FICO", "TRIAD", "internal", "behavioral", "bureau"]
            models = ["v3.1", "v3.2", "v4.0"]

            # Decision based on score
            if new_score >= 700:
                decision = rng.choice(["approve", "limit_increase"])
            elif new_score >= 600:
                decision = rng.choice(["approve", "review"])
            elif new_score >= 500:
                decision = rng.choice(["review", "restrict"])
            else:
                decision = rng.choice(["decline", "restrict"])

            new_rows["SCORE_RECORD"] = [
                {
                    "_account_id": row["account_id"],
                    "_score_date": run_date.isoformat(),
                    "_score_type": rng.choice(score_types),
                    "_score_value": new_score,
                    "_score_band": band,
                    "_model_version": rng.choice(models),
                    "_decision": decision,
                    "_action_code": None,
                    "_result_code": None,
                }
            ]

            # Update account risk_score
            row["risk_score"] = float(new_score)

        return AdvanceResult(
            updated_row=row,
            changed_fields=["risk_score"] if new_rows else [],
            new_rows=new_rows,
        )
