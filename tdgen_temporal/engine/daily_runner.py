"""
Daily runner — orchestrates one calendar day of simulation.

Execution order:
  1.  Load active accounts + temporal state
  2.  Account state machine (balance, delinquency, status)
  3.  Card state machine (expiry, replacement)
  4.  Score record refresh (if run_date.day == score_refresh_day)
  5.  Generate new transactions + authorizations
  6.  Generate statements (accounts where run_date.day == cycle_day)
  7.  Detect new disputes from today's transactions
  8.  Detect new fraud alerts from today's transactions
  9.  Advance open disputes
  10. Advance open fraud alerts
  11. Advance open chargebacks
  12. Advance collection cases
  13. Apply all side effects
  14. Persist all inserts/updates
  15. Write delta files
  16. Update simulation clock
"""

import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import yaml
from faker import Faker

from tdgen_temporal.db.state_store import StateStore
from tdgen_temporal.generators.field_generators import (
    expiry_date_from_today, generate_card_number, make_faker,
)
from tdgen_temporal.generators.statement import generate_statements
from tdgen_temporal.generators.transaction import (
    generate_daily_transactions, generate_new_disputes, generate_new_fraud_alerts,
)
from tdgen_temporal.output.delta_writer import DeltaSet, DeltaWriter
from tdgen_temporal.output.snapshot_writer import SnapshotWriter
from tdgen_temporal.state_machines.account import AccountStateMachine
from tdgen_temporal.state_machines.card import CardStateMachine
from tdgen_temporal.state_machines.chargeback import ChargebackStateMachine
from tdgen_temporal.state_machines.collection_case import CollectionCaseStateMachine
from tdgen_temporal.state_machines.dispute import DisputeStateMachine
from tdgen_temporal.state_machines.fraud_alert import FraudAlertStateMachine
from tdgen_temporal.state_machines.score_record import ScoreRecordStateMachine


@dataclass
class DayResult:
    run_date:   date
    run_id:     str
    inserts:    dict[str, int] = field(default_factory=dict)
    updates:    dict[str, int] = field(default_factory=dict)
    duration:   float = 0.0
    errors:     list[str] = field(default_factory=list)


class DailyRunner:
    def __init__(
        self,
        store: StateStore,
        config: dict,
        output_root: Path,
        fake: Faker,
        rng: random.Random,
    ) -> None:
        self._store  = store
        self._config = config
        self._out    = output_root
        self._fake   = fake
        self._rng    = rng

        formats = config.get("output", {}).get("formats", ["csv", "json"])
        self._delta_writer    = DeltaWriter(output_root, formats)
        self._snapshot_writer = SnapshotWriter(output_root, formats)

        self._sm_account    = AccountStateMachine()
        self._sm_card       = CardStateMachine()
        self._sm_dispute    = DisputeStateMachine()
        self._sm_fraud      = FraudAlertStateMachine()
        self._sm_chargeback = ChargebackStateMachine()
        self._sm_collection = CollectionCaseStateMachine()
        self._sm_score      = ScoreRecordStateMachine()

    def run(self, run_date: date) -> DayResult:
        t0     = time.perf_counter()
        run_id = str(uuid.uuid4())
        result = DayResult(run_date=run_date, run_id=run_id)

        inserts: dict[str, list[dict]] = {}
        updates: dict[str, list[dict]] = {}
        side_effects = []

        store   = self._store
        config  = self._config
        fake    = self._fake
        rng     = self._rng

        # 1. Load accounts
        accounts = store.get_active_accounts()

        # 2. Account state machines
        acc_updates = []
        temp_updates = []
        new_collection_cases = []

        for acc in accounts:
            res = self._sm_account.advance(acc, run_date, config, rng)
            row = res.updated_row
            acc_updates.append(row)
            temp_updates.append({
                "account_id":                   row["account_id"],
                "current_state":                row.get("current_state", "ACTIVE"),
                "days_delinquent":              row.get("days_delinquent", 0),
                "consecutive_missed_payments":  row.get("consecutive_missed_payments", 0),
                "last_payment_date":            row.get("last_payment_date"),
                "last_statement_date":          row.get("last_statement_date"),
                "payment_due_date":             row.get("payment_due_date"),
                "cycle_day":                    row.get("cycle_day", 15),
                "as_of_date":                   run_date.isoformat(),
            })
            side_effects.extend(res.side_effects)

            # Open collection case if newly delinquent and no open case
            if row.get("current_state") == "DELINQUENT" and row.get("days_delinquent", 0) >= 30:
                existing = store.fetch_one(
                    "COLLECTION_CASE",
                    "account_id = ? AND case_status NOT IN ('CLOSED','CHARGEOFF')",
                    (row["account_id"],),
                )
                if not existing:
                    case_id = store.next_id("COLLECTION_CASE")
                    new_collection_cases.append({
                        "case_id":           case_id,
                        "account_id":        row["account_id"],
                        "case_opened_date":  run_date.isoformat(),
                        "delinquency_bucket": "B1",
                        "amount_past_due":   float(row.get("payment_due_amount") or 0),
                        "total_owed":        float(row.get("current_balance") or 0),
                        "case_status":       "ACTIVE",
                        "assigned_collector": None,
                        "contact_method":    rng.choice(["phone", "email", "letter"]),
                        "last_contact_date": None,
                        "next_action_date":  (run_date + timedelta(days=3)).isoformat(),
                        "next_action":       "initial_contact",
                        "recovered_amount":  0,
                        "chargeoff_reason":  None,
                        "chargeoff_date":    None,
                    })

        updates["ACCOUNT"] = acc_updates
        store.bulk_upsert("account_temporal_state", temp_updates)

        # 3. Card state machines
        all_cards = store.fetch_all("CARD", "card_status = 'ACTIVE'")
        card_temporal = store.fetch_all("card_temporal_state")
        card_temp_map = {r["card_id"]: r for r in card_temporal}
        card_updates  = []
        new_cards     = []

        for card in all_cards:
            ctemp = card_temp_map.get(card["card_id"], {})
            merged = {**card, **ctemp}
            res = self._sm_card.advance(merged, run_date, config, rng)
            card_updates.append(res.updated_row)

            if "CARD_REPLACEMENT" in res.new_rows:
                for spec in res.new_rows["CARD_REPLACEMENT"]:
                    new_card_id = store.next_id("CARD")
                    new_cards.append({
                        "card_id":              new_card_id,
                        "account_id":           spec["_account_id"],
                        "card_number":          generate_card_number(),
                        "card_sequence_number": 2,
                        "cardholder_name":      spec["_cardholder_name"],
                        "expiry_date":          expiry_date_from_today(years_ahead=3),
                        "issue_date":           run_date.isoformat(),
                        "card_status":          "ACTIVE",
                        "card_type":            "primary",
                        "chip_enabled":         1,
                        "contactless_enabled":  1,
                        "pin_offset":           "".join([str(rng.randint(0, 9)) for _ in range(4)]),
                        "card_design_id":       None,
                        "digital_wallet_token": None,
                        "token_requestor":      None,
                        "last_used_date":       None,
                    })

        updates["CARD"] = card_updates
        if new_cards:
            inserts["CARD"] = new_cards
            store.bulk_insert("CARD", new_cards)

        # 4. Score refresh
        score_inserts = []
        for acc in acc_updates:
            res = self._sm_score.advance(acc, run_date, config, rng)
            if "SCORE_RECORD" in res.new_rows:
                for spec in res.new_rows["SCORE_RECORD"]:
                    score_id = store.next_id("SCORE_RECORD")
                    score_inserts.append({
                        "score_id":      score_id,
                        "account_id":    spec["_account_id"],
                        "score_date":    spec["_score_date"],
                        "score_type":    spec["_score_type"],
                        "score_value":   spec["_score_value"],
                        "score_band":    spec["_score_band"],
                        "model_version": spec["_model_version"],
                        "decision":      spec["_decision"],
                        "action_code":   spec["_action_code"],
                        "result_code":   spec["_result_code"],
                    })
        if score_inserts:
            inserts["SCORE_RECORD"] = score_inserts
            store.bulk_insert("SCORE_RECORD", score_inserts)

        # 5. Transactions + authorizations
        merchants = store.get_merchants()
        txns, auths = generate_daily_transactions(
            acc_updates, merchants, run_date, config, store, fake, rng
        )
        if auths:
            inserts["AUTHORIZATION"] = auths
            store.bulk_insert("AUTHORIZATION", auths)
        if txns:
            clean_txns = [{k: v for k, v in t.items() if not k.startswith("_")} for t in txns]
            inserts["TRANSACTION"] = clean_txns
            store.bulk_insert("TRANSACTION", clean_txns)

        # 6. Statements
        stmts = generate_statements(acc_updates, run_date, store, rng)
        if stmts:
            inserts["STATEMENT"] = stmts
            store.bulk_insert("STATEMENT", stmts)
            # Update last_statement_date on temporal state
            for stmt in stmts:
                for ts in temp_updates:
                    if ts["account_id"] == stmt["account_id"]:
                        ts["last_statement_date"] = run_date.isoformat()

        # 7. New disputes
        disputes = generate_new_disputes(txns, run_date, config, store, fake, rng)
        if disputes:
            inserts["DISPUTE"] = disputes
            store.bulk_insert("DISPUTE", disputes)
            # Seed dispute temporal state
            d_temps = [{
                "dispute_id":    d["dispute_id"],
                "current_state": "OPEN",
                "days_open":     0,
                "resolved_date": None,
                "as_of_date":    run_date.isoformat(),
            } for d in disputes]
            store.bulk_upsert("dispute_temporal_state", d_temps)

        # 8. New fraud alerts
        fraud_alerts = generate_new_fraud_alerts(txns, run_date, config, store, rng)
        if fraud_alerts:
            inserts["FRAUD_ALERT"] = fraud_alerts
            store.bulk_insert("FRAUD_ALERT", fraud_alerts)
            fa_temps = [{
                "alert_id":     a["alert_id"],
                "current_state": "OPEN",
                "days_open":     0,
                "reviewed_date": None,
                "as_of_date":    run_date.isoformat(),
            } for a in fraud_alerts]
            store.bulk_upsert("fraud_alert_temporal_state", fa_temps)

        # 9. Advance open disputes
        open_disputes  = store.get_active_disputes()
        d_updates      = []
        d_temp_updates = []
        new_chargebacks = []

        for disp in open_disputes:
            res = self._sm_dispute.advance(disp, run_date, config, rng)
            d_updates.append(res.updated_row)
            d_temp_updates.append({
                "dispute_id":    disp["dispute_id"],
                "current_state": res.updated_row.get("temporal_state", "OPEN"),
                "days_open":     res.updated_row.get("days_open", 0),
                "resolved_date": res.updated_row.get("resolved_date"),
                "as_of_date":    run_date.isoformat(),
            })
            side_effects.extend(res.side_effects)
            if "CHARGEBACK" in res.new_rows:
                for spec in res.new_rows["CHARGEBACK"]:
                    cb_id = store.next_id("CHARGEBACK")
                    new_chargebacks.append({
                        "chargeback_id":        cb_id,
                        "dispute_id":           spec["_dispute_id"],
                        "transaction_id":       spec["_transaction_id"],
                        "chargeback_date":      spec["_chargeback_date"],
                        "chargeback_amount":    spec["_chargeback_amount"],
                        "chargeback_reason_code": fake.bothify("CB-###"),
                        "chargeback_stage":     spec["_initial_stage"],
                        "representment_status": "none",
                        "representment_date":   None,
                        "recovered_amount":     0,
                        "network_case_id":      fake.bothify("NET-########"),
                    })

        if d_updates:
            updates["DISPUTE"] = d_updates
        store.bulk_upsert("dispute_temporal_state", d_temp_updates)
        if new_chargebacks:
            inserts["CHARGEBACK"] = new_chargebacks
            store.bulk_insert("CHARGEBACK", new_chargebacks)
            cb_temps = [{
                "chargeback_id": cb["chargeback_id"],
                "current_state": "FIRST_CHARGEBACK",
                "days_open":     0,
                "as_of_date":    run_date.isoformat(),
            } for cb in new_chargebacks]
            store.bulk_upsert("chargeback_temporal_state", cb_temps)

        # 10. Advance open fraud alerts
        open_alerts   = store.get_active_fraud_alerts()
        fa_updates    = []
        fa_temp_upds  = []
        for alert in open_alerts:
            res = self._sm_fraud.advance(alert, run_date, config, rng)
            fa_updates.append(res.updated_row)
            fa_temp_upds.append({
                "alert_id":      alert["alert_id"],
                "current_state": res.updated_row.get("temporal_state", "OPEN"),
                "days_open":     res.updated_row.get("days_open", 0),
                "reviewed_date": res.updated_row.get("resolved_date"),
                "as_of_date":    run_date.isoformat(),
            })
            side_effects.extend(res.side_effects)
        if fa_updates:
            updates["FRAUD_ALERT"] = fa_updates
        store.bulk_upsert("fraud_alert_temporal_state", fa_temp_upds)

        # 11. Advance open chargebacks
        open_cbs   = store.get_active_chargebacks()
        cb_updates = []
        cb_temp_u  = []
        for cb in open_cbs:
            res = self._sm_chargeback.advance(cb, run_date, config, rng)
            cb_updates.append(res.updated_row)
            cb_temp_u.append({
                "chargeback_id": cb["chargeback_id"],
                "current_state": res.updated_row.get("temporal_state", "FIRST_CHARGEBACK"),
                "days_open":     res.updated_row.get("days_open", 0),
                "as_of_date":    run_date.isoformat(),
            })
        if cb_updates:
            updates["CHARGEBACK"] = cb_updates
        store.bulk_upsert("chargeback_temporal_state", cb_temp_u)

        # 12. Advance collection cases
        open_cases   = store.get_active_collection_cases()
        cc_updates   = []
        cc_temp_u    = []
        # Enrich with days_delinquent from account
        acc_del_map = {a["account_id"]: a.get("days_delinquent", 0) for a in acc_updates}
        for case in open_cases:
            case["days_delinquent"] = acc_del_map.get(case["account_id"], 0)
            res = self._sm_collection.advance(case, run_date, config, rng)
            cc_updates.append(res.updated_row)
            cc_temp_u.append({
                "case_id":       case["case_id"],
                "current_state": res.updated_row.get("current_state", "ACTIVE"),
                "current_bucket": res.updated_row.get("current_bucket", "B1"),
                "days_in_bucket": res.updated_row.get("days_in_bucket", 0),
                "as_of_date":    run_date.isoformat(),
            })

        # Add new collection cases
        if new_collection_cases:
            inserts["COLLECTION_CASE"] = new_collection_cases
            store.bulk_insert("COLLECTION_CASE", new_collection_cases)
            cc_init_temps = [{
                "case_id":       c["case_id"],
                "current_state": "ACTIVE",
                "current_bucket": "B1",
                "days_in_bucket": 0,
                "as_of_date":    run_date.isoformat(),
            } for c in new_collection_cases]
            store.bulk_upsert("collection_case_temporal_state", cc_init_temps)

        if cc_updates:
            updates["COLLECTION_CASE"] = cc_updates
        store.bulk_upsert("collection_case_temporal_state", cc_temp_u)

        # 13. Apply side effects
        for se in side_effects:
            if se.updates.get("current_balance") is None:
                continue  # placeholder side effect — skip
            try:
                store.update_row(se.table, se.pk_col, se.pk_val, se.updates)
            except Exception as e:
                result.errors.append(f"SideEffect error: {e}")

        # Apply card blocks from fraud/chargeoff
        for se in side_effects:
            if se.table == "CARD" and "card_status" in se.updates:
                store._conn.execute(
                    'UPDATE "CARD" SET card_status = ? WHERE account_id = ? AND card_status = \'ACTIVE\'',
                    (se.updates["card_status"], se.pk_val),
                )
                store._conn.commit()

        # 14. Persist account updates
        for acc_row in acc_updates:
            acc_id = acc_row["account_id"]
            store.update_row("ACCOUNT", "account_id", acc_id, {
                "current_balance":      acc_row.get("current_balance"),
                "available_credit":     acc_row.get("available_credit"),
                "account_status":       acc_row.get("account_status"),
                "days_delinquent":      acc_row.get("days_delinquent"),
                "payment_due_date":     acc_row.get("payment_due_date"),
                "last_payment_date":    acc_row.get("last_payment_date"),
                "last_payment_amount":  acc_row.get("last_payment_amount"),
                "last_monetary_date":   acc_row.get("last_monetary_date"),
                "risk_score":           acc_row.get("risk_score"),
            })

        # Persist dispute updates
        for d_row in d_updates:
            store.update_row("DISPUTE", "dispute_id", d_row["dispute_id"], {
                "dispute_status": d_row.get("dispute_status"),
                "resolution":     d_row.get("resolution"),
                "resolved_date":  d_row.get("resolved_date"),
            })

        # Persist fraud alert updates
        for fa_row in fa_updates:
            store.update_row("FRAUD_ALERT", "alert_id", fa_row["alert_id"], {
                "alert_status":  fa_row.get("alert_status"),
                "action_taken":  fa_row.get("action_taken"),
                "resolved_date": fa_row.get("resolved_date"),
            })

        # Persist chargeback updates
        for cb_row in cb_updates:
            store.update_row("CHARGEBACK", "chargeback_id", cb_row["chargeback_id"], {
                "chargeback_stage":     cb_row.get("chargeback_stage"),
                "representment_status": cb_row.get("representment_status"),
                "representment_date":   cb_row.get("representment_date"),
                "recovered_amount":     cb_row.get("recovered_amount"),
            })

        # Persist collection case updates
        for cc_row in cc_updates:
            store.update_row("COLLECTION_CASE", "case_id", cc_row["case_id"], {
                "case_status":       cc_row.get("case_status"),
                "delinquency_bucket": cc_row.get("delinquency_bucket"),
                "chargeoff_date":    cc_row.get("chargeoff_date"),
                "chargeoff_reason":  cc_row.get("chargeoff_reason"),
                "next_action":       cc_row.get("next_action"),
                "next_action_date":  cc_row.get("next_action_date"),
            })

        # 15. Write delta files
        delta = DeltaSet(
            run_date=run_date,
            inserts={k: v for k, v in inserts.items() if v},
            updates={k: v for k, v in updates.items() if v},
        )
        self._delta_writer.write(delta)

        if config.get("output", {}).get("write_snapshots", False):
            self._snapshot_writer.write(run_date, store)

        # 16. Update simulation clock
        total_runs = store.get_total_runs() + 1
        store.set_simulation_meta(run_date, run_id, total_runs)
        store.record_run(
            run_id=run_id,
            run_date=run_date,
            run_mode="advance",
            accounts_processed=len(accounts),
            inserts={k: len(v) for k, v in inserts.items()},
            updates={k: len(v) for k, v in updates.items()},
            duration=time.perf_counter() - t0,
        )

        result.inserts  = {k: len(v) for k, v in inserts.items()}
        result.updates  = {k: len(v) for k, v in updates.items()}
        result.duration = time.perf_counter() - t0
        return result
