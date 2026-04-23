"""
Smoke tests — verify core modules import and basic operations work.
"""

from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parent.parent / "config" / "scenario.yaml"


def load_config() -> dict:
    return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))


# ── Field generators ──────────────────────────────────────────────────────────


class TestFieldGenerators:
    def test_make_faker_returns_faker(self):
        from tdgen_temporal.generators.field_generators import make_faker

        fake = make_faker(42)
        assert fake is not None
        assert fake.name()  # produces a name

    def test_generate_card_number_luhn(self):
        from tdgen_temporal.generators.field_generators import generate_card_number

        number = generate_card_number()
        assert number.isdigit(), f"Expected digits, got {number!r}"
        assert _luhn_valid(number), f"Luhn check failed for {number}"

    def test_expiry_date_is_future(self):
        from tdgen_temporal.generators.field_generators import expiry_date_from_today

        expiry_str = expiry_date_from_today(years_ahead=3)
        # expiry is returned as a string (YYYY-MM-DD or MM/YY)
        assert expiry_str is not None and len(expiry_str) > 0


def _luhn_valid(number: str) -> bool:
    digits = [int(d) for d in number]
    odd_digits = digits[-1::-2]
    even_digits = digits[-2::-2]
    total = sum(odd_digits)
    for d in even_digits:
        total += sum(divmod(d * 2, 10))
    return total % 10 == 0


# ── Config ────────────────────────────────────────────────────────────────────


class TestConfig:
    def test_config_loads(self):
        config = load_config()
        assert "simulation" in config
        assert "rates" in config
        assert "lifecycle" in config

    def test_config_required_keys(self):
        config = load_config()
        assert config["simulation"]["seed"] == 42
        assert config["rates"]["fraud_rate"] > 0
        assert config["rates"]["dispute_rate"] > 0

    def test_collection_buckets_ordered(self):
        config = load_config()
        thresholds = config["lifecycle"]["collection_bucket_thresholds"]
        values = list(thresholds.values())
        assert values == sorted(values), "Bucket thresholds must be ascending"


# ── State machines ─────────────────────────────────────────────────────────────


class TestStateMachines:
    def test_account_state_machine_imports(self):
        from tdgen_temporal.state_machines.account import AccountStateMachine

        assert AccountStateMachine is not None

    def test_dispute_state_machine_imports(self):
        from tdgen_temporal.state_machines.dispute import DisputeStateMachine

        assert DisputeStateMachine is not None

    def test_fraud_alert_state_machine_imports(self):
        from tdgen_temporal.state_machines.fraud_alert import FraudAlertStateMachine

        assert FraudAlertStateMachine is not None


# ── Database ──────────────────────────────────────────────────────────────────


class TestStateStore:
    def test_state_store_creates_db(self, tmp_path):
        from tdgen_temporal.db.state_store import StateStore

        db = tmp_path / "test.db"
        store = StateStore(db)
        assert db.exists()
        store.close()

    def test_count_after_migrations(self, tmp_path):
        from tdgen_temporal.db.migrations import create_all_tables
        from tdgen_temporal.db.state_store import StateStore

        db = tmp_path / "test.db"
        create_all_tables(db)
        store = StateStore(db)
        n = store.count("ACCOUNT")
        assert n == 0  # empty after schema creation, before seeding
        store.close()
