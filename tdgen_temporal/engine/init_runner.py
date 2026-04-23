"""
Init runner — seeds the database from scratch for Day 0.
"""

import random
import time
import uuid
from datetime import date
from pathlib import Path

import yaml
from faker import Faker

from tdgen_temporal.db.migrations import create_all_tables
from tdgen_temporal.db.state_store import StateStore
from tdgen_temporal.generators.ref_tables import REF_DATA
from tdgen_temporal.generators.seed import (
    seed_accounts, seed_clients, seed_merchants, seed_products, seed_providers,
)
from tdgen_temporal.generators.field_generators import make_faker


def run_init(
    db_path: Path,
    config_path: Path,
    run_date: date | None = None,
) -> dict:
    """
    Create all tables and seed the Day 0 population.
    Returns a summary dict.
    """
    t0 = time.perf_counter()

    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    sim    = config.get("simulation", {})
    pop    = sim.get("initial_population", {})

    seed   = sim.get("seed", 42)
    fake   = make_faker(seed)
    rng    = random.Random(seed)

    if run_date is None:
        run_date = date.today()

    run_id = str(uuid.uuid4())

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        print(f"[init] Removing existing database {db_path}...")
        db_path.unlink()

    print(f"[init] Creating tables in {db_path}...")
    create_all_tables(db_path)
    store = StateStore(db_path)

    # Initialise PK sequences
    for tbl in ["CLIENT", "PROVIDER", "PRODUCT_DEFINITION", "MERCHANT",
                "ACCOUNT", "CUSTOMER", "CARD", "AUTHORIZATION", "TRANSACTION",
                "STATEMENT", "DISPUTE", "CHARGEBACK", "FRAUD_ALERT",
                "SCORE_RECORD", "COLLECTION_CASE"]:
        store.init_sequence(tbl, 1)

    # REF tables
    print("[init] Seeding reference tables...")
    for table_name, rows in REF_DATA.items():
        store.bulk_insert(table_name, rows)

    # Core entities
    n_clients   = pop.get("clients",   3)
    n_providers = pop.get("providers", 10)
    n_products  = pop.get("products",  20)
    n_merchants = pop.get("merchants", 200)
    n_accounts  = pop.get("accounts",  500)

    print(f"[init] Seeding {n_clients} clients...")
    clients = seed_clients(fake, n_clients, store)
    store.bulk_insert("CLIENT", clients)

    print(f"[init] Seeding {n_providers} providers...")
    providers = seed_providers(fake, clients, n_providers, store)
    store.bulk_insert("PROVIDER", providers)

    print(f"[init] Seeding {n_products} products...")
    products = seed_products(fake, clients, n_products, store)
    store.bulk_insert("PRODUCT_DEFINITION", products)

    print(f"[init] Seeding {n_merchants} merchants...")
    merchants = seed_merchants(fake, n_merchants, store)
    store.bulk_insert("MERCHANT", merchants)

    print(f"[init] Seeding {n_accounts} accounts (+ customers + cards)...")
    accounts, customers, cards, temp_states = seed_accounts(
        fake, providers, products, n_accounts, run_date, store
    )
    store.bulk_insert("ACCOUNT",  accounts)
    store.bulk_insert("CUSTOMER", customers)
    store.bulk_insert("CARD",     cards)
    store.bulk_upsert("account_temporal_state", temp_states)

    # Set simulation clock
    store.set_simulation_meta(run_date, run_id, total_runs=0)
    store.record_run(
        run_id=run_id,
        run_date=run_date,
        run_mode="init",
        accounts_processed=len(accounts),
        inserts={
            "CLIENT": len(clients), "PROVIDER": len(providers),
            "PRODUCT_DEFINITION": len(products), "MERCHANT": len(merchants),
            "ACCOUNT": len(accounts), "CUSTOMER": len(customers), "CARD": len(cards),
        },
        updates={},
        duration=time.perf_counter() - t0,
    )
    store.close()

    elapsed = time.perf_counter() - t0
    summary = {
        "run_id":    run_id,
        "run_date":  run_date.isoformat(),
        "db_path":   str(db_path),
        "tables":    len(REF_DATA) + 5,
        "accounts":  len(accounts),
        "customers": len(customers),
        "cards":     len(cards),
        "merchants": len(merchants),
        "duration":  round(elapsed, 2),
    }

    print(f"\n[init] Done in {elapsed:.1f}s")
    print(f"  Accounts : {len(accounts):,}")
    print(f"  Customers: {len(customers):,}")
    print(f"  Cards    : {len(cards):,}")
    print(f"  Merchants: {len(merchants):,}")
    print(f"  DB       : {db_path}")

    return summary
