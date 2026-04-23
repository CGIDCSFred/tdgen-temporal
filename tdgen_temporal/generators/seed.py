"""
Day-0 population seeding — generates the initial set of accounts,
customers, cards, merchants, clients, providers, and products.
"""

import random
from datetime import date, timedelta
from typing import Any

from faker import Faker

from tdgen_temporal.generators.field_generators import (
    _CA_PROVINCES, _CARD_BRANDS, _CURRENCIES, _US_STATES,
    delinquency_bucket, expiry_date_from_today, generate_account_number,
    generate_card_number, generate_sin, random_decimal, random_past_date,
    random_product_name, score_band,
)


def seed_clients(fake: Faker, n: int, store: "StateStore") -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "client_id":       store.next_id("CLIENT"),
            "client_name":     fake.company(),
            "bin_range":       "".join([str(random.randint(4, 5))] + [str(random.randint(0, 9)) for _ in range(5)]),
            "base_currency":   "CAD",
            "region":          random.choice(["NA", "EMEA", "APAC", "LATAM"]),
            "processing_mode": random.choice(["batch", "online", "hybrid"]),
            "association_id":  fake.bothify("??####").upper(),
        })
    return rows


def seed_providers(fake: Faker, clients: list[dict], n: int, store: "StateStore") -> list[dict]:
    rows = []
    portfolio_types = ["retail", "small_business", "consumer", "commercial"]
    for _ in range(n):
        rows.append({
            "provider_id":     store.next_id("PROVIDER"),
            "client_id":       random.choice(clients)["client_id"],
            "provider_name":   fake.company(),
            "portfolio_type":  random.choice(portfolio_types),
            "reporting_group": fake.bothify("???-###").upper(),
            "status":          random.choices(["active", "inactive"], weights=[85, 15])[0],
        })
    return rows


def seed_products(fake: Faker, clients: list[dict], n: int, store: "StateStore") -> list[dict]:
    rows = []
    rewards = ["points", "miles", "cashback", None]
    billing = ["monthly", "bi-monthly"]
    for i in range(n):
        rows.append({
            "tsys_product_code":    store.next_id("PRODUCT_DEFINITION"),
            "client_product_code":  i + 1,
            "client_id":            random.choice(clients)["client_id"],
            "product_description":  random_product_name(),
            "card_brand":           random.choice(_CARD_BRANDS),
            "default_credit_limit": random.randint(500, 50000),
            "annual_fee":           random.choice([0, 39, 79, 99, 120, 150]),
            "apr_purchase":         round(random.uniform(9.99, 29.99), 2),
            "apr_cash_advance":     round(random.uniform(19.99, 24.99), 2),
            "grace_period_days":    random.choice([21, 25]),
            "rewards_program":      random.choice(rewards),
            "billing_cycle_type":   random.choice(billing),
            "fee_schedule_id":      fake.bothify("FEE-####"),
        })
    return rows


def seed_merchants(fake: Faker, n: int, store: "StateStore") -> list[dict]:
    from tdgen_temporal.generators.field_generators import _MCC_CODES
    rows = []
    risk_tiers = ["low", "medium", "high"]
    provinces  = _CA_PROVINCES + _US_STATES
    for _ in range(n):
        rows.append({
            "merchant_id":    store.next_id("MERCHANT"),
            "merchant_name":  fake.company(),
            "dba_name":       fake.company(),
            "mcc_code":       random.choice(_MCC_CODES),
            "terminal_id":    fake.bothify("TRM-########"),
            "acquirer_id":    fake.bothify("ACQ-####"),
            "city":           fake.city(),
            "state_province": random.choice(provinces),
            "country_code":   random.choices(["CA", "US"], weights=[70, 30])[0],
            "postal_zip":     fake.postcode(),
            "merchant_url":   None,
            "risk_tier":      random.choices(risk_tiers, weights=[65, 25, 10])[0],
            "is_online":      random.choices([0, 1], weights=[70, 30])[0],
        })
    return rows


def seed_accounts(
    fake: Faker,
    providers: list[dict],
    products: list[dict],
    n: int,
    run_date: date,
    store: "StateStore",
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """
    Returns (accounts, customers, cards, account_temporal_states).
    """
    accounts    = []
    customers   = []
    cards       = []
    temp_states = []

    statuses = ["ACTIVE", "ACTIVE", "ACTIVE", "ACTIVE", "DELINQUENT", "CHARGEOFF", "CLOSED"]
    card_types = ["primary", "supplementary"]

    for _ in range(n):
        product    = random.choice(products)
        provider   = random.choice(providers)
        credit_lim = product["default_credit_limit"] + random.randint(-5000, 15000)
        credit_lim = max(500, credit_lim)
        balance    = round(random.uniform(0, credit_lim * 0.8), 2)
        status     = random.choices(statuses, weights=[70, 5, 5, 5, 8, 5, 2])[0]
        open_date  = random_past_date(fake, years_back=8, as_of=run_date)
        open_date_parsed = date.fromisoformat(open_date)
        cycle_day  = random.randint(1, 28)

        # days delinquent
        days_del = 0
        if status == "DELINQUENT":
            days_del = random.randint(1, 120)
        elif status == "CHARGEOFF":
            days_del = random.randint(180, 360)

        acc_id = store.next_id("ACCOUNT")
        accounts.append({
            "account_id":           acc_id,
            "provider_id":          provider["provider_id"],
            "tsys_product_code":    product["tsys_product_code"],
            "client_product_code":  product["client_product_code"],
            "account_number":       generate_account_number(),
            "credit_limit":         credit_lim,
            "current_balance":      balance,
            "available_credit":     round(credit_lim - balance, 2),
            "cash_advance_limit":   round(credit_lim * 0.25, 2),
            "cash_advance_balance": 0,
            "payment_due_amount":   round(balance * 0.02, 2),
            "payment_due_date":     (run_date + timedelta(days=random.randint(5, 25))).isoformat(),
            "last_payment_date":    random_past_date(fake, years_back=1, as_of=run_date, not_before=open_date_parsed) if random.random() > 0.2 else None,
            "last_payment_amount":  round(random.uniform(25, 500), 2) if random.random() > 0.2 else None,
            "open_date":            open_date,
            "closed_date":          random_past_date(fake, years_back=1, as_of=run_date, not_before=open_date_parsed) if status == "CLOSED" else None,
            "account_status":       status,
            "status_reason":        None,
            "currency_code":        "CAD",
            "cycle_day":            cycle_day,
            "days_delinquent":      days_del,
            "block_code":           None,
            "risk_score":           round(random.uniform(300, 850), 1),
            "last_monetary_date":   random_past_date(fake, years_back=1, as_of=run_date, not_before=open_date_parsed),
            "last_non_monetary_date": None,
        })

        # Customer
        first = fake.first_name()
        last  = fake.last_name()
        customers.append({
            "customer_id":        store.next_id("CUSTOMER"),
            "account_id":         acc_id,
            "first_name":         first,
            "last_name":          last,
            "name_line_1":        f"{first} {last}",
            "date_of_birth":      fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat(),
            "ssn_sin":            generate_sin(),
            "address_line_1":     fake.street_address(),
            "address_line_2":     None,
            "city":               fake.city(),
            "state_province":     random.choice(_CA_PROVINCES),
            "postal_zip":         fake.postcode(),
            "country_code":       "CA",
            "phone_home":         fake.phone_number(),
            "phone_work":         fake.phone_number() if random.random() > 0.4 else None,
            "phone_mobile":       fake.phone_number(),
            "email":              fake.email(),
            "language_preference": random.choices(["EN", "FR"], weights=[75, 25])[0],
            "relationship_type":  "primary",
            "id_type":            random.choice(["PASSPORT", "DRIVERS_LICENSE", "SIN"]),
            "id_number":          fake.bothify("??######").upper(),
            "employer_name":      fake.company() if random.random() > 0.2 else None,
            "annual_income":      round(random.uniform(25000, 250000), 0),
        })

        # Card
        cards.append({
            "card_id":              store.next_id("CARD"),
            "account_id":           acc_id,
            "card_number":          generate_card_number(),
            "card_sequence_number": 1,
            "cardholder_name":      f"{first} {last}",
            "expiry_date":          expiry_date_from_today(years_ahead=3),
            "issue_date":           open_date,
            "card_status":          "ACTIVE" if status in ("ACTIVE", "DELINQUENT") else ("BLOCKED" if status == "CHARGEOFF" else "CANCELLED"),
            "card_type":            "primary",
            "chip_enabled":         1,
            "contactless_enabled":  1,
            "pin_offset":           "".join(random.choices("0123456789", k=4)),
            "card_design_id":       fake.bothify("DESIGN-###"),
            "digital_wallet_token": None,
            "token_requestor":      None,
            "last_used_date":       random_past_date(fake, years_back=1, as_of=run_date, not_before=open_date_parsed),
        })

        # Temporal state
        temp_states.append({
            "account_id":                   acc_id,
            "current_state":                status,
            "days_delinquent":              days_del,
            "consecutive_missed_payments":  0 if status == "ACTIVE" else random.randint(1, 6),
            "last_payment_date":            accounts[-1]["last_payment_date"],
            "last_statement_date":          random_past_date(fake, years_back=1, as_of=run_date, not_before=open_date_parsed),
            "payment_due_date":             accounts[-1]["payment_due_date"],
            "cycle_day":                    cycle_day,
            "as_of_date":                   run_date.isoformat(),
        })

    return accounts, customers, cards, temp_states
