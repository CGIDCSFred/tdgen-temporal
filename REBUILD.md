# TDGen-Temporal — Rebuild Guide

This document is for colleagues who want to run or recreate TDGen-Temporal.
It covers two paths: cloning the existing repository (fastest), and rebuilding
the entire project from scratch using Claude Code (no repo access required).

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | 3.10 or later | https://www.python.org/downloads/ |
| Git | any | https://git-scm.com/download/win |
| Graphviz | 12.x | https://graphviz.org/download/ — tick **Add to PATH** during install |
| Claude Code CLI | latest | `npm install -g @anthropic-ai/claude-code` (requires Node 18+) |

Verify Graphviz is on PATH after install:

```
dot -V
```

---

## Option A — Clone and run (fastest)

```powershell
git clone <repo-url> tdgen-temporal
cd tdgen-temporal

python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python -m tdgen_temporal.cli init --date 2024-01-01
python -m tdgen_temporal.cli advance --days 30

.\launch_ui.bat
```

Browser opens at **http://localhost:8501**.

---

## Option B — Rebuild from scratch with Claude Code

Use this if you have no repository access or want to regenerate the project
on a clean machine.

### Step 1 — Create a project directory

```powershell
mkdir tdgen-temporal
cd tdgen-temporal
git init
```

### Step 2 — Open Claude Code

```powershell
claude
```

### Step 3 — Paste the rebuild prompt

Copy everything inside the code block below and paste it into Claude Code.
It is a complete specification for the entire project — Claude Code will
create every file, install dependencies, run tests, and verify the simulation
works before finishing.

---

```
Build a Python application called tdgen-temporal — a schema-driven synthetic test
data generator with temporal simulation. It maintains a SQLite state database and
emits daily delta files (CSV + JSON) for every insert and update.

Read this specification in full before writing any code. Then create every file
described. After all files exist, create a virtual environment, install
dependencies, run smoke tests, initialise the simulation at 2024-01-01, advance
7 days, and run validate --errors-only. Fix any errors before finishing.

════════════════════════════════════════════════════════════════════════════════
1. TECHNOLOGY STACK
════════════════════════════════════════════════════════════════════════════════

- Python 3.10+ (use `from __future__ import annotations` at top of every file)
- SQLite WAL mode via stdlib sqlite3 — no ORM
- faker >= 24.0 (locale en_CA)
- pandas >= 2.0
- pyyaml >= 6.0
- streamlit >= 1.35.0
- plotly >= 5.22.0
- pandera >= 0.20
- openpyxl >= 3.1

⚠ CRITICAL SQLITE NOTE: "TRANSACTION" and "AUTHORIZATION" are reserved keywords
in SQLite. Always quote them in DDL and DML:
  CREATE TABLE IF NOT EXISTS "TRANSACTION" (...)
  SELECT * FROM "TRANSACTION" WHERE ...
Never write `FROM TRANSACTION` unquoted — SQLite raises OperationalError.

════════════════════════════════════════════════════════════════════════════════
2. PROJECT LAYOUT
════════════════════════════════════════════════════════════════════════════════

tdgen_temporal/
    __init__.py
    cli.py
    schema.py
    db/
        __init__.py
        migrations.py
        state_store.py
    engine/
        __init__.py
        init_runner.py
        daily_runner.py
        backfill_runner.py
    generators/
        __init__.py
        field_generators.py
        ref_tables.py
        seed.py
        transaction.py
        statement.py
    output/
        __init__.py
        delta_writer.py
        snapshot_writer.py   (stub only)
    state_machines/
        __init__.py
        base.py
        account.py
        card.py
        dispute.py
        fraud_alert.py
        chargeback.py
        collection_case.py
        score_record.py
    validators/
        __init__.py
        results.py
        referential.py
        temporal.py
        state.py
config/
    scenario.yaml
tests/
    __init__.py
    test_smoke.py
ui/
    app.py
.github/
    workflows/
        ci.yml
pyproject.toml
requirements.txt
.pre-commit-config.yaml
launch_ui.bat

════════════════════════════════════════════════════════════════════════════════
3. CONFIGURATION FILES
════════════════════════════════════════════════════════════════════════════════

── pyproject.toml ──────────────────────────────────────────────────────────────

[build-system]
requires = ["setuptools>=70", "wheel"]
build-backend = "setuptools.backends.legacy:build"

[project]
name = "tdgen-temporal"
version = "0.1.0"
description = "Temporal synthetic test data generator"
requires-python = ">=3.10"
dependencies = [
    "faker>=24.0", "pandas>=2.0", "pyyaml>=6.0",
    "streamlit>=1.35.0", "plotly>=5.22.0",
]

[project.scripts]
tdgen = "tdgen_temporal.cli:main"

[tool.ruff]
target-version = "py310"
line-length = 100
exclude = ["docs/"]

[tool.ruff.lint]
select = ["E","W","F","I","UP","B"]
ignore = ["E501","B008","B905"]

[tool.ruff.lint.per-file-ignores]
"tests/*" = ["S101"]

[tool.mypy]
python_version = "3.10"
ignore_missing_imports = true
warn_unused_ignores = true
check_untyped_defs = false

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_functions = ["test_*"]
addopts = "-v --tb=short"

[tool.coverage.run]
source = ["tdgen_temporal"]
omit = ["tests/*","ui/*","docs/*"]

── requirements.txt ─────────────────────────────────────────────────────────────

faker>=24.0
pandas>=2.0
pandera>=0.20
openpyxl>=3.1
pyyaml>=6.0
streamlit>=1.35.0
plotly>=5.22.0

── config/scenario.yaml ────────────────────────────────────────────────────────

simulation:
  seed: 42
  locale: en_CA
  initial_population:
    accounts:   500
    merchants:  200
    clients:      3
    providers:   10
    products:    20

rates:
  transactions_per_account_per_day_mean:   1.8
  transactions_per_account_per_day_stddev: 1.2
  payment_probability:     0.65
  delinquency_rate:        0.04
  chargeoff_rate:          0.001
  fraud_rate:              0.002
  dispute_rate:            0.003
  chargeback_rate:         0.40
  fraud_confirmation_rate: 0.30
  dispute_withdrawal_rate: 0.05
  score_refresh_day:       5      # day-of-month; 5 ensures scores appear in any 30-day run from day 1

lifecycle:
  dispute_investigating_days:     7
  dispute_resolution_days:       30
  fraud_alert_review_days:        2
  chargeback_representment_days: 10
  collection_bucket_thresholds:
    B1:        30
    B2:        60
    B3:        90
    B4:       120
    CHARGEOFF: 180

output:
  formats:
    - csv
    - json
  write_snapshots: false

── launch_ui.bat ────────────────────────────────────────────────────────────────

@echo off
start "" http://localhost:8501
"%~dp0.venv\Scripts\streamlit" run "%~dp0ui\app.py" --server.headless=false %*

════════════════════════════════════════════════════════════════════════════════
4. DATABASE SCHEMA  (db/migrations.py)
════════════════════════════════════════════════════════════════════════════════

Enable WAL and disable FK enforcement on every connection:
  PRAGMA journal_mode=WAL
  PRAGMA foreign_keys=OFF

Create all tables using CREATE TABLE IF NOT EXISTS.

Control tables:
  simulation_meta(id INTEGER PK CHECK(id=1), current_run_date TEXT NOT NULL,
    last_run_id TEXT NOT NULL, total_runs INTEGER DEFAULT 0,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL)

  run_log(run_id TEXT PK, run_date TEXT NOT NULL, run_mode TEXT NOT NULL,
    accounts_processed INTEGER, inserts_json TEXT, updates_json TEXT,
    duration_seconds REAL, created_at TEXT NOT NULL)

  pk_sequences(table_name TEXT PK, next_id INTEGER NOT NULL DEFAULT 1)

REF tables (all TEXT PKs, no FK enforcement):
  REF_ACCOUNT_STATUS(status_code PK, status_description, is_active INT, weight_pct REAL)
  REF_CARD_STATUS(status_code PK, status_description, is_usable INT, weight_pct REAL)
  REF_CARD_BRAND(brand_code PK, brand_name, bin_prefix, weight_pct REAL)
  REF_TRANSACTION_TYPE(type_code PK, type_description, is_debit INT, weight_pct REAL)
  REF_DISPUTE_TYPE(type_code PK, type_description, weight_pct REAL)
  REF_CHARGEBACK_STAGE(stage_code PK, stage_description, stage_order INT)
  REF_FRAUD_ALERT_TYPE(alert_type_code PK, alert_type_description, detection_system, weight_pct REAL)
  REF_AUTH_RESPONSE(response_code PK, response_description, is_approved INT, weight_pct REAL)
  REF_POS_ENTRY_MODE(entry_mode_code PK, entry_mode_description, is_card_present INT, weight_pct REAL)
  REF_CHANNEL(channel_code PK, channel_description, weight_pct REAL)
  REF_DELINQUENCY_BUCKET(bucket_code PK, bucket_description, min_days INT, max_days INT, bucket_order INT)
  REF_CURRENCY(currency_code PK, currency_name, symbol, decimal_places INT)
  REF_COUNTRY(country_code PK, country_name, region)
  REF_MCC(mcc_code PK, mcc_description, mcc_group, weight_pct REAL)
  REF_STATUS_REASON(reason_code PK, reason_description, applies_to_status)
  REF_SENSITIVITY_LEVEL(sensitivity_code PK, sensitivity_description, generation_rule,
    applicable_regulations)

Entity tables (all INTEGER PKs):
  CLIENT(client_id PK, client_name, bin_range, base_currency, region, processing_mode,
    association_id)

  PROVIDER(provider_id PK, client_id, provider_name, portfolio_type, reporting_group,
    status)

  PRODUCT_DEFINITION(tsys_product_code, client_product_code, client_id,
    product_description, card_brand, default_credit_limit INT, annual_fee REAL,
    apr_purchase REAL, apr_cash_advance REAL, grace_period_days INT, rewards_program,
    billing_cycle_type, fee_schedule_id,
    PRIMARY KEY(tsys_product_code, client_product_code))

  MERCHANT(merchant_id PK, merchant_name, dba_name, mcc_code, terminal_id, acquirer_id,
    city, state_province, country_code, postal_zip, merchant_url, risk_tier, is_online INT)

  ACCOUNT(account_id PK, provider_id, tsys_product_code, client_product_code,
    account_number, credit_limit REAL, current_balance REAL DEFAULT 0,
    available_credit REAL, cash_advance_limit REAL, cash_advance_balance REAL DEFAULT 0,
    payment_due_amount REAL DEFAULT 0, payment_due_date, last_payment_date,
    last_payment_amount REAL, open_date, closed_date,
    account_status TEXT DEFAULT 'ACTIVE', status_reason, currency_code TEXT DEFAULT 'CAD',
    cycle_day INT, days_delinquent INT DEFAULT 0, block_code, risk_score REAL,
    last_monetary_date, last_non_monetary_date)

  CUSTOMER(customer_id PK, account_id, first_name, last_name, name_line_1,
    date_of_birth, ssn_sin, address_line_1, address_line_2, city, state_province,
    postal_zip, country_code, phone_home, phone_work, phone_mobile, email,
    language_preference, relationship_type, id_type, id_number, employer_name,
    annual_income REAL)

  CARD(card_id PK, account_id, card_number, card_sequence_number INT, cardholder_name,
    expiry_date, issue_date, card_status TEXT DEFAULT 'ACTIVE', card_type,
    chip_enabled INT, contactless_enabled INT, pin_offset, card_design_id,
    digital_wallet_token, token_requestor, last_used_date)

  -- AUTHORIZATION is a SQLite reserved word but NOT a keyword — quoting is safe here
  AUTHORIZATION(auth_id PK, account_id, card_id, merchant_id, auth_timestamp,
    auth_amount REAL, currency_code, auth_response_code, auth_approval_code,
    decline_reason, pos_entry_mode, pos_condition_code, channel, terminal_id,
    network, avs_response, cvv_response, risk_score REAL, three_ds_result,
    ip_address, device_fingerprint, available_after_auth REAL, auth_type,
    auth_hold_days INT)

  -- TRANSACTION IS a SQLite reserved keyword — MUST be quoted in DDL and all queries
  "TRANSACTION"(transaction_id PK, account_id, card_id, merchant_id, auth_id,
    transaction_date, post_date, transaction_amount REAL, billing_amount REAL,
    transaction_currency, conversion_rate REAL, transaction_type, transaction_status,
    description, mcc_code, pos_entry_mode, channel, reference_number, batch_id,
    is_recurring INT, is_international INT, interchange_qualifier,
    interchange_fee REAL, statement_id INT)

  STATEMENT(statement_id PK, account_id, statement_date, payment_due_date,
    opening_balance REAL, closing_balance REAL, total_credits REAL, total_debits REAL,
    minimum_payment REAL, interest_charged REAL, fees_charged REAL,
    transaction_count INT, available_credit REAL, cycle_id)

  DISPUTE(dispute_id PK, transaction_id, account_id, dispute_opened_date,
    dispute_type, dispute_status TEXT DEFAULT 'OPEN', dispute_reason_code,
    disputed_amount REAL, cardholder_explanation, assigned_analyst,
    response_due_date, resolution, resolved_date)

  CHARGEBACK(chargeback_id PK, dispute_id, transaction_id, chargeback_date,
    chargeback_amount REAL, chargeback_reason_code, chargeback_stage,
    representment_status, representment_date, recovered_amount REAL, network_case_id)

  FRAUD_ALERT(alert_id PK, account_id, transaction_id, alert_timestamp, alert_source,
    alert_type, risk_score INT, alert_status TEXT DEFAULT 'OPEN', action_taken,
    analyst_id, resolved_date, case_link)

  SCORE_RECORD(score_id PK, account_id, score_date, score_type, score_value INT,
    score_band, model_version, decision, action_code, result_code)

  COLLECTION_CASE(case_id PK, account_id, case_opened_date, delinquency_bucket,
    amount_past_due REAL, total_owed REAL, case_status TEXT DEFAULT 'ACTIVE',
    assigned_collector, contact_method, last_contact_date, next_action_date,
    next_action, recovered_amount REAL DEFAULT 0, chargeoff_reason, chargeoff_date)

Temporal state tables (simulation-internal, not exported in delta files):
  account_temporal_state(account_id PK, current_state TEXT NOT NULL DEFAULT 'ACTIVE',
    days_delinquent INT NOT NULL DEFAULT 0,
    consecutive_missed_payments INT NOT NULL DEFAULT 0,
    last_payment_date, last_statement_date, payment_due_date,
    cycle_day INT NOT NULL, as_of_date TEXT NOT NULL)

  card_temporal_state(card_id PK, current_state TEXT NOT NULL DEFAULT 'ACTIVE',
    days_to_expiry INT, replacement_issued INT DEFAULT 0, as_of_date TEXT NOT NULL)

  dispute_temporal_state(dispute_id PK, current_state TEXT NOT NULL DEFAULT 'OPEN',
    days_open INT NOT NULL DEFAULT 0, resolved_date, as_of_date TEXT NOT NULL)

  fraud_alert_temporal_state(alert_id PK, current_state TEXT NOT NULL DEFAULT 'OPEN',
    days_open INT NOT NULL DEFAULT 0, reviewed_date, as_of_date TEXT NOT NULL)

  chargeback_temporal_state(chargeback_id PK,
    current_state TEXT NOT NULL DEFAULT 'FIRST_CHARGEBACK',
    days_open INT NOT NULL DEFAULT 0, as_of_date TEXT NOT NULL)

  collection_case_temporal_state(case_id PK,
    current_state TEXT NOT NULL DEFAULT 'ACTIVE',
    current_bucket TEXT NOT NULL DEFAULT 'B1',
    days_in_bucket INT NOT NULL DEFAULT 0, as_of_date TEXT NOT NULL)

════════════════════════════════════════════════════════════════════════════════
5. REFERENCE DATA  (generators/ref_tables.py)
════════════════════════════════════════════════════════════════════════════════

Seed at init time using plain INSERT INTO.

REF_ACCOUNT_STATUS: ACTIVE(w=70,active=1), DELINQUENT(w=12,active=1),
  SUSPENDED(w=5,active=0), CHARGEOFF(w=8,active=0), CLOSED(w=5,active=0)

REF_CARD_STATUS: ACTIVE(w=75,usable=1), BLOCKED(w=5,0), EXPIRED(w=8,0),
  CANCELLED(w=7,0), LOST(w=3,0), STOLEN(w=2,0)

REF_CARD_BRAND: Visa(bin=4,w=45), Mastercard(bin=5,w=40), Amex(bin=37,w=10),
  Interac(bin=63,w=5)

REF_TRANSACTION_TYPE: PURCHASE(debit=1,w=60), CASH_ADVANCE(debit=1,w=5),
  PAYMENT(debit=0,w=20), REFUND(debit=0,w=8), FEE(debit=1,w=3),
  INTEREST(debit=1,w=3), REVERSAL(debit=0,w=1)

REF_DISPUTE_TYPE: FRAUD(w=40), NOT_RECEIVED(w=20), DUPLICATE(w=15),
  WRONG_AMOUNT(w=15), QUALITY(w=7), SUBSCRIPTION(w=3)

REF_CHARGEBACK_STAGE: FIRST_CHARGEBACK(order=1), REPRESENTMENT(order=2),
  PRE_ARBITRATION(order=3), ARBITRATION(order=4)

REF_FRAUD_ALERT_TYPE: VELOCITY(rule_engine,w=30), GEO_ANOMALY(RTD,w=20),
  CARD_TESTING(rule_engine,w=15), HIGH_RISK_MCC(rule_engine,w=10),
  LARGE_TXN(CardGuard,w=15), 3DS_FAIL(3DS,w=5), SCORE_DROP(scoring,w=5)

REF_AUTH_RESPONSE: 00=Approved(approved=1,w=80), 05=Do_not_honour(w=5),
  14=Invalid_card(w=2), 51=Insufficient_funds(w=6), 54=Expired(w=3),
  57=Not_permitted(w=2), 61=Exceeds_limit(w=1), 78=No_account(w=1)

REF_POS_ENTRY_MODE: chip(present=1,w=45), contactless(present=1,w=30),
  swipe(present=1,w=5), ecommerce(present=0,w=15), manual(present=0,w=3),
  atm(present=1,w=2)

REF_CHANNEL: in_store(w=45), online(w=35), mobile(w=15), telephone(w=3), atm(w=2)

REF_DELINQUENCY_BUCKET: CURRENT(0-0,order=0), B1(1-30,order=1),
  B2(31-60,order=2), B3(61-90,order=3), B4(91-120,order=4), B5(121-999,order=5)

REF_CURRENCY: CAD, USD, EUR, GBP

REF_COUNTRY: CA(NA), US(NA), GB(EU)

REF_MCC (14 codes with weights): 5411=Grocery(food,w=15), 5812=Restaurant(food,w=12),
  5541=Gas(auto,w=8), 5310=Discount(retail,w=10), 5912=Pharmacy(health,w=7),
  5999=Misc_retail(retail,w=8), 4111=Transit(transport,w=5), 4121=Taxi(transport,w=4),
  5734=Electronics(tech,w=6), 7011=Hotel(travel,w=5), 5045=Computers(tech,w=4),
  5065=Electronics_parts(tech,w=3), 5621=Women_clothing(retail,w=7),
  5651=Family_clothing(retail,w=6)

REF_STATUS_REASON: PAYMENT_RECEIVED→ACTIVE, CREDIT_APPROVED→ACTIVE,
  MISSED_PAYMENT→DELINQUENT, FRAUD_CONFIRMED→SUSPENDED, OVERLIMIT→SUSPENDED,
  CHARGEOFF_BAD_DEBT→CHARGEOFF, CUSTOMER_REQUEST→CLOSED, BANK_DECISION→CLOSED

REF_SENSITIVITY_LEVEL: PII(faker_approved,PIPEDA/GDPR), PCI(luhn_valid,PCI-DSS),
  PHI(faker_approved,PHIPA), NONE(any,none)

════════════════════════════════════════════════════════════════════════════════
6. STATE STORE  (db/state_store.py)
════════════════════════════════════════════════════════════════════════════════

Class StateStore wraps a single sqlite3.Connection (row_factory=sqlite3.Row).
Constructor opens the connection; enable WAL and disable FK enforcement.

Methods:
  next_id(table_name) → int          atomic increment in pk_sequences
  init_sequence(table_name, start)   insert if not exists
  bulk_insert(table, rows)           INSERT INTO using row dict keys as columns
  bulk_upsert(table, rows)           INSERT OR REPLACE
  update_row(table, pk_col, pk_val, updates)
  count(table) → int                 SELECT COUNT(*) — quote reserved words
  get_all_accounts() → list[dict]    JOIN ACCOUNT with account_temporal_state
  get_active_accounts() → list[dict] same but WHERE current_state != 'CLOSED'
    and WHERE current_state != 'CHARGEOFF'
  get_open_disputes() → list[dict]   JOIN DISPUTE with dispute_temporal_state
    WHERE current_state NOT IN ('CLOSED','WITHDRAWN')
  get_open_fraud_alerts() → list[dict]
  get_open_chargebacks() → list[dict]
  get_open_collection_cases() → list[dict]
  get_active_cards_for_account(account_id) → list[dict]
  get_all_merchants() → list[dict]
  set_simulation_meta(run_date, run_id, total_runs)  UPSERT simulation_meta
  get_simulation_meta() → dict | None
  get_current_run_date() → date | None
  record_run(run_id, run_date, run_mode, accounts_processed, inserts, updates, duration)
  close()

⚠ When building bulk_insert, build the INSERT SQL from the first row's keys.
  Quote table names that are SQLite reserved words (TRANSACTION, AUTHORIZATION).
  The safest approach: always quote: f'INSERT INTO "{table}" ...'

════════════════════════════════════════════════════════════════════════════════
7. FIELD GENERATORS  (generators/field_generators.py)
════════════════════════════════════════════════════════════════════════════════

make_faker(seed=None) → Faker("en_CA") seeded if provided

generate_card_number() → Luhn-valid 16-digit string, prefix 4 or 51–55

generate_sin() → "NNN-NNN-NNN" format (synthetic Canadian SIN)

generate_account_number() → 16 random digits as string

score_band(score: int) → str:
  >= 750: "excellent"
  >= 700: "good"
  >= 650: "fair"
  >= 600: "medium"
  >= 500: "low"
  else:   "very_high_risk"

delinquency_bucket(days: int) → str:
  0:       "CURRENT"
  1-30:    "B1"
  31-60:   "B2"
  61-90:   "B3"
  91-120:  "B4"
  121+:    "B5+"

expiry_date_from_today(years_ahead=3) → last day of a random future month (YYYY-MM-DD)

random_past_date(fake, years_back=5) → date string YYYY-MM-DD

random_narrative(fake) → pick from a list of ~20 realistic Canadian merchant names
  (e.g. "TIM HORTONS #1234", "SHOPPERS DRUG MART", "PETRO-CANADA", "AMAZON.CA")

random_decimal(mn, mx, decimals=2) → rounded float

════════════════════════════════════════════════════════════════════════════════
8. DAY 0 SEEDING  (generators/seed.py)
════════════════════════════════════════════════════════════════════════════════

seed_clients(fake, n, store) — faker company names, BIN range "4XXXXX-5XXXXX",
  base_currency=CAD, region from [NA,EMEA,APAC,LATAM],
  processing_mode from [batch,online,hybrid]

seed_providers(fake, clients, n, store) — link to random client,
  portfolio_type from [retail,small_business,consumer,commercial],
  status=ACTIVE 85% else INACTIVE

seed_products(fake, clients, n, store) — 20 products, APR 9.99–29.99%,
  credit limit 500–50000 in steps, annual_fee from {0,39,79,99,120,150},
  rewards from [points,miles,cashback,None]

seed_merchants(fake, n, store) — 200 merchants, 70% CA / 30% US,
  risk_tier: low=65%, medium=25%, high=10%, is_online=30%

seed_accounts(fake, providers, products, n, run_date, store):
  Returns (accounts, customers, cards, account_temporal_states).
  Status distribution: ACTIVE ×~350, DELINQUENT ~40, CHARGEOFF ~25,
  CLOSED ~10, SUSPENDED ~5 (weights 70/12/8/5/5 pct approx out of 500).
  Each account → one CUSTOMER and one CARD.
  DELINQUENT accounts: days_delinquent 1–120.
  CHARGEOFF accounts: days_delinquent 180–360, block_code="C".
  CLOSED accounts: closed_date set to random past date.
  Cards: ACTIVE/DELINQUENT → card_status=ACTIVE;
         CHARGEOFF → BLOCKED; CLOSED → CANCELLED.
  open_date: random date in the past 5 years, never in the future.

  ⚠ TEMPORAL GUARD: In daily_runner and score_record state machine, always
  check run_date >= open_date before generating any events for an account.
  Skip accounts whose open_date is after run_date.

════════════════════════════════════════════════════════════════════════════════
9. STATE MACHINES  (state_machines/)
════════════════════════════════════════════════════════════════════════════════

── base.py ──────────────────────────────────────────────────────────────────────

@dataclass
class SideEffect:
    table: str
    pk_col: str
    pk_val: Any
    updates: dict

@dataclass
class AdvanceResult:
    updated_row: dict
    changed_fields: list[str]
    side_effects: list[SideEffect] = field(default_factory=list)
    new_rows: dict[str, list[dict]] = field(default_factory=dict)

class StateMachine(ABC):
    @abstractmethod
    def advance(self, entity_row, run_date, config, rng) -> AdvanceResult: ...

── account.py ───────────────────────────────────────────────────────────────────

States: ACTIVE → DELINQUENT → CHARGEOFF → CLOSED

Each day, if run_date >= payment_due_date:
  Payment received (probability=payment_probability OR last_payment_date within 1 day of due):
    reduce balance by max(min_payment, payment_due_amount)
    reset days_delinquent to 0 if balance <= payment_due_amount
    advance payment_due_date +30 days
    update last_payment_date
  Missed payment:
    days_delinquent++
    consecutive_missed_payments++
    if days_delinquent >= 30 and status==ACTIVE: transition to DELINQUENT
    if days_delinquent >= chargeoff_threshold (180) and random < chargeoff_rate:
      transition to CHARGEOFF
      emit SideEffect to block card (set card_status=BLOCKED)
      open COLLECTION_CASE new_row if not already open

  If DELINQUENT and days_delinquent >= 30 and no open COLLECTION_CASE:
    emit new_rows["COLLECTION_CASE"] with case_opened_date=run_date,
    delinquency_bucket from days_delinquent, amount_past_due, total_owed

── card.py ──────────────────────────────────────────────────────────────────────

States: ACTIVE → EXPIRED; can be BLOCKED via SideEffect from account/fraud

Each day for ACTIVE cards:
  Compute days_to_expiry = (expiry_date - run_date).days
  If days_to_expiry <= 0: transition to EXPIRED, set replacement_issued=1
    emit new_rows["CARD"] with new card: sequence_number=old+1,
    expiry_date=3 years ahead, card_status=ACTIVE, issue_date=run_date

── dispute.py ───────────────────────────────────────────────────────────────────

States: OPEN → INVESTIGATING → RESOLVED → CLOSED | WITHDRAWN

Each day: increment days_open.
OPEN:
  if days_open <= 3 and rng < withdrawal_rate(0.05): → WITHDRAWN; close
  else 90% chance → INVESTIGATING
INVESTIGATING:
  if days_open >= resolution_days(30): force-close → CLOSED (resolution=WRITTEN_OFF)
  elif days_open >= investigating_days(7) and rng < 0.70:
    resolution = random(APPROVED/DENIED/PARTIAL weighted 50/35/15)
    → RESOLVED; set resolved_date=run_date
    if resolution in (APPROVED, PARTIAL) and rng < chargeback_rate:
      emit new_rows["CHARGEBACK"] with chargeback_date=run_date,
      chargeback_amount=disputed_amount, chargeback_stage=FIRST_CHARGEBACK
RESOLVED:
  if (run_date - resolved_date).days >= 5: → CLOSED

── fraud_alert.py ───────────────────────────────────────────────────────────────

States: OPEN → UNDER_REVIEW → CONFIRMED | FALSE_POSITIVE → CLOSED

Each day: increment days_open.
OPEN:
  if days_open >= 7: auto-expire → CLOSED
  else rng < 0.95: → UNDER_REVIEW
UNDER_REVIEW:
  if days_open >= review_days(2):
    if rng < fraud_confirmation_rate(0.30):
      → CONFIRMED; action_taken=block_card
      emit SideEffect: set card_status=BLOCKED for account's active card
    else: → FALSE_POSITIVE; action_taken=none
CONFIRMED:
  if (run_date - resolved_date).days >= 30: → CLOSED

── chargeback.py ────────────────────────────────────────────────────────────────

States: FIRST_CHARGEBACK → REPRESENTMENT → PRE_ARBITRATION → WON | LOST

Each day: increment days_open.
FIRST_CHARGEBACK + days_open >= rep_days(10):
  60% → REPRESENTMENT; 40% → WON (recovered_amount = chargeback_amount)
REPRESENTMENT + days_open >= rep_days*2:
  50% WON / 30% LOST / 20% PRE_ARBITRATION
PRE_ARBITRATION + days_open >= rep_days*3:
  40% WON / 60% LOST

── collection_case.py ───────────────────────────────────────────────────────────

States: ACTIVE → AGENCY_REFERRAL | PROMISE_TO_PAY → CHARGEOFF | RESOLVED → CLOSED

Each day: increment days_in_bucket; recalculate bucket from account.days_delinquent.
ACTIVE:
  if days_delinquent >= 180: → CHARGEOFF; set chargeoff_reason=BAD_DEBT
  elif days_delinquent >= 120 and rng < 0.15: → AGENCY_REFERRAL
  elif rng < 0.05: → PROMISE_TO_PAY
PROMISE_TO_PAY:
  if days_delinquent >= 180: → CHARGEOFF (reason=broken_promise)

── score_record.py ──────────────────────────────────────────────────────────────

Each day, if run_date.day == config.rates.score_refresh_day
AND run_date >= account.open_date:

  current_score = account.risk_score or 650
  drift: DELINQUENT/CHARGEOFF → randint(-50,-5); else → randint(-10,15)
  new_score = clamp(current_score + drift, 300, 850)
  band = score_band(new_score)
  score_type = random from [FICO, TRIAD, internal, behavioral, bureau]
  model_version = random from [v3.1, v3.2, v4.0]
  decision: >=700→[approve,limit_increase]; >=600→[approve,review];
            >=500→[review,restrict]; else→[decline,restrict]

  emit new_rows["SCORE_RECORD"] with all fields
  update account.risk_score = float(new_score)

════════════════════════════════════════════════════════════════════════════════
10. DAILY RUNNER  (engine/daily_runner.py)
════════════════════════════════════════════════════════════════════════════════

DailyRunner.run(run_date: date) executes these 16 steps:

1.  Load active accounts (get_active_accounts — JOIN with temporal state)
2.  AccountStateMachine.advance on each → collect ACCOUNT updates + SideEffects
    ⚠ Skip accounts where run_date < open_date
3.  Load all cards; CardStateMachine.advance → collect CARD updates;
    for CARD_REPLACEMENT new_rows create new CARD row
4.  ScoreRecordStateMachine.advance on each account
    ⚠ Skip if run_date < account.open_date
5.  generate_daily_transactions() → TRANSACTION + AUTHORIZATION rows
    ⚠ Skip accounts where run_date < open_date
6.  generate_statements() → STATEMENT rows for accounts where
    run_date.day == account.cycle_day
7.  generate_new_disputes() from today's transactions (dispute_rate)
8.  generate_new_fraud_alerts() from flagged transactions
9.  DisputeStateMachine on all open disputes
10. FraudAlertStateMachine on all open fraud alerts
11. ChargebackStateMachine on all open chargebacks
12. CollectionCaseStateMachine on all open collection cases
13. Apply SideEffects (card blocks, balance adjustments)
14. Persist: bulk_insert new rows, update_row changed rows
15. DeltaWriter.write() — CSV/JSON delta files
16. set_simulation_meta + record_run

════════════════════════════════════════════════════════════════════════════════
11. TRANSACTION GENERATION  (generators/transaction.py)
════════════════════════════════════════════════════════════════════════════════

Per active, non-CLOSED, non-CHARGEOFF account per day:
  n_tx = max(0, round(rng.gauss(mean=1.8, sigma=1.2)))

  Transaction types by weight: PURCHASE=60, PAYMENT=20, REFUND=8, FEE=4,
    INTEREST=3, CASH_ADVANCE=5

  Amounts:
    PAYMENT: max(10, payment_due_amount * uniform(0.5, 2.0))
    FEE/INTEREST: uniform(2, 50)
    CASH_ADVANCE: uniform(20, 500)
    else: uniform(1, 800)

  Fraud flag: _is_fraud = (type==PURCHASE and rng.random() < fraud_rate)
    Store as internal key "_is_fraud"; strip before writing to DB or files

  Authorization: 92% approval for non-fraud; fraud → declined.
    Declined codes: 05/51/54/57. post_date = transaction_date + 0–2 days.

  Disputes: for each PURCHASE/CASH_ADVANCE, if rng.random() < dispute_rate:
    open DISPUTE. type=FRAUD if _is_fraud, else random.

  Fraud alerts: for each _is_fraud transaction + rng.random() < 0.001 for clean:
    emit FRAUD_ALERT with alert_type from REF_FRAUD_ALERT_TYPE weights.

════════════════════════════════════════════════════════════════════════════════
12. OUTPUT  (output/delta_writer.py)
════════════════════════════════════════════════════════════════════════════════

Output layout:
  output/deltas/YYYY-MM-DD/
    inserts/TRANSACTION.csv + .json
    inserts/AUTHORIZATION.csv + .json
    ...
    updates/ACCOUNT.csv + .json
    ...

Strip any key prefixed with "_" before writing.
Write CSV via pandas; JSON pretty-printed with default=str.

════════════════════════════════════════════════════════════════════════════════
13. CLI  (cli.py)
════════════════════════════════════════════════════════════════════════════════

Four subcommands. All default: --db output/state.db, --config config/scenario.yaml,
--output output.

init --date YYYY-MM-DD [--db] [--config]
  Delete existing DB, create all tables, seed Day 0 population,
  set simulation_meta, record init run.

advance --days N [--db] [--config] [--output]
  Load current_run_date, loop DailyRunner from +1 day for N iterations.

backfill --from YYYY-MM-DD --to YYYY-MM-DD [--db] [--config] [--output]
  Loop DailyRunner over the explicit date range (inclusive).

status [--db]
  Print current_run_date, total_runs, row counts for all entity tables.

validate [--db] [--errors-only]
  Run validators.run_all(db_path), print report via results.print_report.
  Exit code 1 if any ERROR findings.

════════════════════════════════════════════════════════════════════════════════
14. SCHEMA MODULE  (schema.py)
════════════════════════════════════════════════════════════════════════════════

GROUPS dict: 6 groups with label/header-colour/bg-colour:
  core:        label="Core Entities",       header=#3b82f6, bg=#0f1f3d
  transaction: label="Transactions",        header=#10b981, bg=#0a2a1e
  risk:        label="Risk Management",     header=#ef4444, bg=#2a0f0f
  temporal:    label="Temporal State",      header=#f59e0b, bg=#2a1e0a
  reference:   label="Reference Data",      header=#8b5cf6, bg=#1a0f2d
  control:     label="Simulation Control",  header=#6b7280, bg=#0f1520

_TABLE_GROUPS maps 24 table names to their group.
_TABLE_DESCRIPTIONS maps 15 entity table names to one-line human descriptions.
_FOREIGN_KEYS maps table names to list of {col, ref_table, ref_col} dicts
  for logical FK relationships (FK enforcement is OFF in SQLite).

extract_from_db(db_path) → dict:
  Read sqlite_master, PRAGMA table_info per table, build structured schema dict:
  {"name":..., "version":"1.0", "description":..., "exported_at":...,
   "tables":[{"name","group","description","columns":[{"name","type","pk","nullable","fk"}]}]}

to_json(schema) → str: json.dumps(schema, indent=2)

to_sql_ddl(db_path) → str: SELECT sql FROM sqlite_master, format with comments

to_graphviz_dot(schema, groups: set[str], keys_only=True) → str:
  Graphviz DOT with HTML table labels. One subgraph cluster per group.
  Each table: header row (group colour), then PK rows (gold badge) and FK rows
  (green badge). keys_only=True appends "N more columns" stub row.
  FK edges between visible tables only. Edge tooltip = column name.

════════════════════════════════════════════════════════════════════════════════
15. VALIDATORS  (validators/)
════════════════════════════════════════════════════════════════════════════════

── results.py ──────────────────────────────────────────────────────────────────

class Severity(str, Enum): ERROR = "ERROR"; WARNING = "WARNING"

@dataclass class Check:
  name: str          # unique machine key
  category: str      # "referential" | "temporal" | "state"
  severity: Severity
  table: str
  description: str

@dataclass class Finding:
  check: Check
  count: int
  examples: list     # up to 5 example PKs

print_report(checks, findings, verbose=False) — console tabular output

── validators/__init__.py ──────────────────────────────────────────────────────

ALL_CHECKS = referential.CHECKS + temporal.CHECKS + state.CHECKS

def run_all(db_path) → tuple[list[Check], list[Finding]]:
  Open read-only connection (file:path?mode=ro URI)
  Run referential.run, temporal.run, state.run
  Return (ALL_CHECKS, combined findings)

── referential.py ──────────────────────────────────────────────────────────────

CHECKS: list of Check with category="referential", severity=ERROR.
Check each entity table for orphaned foreign keys:
  PROVIDER.client_id → CLIENT.client_id
  ACCOUNT.provider_id → PROVIDER.provider_id
  CUSTOMER.account_id → ACCOUNT.account_id
  CARD.account_id → ACCOUNT.account_id
  "TRANSACTION".account_id → ACCOUNT.account_id  (note: quote TRANSACTION)
  DISPUTE.transaction_id → "TRANSACTION".transaction_id
  FRAUD_ALERT.transaction_id → "TRANSACTION".transaction_id
  CHARGEBACK.dispute_id → DISPUTE.dispute_id
  SCORE_RECORD.account_id → ACCOUNT.account_id
  COLLECTION_CASE.account_id → ACCOUNT.account_id
  AUTHORIZATION.account_id → ACCOUNT.account_id

run(conn) → list[Finding]: execute each check SQL, return Finding for any
  with count > 0 (up to 5 example PKs from the violating rows).

── temporal.py ──────────────────────────────────────────────────────────────────

CHECKS: category="temporal". Check date ordering:
  ERROR: "TRANSACTION".transaction_date >= ACCOUNT.open_date
  ERROR: DISPUTE.dispute_opened_date >= ACCOUNT.open_date
  ERROR: FRAUD_ALERT.alert_timestamp >= ACCOUNT.open_date (first 10 chars)
  ERROR: SCORE_RECORD.score_date >= ACCOUNT.open_date
  WARNING: AUTHORIZATION.auth_timestamp (first 10 chars) >= ACCOUNT.open_date

── state.py ─────────────────────────────────────────────────────────────────────

CHECKS: category="state". Check valid status values and business logic:
  ERROR: ACCOUNT.account_status IN known values
  ERROR: CARD.card_status IN known values
  ERROR: DISPUTE.dispute_status IN known values
  WARNING: COLLECTION_CASE.amount_past_due >= 0
  WARNING: ACCOUNT.current_balance <= ACCOUNT.credit_limit * 1.1 (allow 10% over)
  WARNING: ACCOUNT.available_credit >= -100 (small negative allowed for rounding)

════════════════════════════════════════════════════════════════════════════════
16. STREAMLIT UI  (ui/app.py)
════════════════════════════════════════════════════════════════════════════════

⚠ ALL SQL queries in the UI must quote reserved words:
  f'SELECT COUNT(*) AS n FROM "{tbl}"'   — not f'SELECT COUNT(*) AS n FROM {tbl}'
  The _query helper silently returns empty DataFrame on exception, hiding bugs.

Page config: layout="wide", page_title="TDGen-Temporal"
ROOT = two levels up from ui/app.py. DEFAULT_DB = ROOT/output/state.db.
DEFAULT_CONFIG = ROOT/config/scenario.yaml.

@st.cache_data(ttl=8)
def _query(db_str, sql, params=()) → pd.DataFrame:
  sqlite3.connect; pd.read_sql_query; return empty df on exception.

def table_count(tbl) → int: query f'SELECT COUNT(*) AS n FROM "{tbl}"'

def db_ready(p) → bool: p.exists() and p.stat().st_size > 100

── Sidebar ──────────────────────────────────────────────────────────────────────

If db_ready: show metrics — Simulation date, Days simulated, Entities
  (ACCOUNT count), Events (TRANSACTION count — must quote), Open cases (DISPUTE count).
Refresh button: st.cache_data.clear() + st.rerun().

── Full-width HTML banner ────────────────────────────────────────────────────────

Dark navy gradient (linear-gradient 135deg #0a1628 → #0f2d4a → #0a1e35),
border #1e3a5f, border-radius 12px.
Title: "🗄️ TDGen‑Temporal" (32px bold).
Strapline: "SCHEMA‑DRIVEN SYNTHETIC DATA GENERATOR · TEMPORAL SIMULATION PLATFORM"
  (monospace, #64748b).
Three badges (monospace 12px):
  Blue (#60a5fa, bg #0f1f3d): "ENTITIES · RELATIONSHIPS · ATTRIBUTES"
  Green (#34d399, bg #0a2a1e): "EVENTS · TRANSITIONS · HISTORY"
  Red (#f87171, bg #2a0f0f): "STATES · LIFECYCLE · VALIDATION"
Decorative monospace text top-right (colour #1e3a5f, unselectable).

── Tabs ─────────────────────────────────────────────────────────────────────────

tab_cfg, tab_schema, tab_dash, tab_exp, tab_val = st.tabs([
  "⚙️  Control Panel", "🗂️  Schema", "📊  Dashboard",
  "🔍  Data Explorer", "✅  Validation"
])

TAB 1 — Control Panel:
  Tool description paragraph (domain-agnostic, no credit-card language).
  "How to get started" expander (expanded when db not ready): 4-step guide.
  Two columns [3,2]:
    Left: st.text_area for scenario.yaml, Save config button (validate YAML first).
    Right: four st.expander sections:
      🚀 Initialise: date_input + Initialise button → run_cli("init",...)
      ⏩ Advance: number_input(days) + Advance button → run_cli("advance",...)
      📅 Backfill: two date_inputs + Run backfill → run_cli("backfill",...)
      🗑️ Reset: warning + Reset button → db_path.unlink(missing_ok=True)
  Run log table (last 50 runs, DESC): columns = run_date, run_mode,
    accounts_processed, txns_inserted, auths_inserted, scores_inserted,
    disputes_inserted, fraud_inserted, accounts_updated, duration_s.
    Use json_extract(inserts_json,'$.TRANSACTION') etc. for counts.

TAB 2 — Schema:
  Source: st.session_state["loaded_schema"] takes priority over live DB extract.
  Group checkboxes (one per group, default on: core/transaction/risk).
  keys_only toggle.
  st.graphviz_chart(dot_src, use_container_width=True) — zoom/pan built in.
  Three columns: Export JSON (download_button), Export SQL DDL (download_button),
    Load schema (file_uploader for JSON, validate "tables" key, store in session_state).
  Per-table expandable reference: column name, type, constraints (PK/FK/NOT NULL).

TAB 3 — Dashboard:
  KPI row (7 cols): Accounts, Customers, Cards, Transactions, Disputes,
    Fraud alerts, Chargebacks. Use table_count with quoted names.

  Row 1 (2 cols):
    Daily transaction & auth volume: line chart from run_log
      json_extract(inserts_json,'$.TRANSACTION') and '$.AUTHORIZATION' by run_date.
    Account status distribution: pie chart from
      SELECT account_status, COUNT(*) FROM ACCOUNT GROUP BY account_status.

  Row 2 (2 cols):
    Dispute status: bar chart from DISPUTE GROUP BY dispute_status.
    Fraud alert status: bar chart from FRAUD_ALERT GROUP BY alert_status.

  Row 3 (2 cols):
    Collection cases by bucket: bar chart from COLLECTION_CASE
      GROUP BY delinquency_bucket, ordered B1→B2→B3→B4→CHARGEOFF→RESOLVED.
    Daily risk event volume: area chart from run_log json_extract for
      DISPUTE, FRAUD_ALERT, CHARGEBACK, COLLECTION_CASE by run_date.

  Row 4 (2 cols):
    Credit score band distribution: bar chart from SCORE_RECORD
      GROUP BY score_band. If empty, show st.info explaining score_refresh_day
      and that the user needs to advance past that day of the month.
    Transaction type breakdown: bar chart from "TRANSACTION"
      GROUP BY transaction_type (must quote TRANSACTION).

TAB 4 — Data Explorer:
  TABLES dict maps friendly name → (table_name, pk_col) for 16 tables including
    "Transactions": ("TRANSACTION","transaction_id") — the UI uses:
      full_df = query(f'SELECT * FROM "{tbl_name}"')  ← always quoted
  Search box filters all columns as strings.
  Rows-per-page selector [25,50,100,250].
  Pagination via st.number_input.
  st.dataframe with on_select="rerun", selection_mode="single-row".
  Selected row → show field/value detail table.
  Account drill-down: expandable sections for Transactions, Cards, Disputes,
    Fraud Alerts, Collection Cases, Score Records (all with quoted table names).
  Transaction drill-down: Disputes and Fraud Alerts for that transaction_id.
  Dispute drill-down: Chargebacks for that dispute_id.
  Export CSV button for filtered rows.

TAB 5 — Validation:
  "Errors only" checkbox, "Hide passing checks" checkbox, Run button.
  On run: call validators.run_all(db_path), store in st.session_state.
  Show banner: green (all pass) / yellow (warnings) / red (errors).
  KPI row: Total checks, Passed, Errors, Warnings, Violating rows.
  One st.expander per category (referential/temporal/state), expanded if failures.
  Styled dataframe: red rows for ERROR failures, yellow for WARNING failures.

════════════════════════════════════════════════════════════════════════════════
17. TESTS  (tests/test_smoke.py)
════════════════════════════════════════════════════════════════════════════════

Class TestFieldGenerators:
  test_make_faker_returns_faker
  test_generate_card_number_luhn  (Luhn algorithm check)
  test_expiry_date_is_future

Class TestConfig:
  test_config_loads  (simulation/rates/lifecycle keys present)
  test_config_required_keys
  test_collection_buckets_ordered

Class TestStateMachines:
  test_account_state_machine_imports
  test_dispute_state_machine_imports
  test_fraud_alert_state_machine_imports

Class TestStateStore:
  test_state_store_creates_db  (tmp_path)
  test_count_after_migrations  (0 rows after DDL, before seeding)

════════════════════════════════════════════════════════════════════════════════
18. CI  (.github/workflows/ci.yml)
════════════════════════════════════════════════════════════════════════════════

Trigger: push to any branch, PR to main. Cancel in-progress on same ref.

Jobs (all ubuntu-latest, Python 3.11 unless stated):
1. lint: pip install ruff; ruff format --check .; ruff check .
2. typecheck: pip install -r requirements.txt mypy types-PyYAML;
     mypy tdgen_temporal --ignore-missing-imports
3. test: matrix [3.10, 3.11, 3.12];
     pip install -r requirements.txt pytest pytest-cov;
     pytest tests/ --cov=tdgen_temporal --cov-report=xml -v;
     upload coverage.xml artifact (3.11 only)
4. smoke (needs: lint, test):
     init --date 2024-01-01; advance --days 7; status;
     validate --errors-only; upload output/state.db artifact (3 days retention)

════════════════════════════════════════════════════════════════════════════════
19. KNOWN GOTCHAS
════════════════════════════════════════════════════════════════════════════════

1. TRANSACTION is a SQLite reserved keyword. ALWAYS quote it everywhere:
   DDL: CREATE TABLE IF NOT EXISTS "TRANSACTION" (...)
   DML: SELECT * FROM "TRANSACTION"; INSERT INTO "TRANSACTION" ...
   The _query() helper in app.py silently swallows OperationalError and returns
   an empty DataFrame — unquoted TRANSACTION queries fail invisibly.

2. score_refresh_day = 5 (not 1). If set to 1 and init is on 2024-01-01,
   advancing 30 days covers Jan 2–31 and day 1 never fires. Day 5 ensures
   scores appear naturally in any standard 30-day demo run.

3. open_date temporal guard: accounts seeded with random past open_dates may
   have open_date after the sim start date. Always skip accounts and score
   records where run_date < open_date.

4. _is_fraud key: strip any dict key starting with "_" before writing to
   the database (bulk_insert) and before writing delta files. Never persist
   internal simulation keys.

5. Ruff formatting: run `ruff format .` before every commit. CI runs
   `ruff format --check` and fails if any file would be reformatted.
```

---

## Verifying the rebuild

Once Claude Code finishes, confirm everything works:

```powershell
.\.venv\Scripts\Activate.ps1

pytest tests/ -v

python -m tdgen_temporal.cli init --date 2024-01-01
python -m tdgen_temporal.cli advance --days 30
python -m tdgen_temporal.cli status
python -m tdgen_temporal.cli validate --errors-only
```

Expected: all tests pass; `status` shows 30 runs completed; `validate`
reports 0 errors. Then launch the UI and check all five tabs load correctly
and the Data Explorer shows transactions (select "Transactions" from the
Entity dropdown).

---

## Export the demo schema file

After the simulation is running, generate the bundled demo schema:

```powershell
python -c "
from pathlib import Path
from tdgen_temporal.schema import extract_from_db, to_json
schema = extract_from_db(Path('output/state.db'))
Path('config/tsys_ts2_schema.json').write_text(to_json(schema), encoding='utf-8')
print('Exported', len(schema['tables']), 'tables to config/tsys_ts2_schema.json')
"
```

Load `config/tsys_ts2_schema.json` in the Schema tab to demonstrate the
schema ingestion feature without needing a live database.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| ER diagram blank / `dot: command not found` | Reinstall Graphviz and tick **Add to PATH**; restart terminal; verify with `dot -V` |
| `ModuleNotFoundError: No module named 'tdgen_temporal'` | Activate the venv first: `.\.venv\Scripts\Activate.ps1` |
| Transactions show 0 in Explorer | Select **Transactions** from the Entity dropdown — it defaults to Accounts |
| Score records empty | Advance the simulation past day 5 of a month (`score_refresh_day=5`); scores only generate once per month |
| Stale data after Reset | Press F5 in the browser after resetting — Streamlit caches state within a session |
| `pandera` / `openpyxl` not found | Install from `requirements.txt`: `pip install -r requirements.txt` |
| CI lint fails | Run `ruff format .` locally before pushing |
