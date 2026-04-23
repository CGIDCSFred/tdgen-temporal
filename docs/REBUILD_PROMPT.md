# TDGEN-TEMPORAL — Rebuild Prompt

Build a Python CLI application called **tdgen-temporal** that generates
temporally-consistent synthetic test data for a TSYS TS2 credit-card processing
system.  The simulator maintains a SQLite state database and emits daily delta
files (CSV + JSON) for every insert and update.

---

## 1. Technology stack

- Python 3.11+
- SQLite (WAL mode) via the standard `sqlite3` module
- `faker` (locale `en_CA`) for synthetic PII/addresses
- `pandas` for CSV output
- `PyYAML` for config
- No ORM — raw SQL only

---

## 2. Project layout

```
tdgen_temporal/
    cli.py
    db/
        migrations.py       # DDL only
        state_store.py      # all DB access
    engine/
        init_runner.py
        daily_runner.py
        backfill_runner.py
    generators/
        field_generators.py
        ref_tables.py
        seed.py
        transaction.py
        statement.py
    state_machines/
        base.py
        account.py
        card.py
        dispute.py
        fraud_alert.py
        chargeback.py
        collection_case.py
    output/
        delta_writer.py
        snapshot_writer.py  # unused stub
config/
    scenario.yaml
tests/
```

---

## 3. CLI (cli.py)

Four sub-commands:

| Command | Purpose |
|---|---|
| `init --db PATH --date YYYY-MM-DD --config PATH` | Delete existing DB, create all tables, seed Day 0 population |
| `advance --db PATH --days N --config PATH --output PATH` | Continue simulation N days forward from the DB's current date |
| `backfill --db PATH --from YYYY-MM-DD --to YYYY-MM-DD --config PATH --output PATH` | Run daily simulation over an explicit date range |
| `status --db PATH` | Print current run date, total runs, and row counts for all entity tables |

Defaults: `--db output/state.db`, `--config config/scenario.yaml`, `--output output`

---

## 4. Config file (config/scenario.yaml)

```yaml
simulation:
  seed: 42
  locale: en_CA
  initial_population:
    accounts: 500
    merchants: 200
    clients: 3
    providers: 10
    products: 20

rates:
  daily_new_accounts: 0.005
  transactions_per_account_per_day_mean: 1.8
  transactions_per_account_per_day_stddev: 1.2
  payment_probability: 0.65
  fraud_rate: 0.002
  dispute_rate: 0.003
  chargeback_rate: 0.40
  delinquency_rate: 0.04
  chargeoff_rate: 0.001
  score_refresh_day: 1
  fraud_confirmation_rate: 0.30
  dispute_withdrawal_rate: 0.05

lifecycle:
  dispute_investigating_days: 7
  dispute_resolution_days: 30
  fraud_alert_review_days: 2
  chargeback_representment_days: 10
  collection_bucket_thresholds:
    B1: 30
    B2: 60
    B3: 90
    B4: 120
    CHARGEOFF: 180

output:
  formats:
    - csv
    - json
  write_snapshots: false
```

---

## 5. Database schema (migrations.py)

Use `CREATE TABLE IF NOT EXISTS`.  Enable WAL (`PRAGMA journal_mode=WAL`) and
disable FK enforcement (`PRAGMA foreign_keys=OFF`) on every connection.

### Simulation-control tables

```sql
simulation_meta (id INTEGER PRIMARY KEY CHECK(id=1), current_run_date TEXT,
    last_run_id TEXT, total_runs INTEGER DEFAULT 0,
    created_at TEXT, updated_at TEXT)

run_log (run_id TEXT PRIMARY KEY, run_date TEXT, run_mode TEXT,
    accounts_processed INTEGER, inserts_json TEXT, updates_json TEXT,
    duration_seconds REAL, created_at TEXT)

pk_sequences (table_name TEXT PRIMARY KEY, next_id INTEGER DEFAULT 1)
```

### REF tables

```sql
REF_ACCOUNT_STATUS  (status_code PK, status_description, is_active INT, weight_pct REAL)
REF_CARD_STATUS     (status_code PK, status_description, is_usable INT, weight_pct REAL)
REF_CARD_BRAND      (brand_code PK, brand_name, bin_prefix, weight_pct REAL)
REF_TRANSACTION_TYPE(type_code PK, type_description, is_debit INT, weight_pct REAL)
REF_DISPUTE_TYPE    (type_code PK, type_description, weight_pct REAL)
REF_CHARGEBACK_STAGE(stage_code PK, stage_description, stage_order INT)
REF_FRAUD_ALERT_TYPE(alert_type_code PK, alert_type_description, detection_system, weight_pct REAL)
REF_AUTH_RESPONSE   (response_code PK, response_description, is_approved INT, weight_pct REAL)
REF_POS_ENTRY_MODE  (entry_mode_code PK, entry_mode_description, is_card_present INT, weight_pct REAL)
REF_CHANNEL         (channel_code PK, channel_description, weight_pct REAL)
REF_DELINQUENCY_BUCKET(bucket_code PK, bucket_description, min_days INT, max_days INT, bucket_order INT)
REF_CURRENCY        (currency_code PK, currency_name, symbol, decimal_places INT)
REF_COUNTRY         (country_code PK, country_name, region)
REF_MCC             (mcc_code PK, mcc_description, mcc_group, weight_pct REAL)
REF_STATUS_REASON   (reason_code PK, reason_description, applies_to_status)
REF_SENSITIVITY_LEVEL(sensitivity_code PK, sensitivity_description, generation_rule, applicable_regulations)
```

### Entity tables

```sql
CLIENT (client_id PK, client_name, bin_range, base_currency, region,
        processing_mode, association_id)

PROVIDER (provider_id PK, client_id, provider_name, portfolio_type,
          reporting_group, status)

PRODUCT_DEFINITION (tsys_product_code, client_product_code, client_id,
    product_description, card_brand, default_credit_limit INT,
    annual_fee REAL, apr_purchase REAL, apr_cash_advance REAL,
    grace_period_days INT, rewards_program, billing_cycle_type,
    fee_schedule_id, PRIMARY KEY (tsys_product_code, client_product_code))

MERCHANT (merchant_id PK, merchant_name, dba_name, mcc_code, terminal_id,
    acquirer_id, city, state_province, country_code, postal_zip,
    merchant_url, risk_tier, is_online INT)

ACCOUNT (account_id PK, provider_id, tsys_product_code, client_product_code,
    account_number, credit_limit REAL, current_balance REAL DEFAULT 0,
    available_credit REAL, cash_advance_limit REAL, cash_advance_balance REAL DEFAULT 0,
    payment_due_amount REAL DEFAULT 0, payment_due_date TEXT, last_payment_date TEXT,
    last_payment_amount REAL, open_date TEXT, closed_date TEXT,
    account_status TEXT DEFAULT 'ACTIVE', status_reason TEXT,
    currency_code TEXT DEFAULT 'CAD', cycle_day INT, days_delinquent INT DEFAULT 0,
    block_code TEXT, risk_score REAL, last_monetary_date TEXT, last_non_monetary_date TEXT)

CUSTOMER (customer_id PK, account_id, first_name, last_name, name_line_1,
    date_of_birth, ssn_sin, address_line_1, address_line_2, city,
    state_province, postal_zip, country_code, phone_home, phone_work,
    phone_mobile, email, language_preference, relationship_type,
    id_type, id_number, employer_name, annual_income REAL)

CARD (card_id PK, account_id, card_number, card_sequence_number INT,
    cardholder_name, expiry_date, issue_date,
    card_status TEXT DEFAULT 'ACTIVE', card_type, chip_enabled INT,
    contactless_enabled INT, pin_offset, card_design_id,
    digital_wallet_token, token_requestor, last_used_date)

AUTHORIZATION (auth_id PK, account_id, card_id, merchant_id,
    auth_timestamp, auth_amount REAL, currency_code, auth_response_code,
    auth_approval_code, decline_reason, pos_entry_mode, pos_condition_code,
    channel, terminal_id, network, avs_response, cvv_response,
    risk_score REAL, three_ds_result, ip_address, device_fingerprint,
    available_after_auth REAL, auth_type, auth_hold_days INT)

TRANSACTION (transaction_id PK, account_id, card_id, merchant_id, auth_id,
    transaction_date, post_date, transaction_amount REAL, billing_amount REAL,
    transaction_currency, conversion_rate REAL, transaction_type,
    transaction_status, description, mcc_code, pos_entry_mode, channel,
    reference_number, batch_id, is_recurring INT, is_international INT,
    interchange_qualifier, interchange_fee REAL, statement_id INT)

STATEMENT (statement_id PK, account_id, statement_date, payment_due_date,
    opening_balance REAL, closing_balance REAL, total_credits REAL,
    total_debits REAL, minimum_payment REAL, interest_charged REAL,
    fees_charged REAL, transaction_count INT, available_credit REAL, cycle_id)

DISPUTE (dispute_id PK, transaction_id, account_id, dispute_opened_date,
    dispute_type, dispute_status TEXT DEFAULT 'OPEN', dispute_reason_code,
    disputed_amount REAL, cardholder_explanation, assigned_analyst,
    response_due_date, resolution, resolved_date)

CHARGEBACK (chargeback_id PK, dispute_id, transaction_id, chargeback_date,
    chargeback_amount REAL, chargeback_reason_code, chargeback_stage,
    representment_status, representment_date, recovered_amount REAL,
    network_case_id)

FRAUD_ALERT (alert_id PK, account_id, transaction_id, alert_timestamp,
    alert_source, alert_type, risk_score INT,
    alert_status TEXT DEFAULT 'OPEN', action_taken, analyst_id,
    resolved_date, case_link)

SCORE_RECORD (score_id PK, account_id, score_date, score_type,
    score_value INT, score_band, model_version, decision,
    action_code, result_code)

COLLECTION_CASE (case_id PK, account_id, case_opened_date, delinquency_bucket,
    amount_past_due REAL, total_owed REAL,
    case_status TEXT DEFAULT 'ACTIVE', assigned_collector, contact_method,
    last_contact_date, next_action_date, next_action,
    recovered_amount REAL DEFAULT 0, chargeoff_reason, chargeoff_date)
```

### Temporal state tables (simulation-internal, not exported)

```sql
account_temporal_state (account_id PK, current_state TEXT DEFAULT 'ACTIVE',
    days_delinquent INT DEFAULT 0, consecutive_missed_payments INT DEFAULT 0,
    last_payment_date, last_statement_date, payment_due_date,
    cycle_day INT, as_of_date TEXT)

card_temporal_state (card_id PK, current_state TEXT DEFAULT 'ACTIVE',
    days_to_expiry INT, replacement_issued INT DEFAULT 0, as_of_date TEXT)

dispute_temporal_state (dispute_id PK, current_state TEXT DEFAULT 'OPEN',
    days_open INT DEFAULT 0, resolved_date, as_of_date TEXT)

fraud_alert_temporal_state (alert_id PK, current_state TEXT DEFAULT 'OPEN',
    days_open INT DEFAULT 0, reviewed_date, as_of_date TEXT)

chargeback_temporal_state (chargeback_id PK,
    current_state TEXT DEFAULT 'FIRST_CHARGEBACK',
    days_open INT DEFAULT 0, as_of_date TEXT)

collection_case_temporal_state (case_id PK,
    current_state TEXT DEFAULT 'ACTIVE',
    current_bucket TEXT DEFAULT 'B1',
    days_in_bucket INT DEFAULT 0, as_of_date TEXT)
```

---

## 6. Reference data (ref_tables.py)

Seed exactly these rows at `init` time using plain `INSERT INTO`:

**REF_ACCOUNT_STATUS**: ACTIVE (w=70, active), DELINQUENT (w=12, active),
SUSPENDED (w=5, inactive), CHARGEOFF (w=8, inactive), CLOSED (w=5, inactive)

**REF_CARD_STATUS**: ACTIVE (w=75, usable), BLOCKED (w=5), EXPIRED (w=8),
CANCELLED (w=7), LOST (w=3), STOLEN (w=2)

**REF_CARD_BRAND**: Visa (bin=4, w=45), Mastercard (bin=5, w=40),
Amex (bin=37, w=10), Interac (bin=63, w=5)

**REF_TRANSACTION_TYPE**: PURCHASE (debit, w=60), CASH_ADVANCE (debit, w=5),
PAYMENT (credit, w=20), REFUND (credit, w=8), FEE (debit, w=3),
INTEREST (debit, w=3), REVERSAL (credit, w=1)

**REF_DISPUTE_TYPE**: FRAUD (w=40), NOT_RECEIVED (w=20), DUPLICATE (w=15),
WRONG_AMOUNT (w=15), QUALITY (w=7), SUBSCRIPTION (w=3)

**REF_CHARGEBACK_STAGE**: FIRST_CHARGEBACK (order=1), REPRESENTMENT (order=2),
PRE_ARBITRATION (order=3), ARBITRATION (order=4)

**REF_FRAUD_ALERT_TYPE**: VELOCITY (rule_engine, w=30), GEO_ANOMALY (RTD, w=20),
CARD_TESTING (rule_engine, w=15), HIGH_RISK_MCC (rule_engine, w=10),
LARGE_TXN (CardGuard, w=15), 3DS_FAIL (3DS, w=5), SCORE_DROP (scoring, w=5)

**REF_AUTH_RESPONSE**: 00=Approved (approved, w=80), 05=Do not honour (w=5),
14=Invalid card (w=2), 51=Insufficient funds (w=6), 54=Expired (w=3),
57=Not permitted (w=2), 61=Exceeds limit (w=1), 78=No account (w=1)

**REF_POS_ENTRY_MODE**: chip (present, w=45), contactless (present, w=30),
swipe (present, w=5), ecommerce (CNP, w=15), manual (CNP, w=3), atm (present, w=2)

**REF_CHANNEL**: in_store (w=45), online (w=35), mobile (w=15), telephone (w=3), atm (w=2)

**REF_DELINQUENCY_BUCKET**: CURRENT (0–0, order=0), B1 (1–30, order=1),
B2 (31–60, order=2), B3 (61–90, order=3), B4 (91–120, order=4),
B5 (121–999, order=5)

**REF_CURRENCY**: CAD, USD, EUR, GBP

**REF_COUNTRY**: CA (NA), US (NA), GB (EU)

**REF_STATUS_REASON**: PAYMENT_RECEIVED→ACTIVE, CREDIT_APPROVED→ACTIVE,
MISSED_PAYMENT→DELINQUENT, FRAUD_CONFIRMED→SUSPENDED, OVERLIMIT→SUSPENDED,
CHARGEOFF_BAD_DEBT→CHARGEOFF, CUSTOMER_REQUEST→CLOSED, BANK_DECISION→CLOSED

**REF_SENSITIVITY_LEVEL**: PII (faker_approved, PIPEDA/GDPR),
PCI (luhn_valid, PCI-DSS), PHI (faker_approved, PHIPA), NONE (any)

---

## 7. State store (db/state_store.py)

Wrap a single `sqlite3.Connection` (row_factory = sqlite3.Row).  Provide:

- `next_id(table_name)` — atomic increment of `pk_sequences`
- `init_sequence(table_name, start)`
- `bulk_insert(table, rows)` — plain `INSERT INTO`
- `bulk_upsert(table, rows)` — `INSERT OR REPLACE`
- `update_row(table, pk_col, pk_val, updates)`
- `get_all_accounts()` / `get_active_accounts()` — JOIN with `account_temporal_state`
- `get_open_disputes()` / `get_open_fraud_alerts()` / `get_open_chargebacks()` / `get_open_collection_cases()` — JOIN with respective temporal state tables
- `get_active_cards_for_account(account_id)`
- `get_all_merchants()`
- `set_simulation_meta(run_date, run_id, total_runs)` — UPSERT into `simulation_meta`
- `get_simulation_meta()` / `get_current_run_date()`
- `record_run(run_id, run_date, run_mode, accounts_processed, inserts, updates, duration)` — insert into `run_log`

---

## 8. Field generators (generators/field_generators.py)

- `make_faker(seed)` — Faker("en_CA") with optional seed
- `generate_card_number()` — Luhn-valid 16-digit number, prefix 4/51–55
- `generate_sin()` — synthetic Canadian SIN format `NNN-NNN-NNN`
- `generate_account_number()` — 16 random digits
- `score_band(score)` — maps int to excellent/good/fair/medium/low/very_high_risk
- `delinquency_bucket(days)` — maps int to CURRENT/B1/B2/B3/B4/B5+
- `expiry_date_from_today(years_ahead=3)` — last day of a random future month
- `random_past_date(fake, years_back=5)`
- `random_narrative(fake)` — picks from a list of realistic Canadian merchant names
- `random_decimal(mn, mx, decimals=2)`

MCC codes in use: 5411, 5812, 5541, 5310, 5912, 5999, 4111, 4121, 5734, 7011,
5045, 5065, 5621, 5651

---

## 9. Day 0 seeding (generators/seed.py)

`seed_clients(fake, n, store)` — faker company name, BIN range (4-5 prefix), CAD,
region (NA/EMEA/APAC/LATAM), processing_mode (batch/online/hybrid)

`seed_providers(fake, clients, n, store)` — link to random client, portfolio_type
(retail/small_business/consumer/commercial), status 85% active

`seed_products(fake, clients, n, store)` — 20 products, APR 9.99–29.99%,
credit limit 500–50,000, annual fee from {0,39,79,99,120,150},
rewards (points/miles/cashback/None)

`seed_merchants(fake, n, store)` — 200 merchants, 70% CA / 30% US,
risk_tier (low 65%, medium 25%, high 10%), is_online 30%

`seed_accounts(fake, providers, products, n, run_date, store)` — returns
(accounts, customers, cards, account_temporal_states).  Status distribution:
ACTIVE ×4 (70%), DELINQUENT (8%), CHARGEOFF (5%), CLOSED (2%), with remainder
split across duplicates.  Each account gets exactly one CUSTOMER and one CARD.
DELINQUENT accounts start with days_delinquent 1–120; CHARGEOFF 180–360.
Cards for ACTIVE/DELINQUENT = ACTIVE; CHARGEOFF = BLOCKED; CLOSED = CANCELLED.

---

## 10. State machines (state_machines/)

### Base classes (base.py)

```python
@dataclass
class SideEffect:
    table: str; pk_col: str; pk_val: Any; updates: dict

@dataclass
class AdvanceResult:
    updated_row: dict
    changed_fields: list[str]
    side_effects: list[SideEffect] = field(default_factory=list)
    new_rows: dict[str, list[dict]] = field(default_factory=dict)

class StateMachine(ABC):
    def advance(self, entity_row, run_date, config, rng) -> AdvanceResult: ...
```

### Account (ACTIVE → DELINQUENT → CHARGEOFF → CLOSED)

Each day, if `run_date >= payment_due_date`:
- Payment received (65% probability or last_payment_date >= due_date−1): reduce
  balance by max(balance×5%, payment_due_amount), reset delinquency if balance
  < payment_due_amount, advance due_date +30 days
- Missed payment: days_delinquent++, consecutive_missed++, ACTIVE→DELINQUENT
- If DELINQUENT and days_delinquent >= chargeoff_threshold (default 180) and
  random < chargeoff_rate: transition to CHARGEOFF, emit SideEffect to block card

### Card (ACTIVE → EXPIRED; ACTIVE → BLOCKED)

Each day:
- If expiry_date <= run_date and state==ACTIVE: transition to EXPIRED, set
  replacement_issued=1, emit new_rows["CARD_REPLACEMENT"] with account_id,
  cardholder_name, replaced_card_id (DailyRunner creates the new CARD row)
- Card can also be BLOCKED via SideEffect from Account or FraudAlert machines

### Dispute (OPEN → INVESTIGATING → RESOLVED → CLOSED | WITHDRAWN)

Each day increment days_open, then:
- OPEN: if days_open ≤ 3 and random < withdrawal_rate(5%) → WITHDRAWN;
  else 90% chance → INVESTIGATING
- INVESTIGATING: if days_open ≥ resolution_days(30) → force CLOSED/WRITTEN_OFF;
  if days_open ≥ investigating_days(7) and random < 70% → RESOLVED with
  resolution=APPROVED/DENIED/PARTIAL; if APPROVED/PARTIAL and random < chargeback_rate
  emit new_rows["CHARGEBACK"]
- RESOLVED: if (run_date − resolved_date) ≥ 5 days → CLOSED

### Fraud Alert (OPEN → UNDER_REVIEW → CONFIRMED | FALSE_POSITIVE → CLOSED)

Each day increment days_open:
- OPEN: if days_open ≥ 7 → auto-expire to CLOSED; else 95% → UNDER_REVIEW
- UNDER_REVIEW: if days_open ≥ review_days(2): 30% → CONFIRMED (action=block_card,
  emit SideEffect to block card); 70% → FALSE_POSITIVE (action=none)
- CONFIRMED: if (run_date − resolved_date) ≥ 30 days → CLOSED

### Chargeback (FIRST_CHARGEBACK → REPRESENTMENT → PRE_ARBITRATION → WON | LOST)

Each day increment days_open:
- FIRST_CHARGEBACK + days_open ≥ rep_days(10): 60% → REPRESENTMENT,
  40% → WON (recovered_amount = chargeback_amount)
- REPRESENTMENT + days_open ≥ rep_days×2: 50% WON / 30% LOST / 20% PRE_ARBITRATION
- PRE_ARBITRATION + days_open ≥ rep_days×3: 40% WON / 60% LOST

### Collection Case (ACTIVE → AGENCY_REFERRAL | PROMISE_TO_PAY → CHARGEOFF → CLOSED)

Each day increment days_in_bucket, recalculate bucket from account days_delinquent:
- ACTIVE: if days_del ≥ 180 → CHARGEOFF; if days_del ≥ 120 and random < 15% →
  AGENCY_REFERRAL; else random < 5% → PROMISE_TO_PAY
- PROMISE_TO_PAY: if days_del ≥ 180 → CHARGEOFF (reason=broken_promise)

---

## 11. Daily runner (engine/daily_runner.py)

`DailyRunner.run(run_date)` executes these steps in order:

1. Load all active accounts (JOIN account_temporal_state)
2. Run AccountStateMachine on each → collect ACCOUNT updates + SideEffects
3. Load all cards; run CardStateMachine → collect CARD updates; for any
   CARD_REPLACEMENT new_rows create a new CARD row (sequence_number = old+1,
   expiry = 3+ years, status = ACTIVE)
4. Emit SCORE_RECORD for accounts where `run_date.day == score_refresh_day`
5. Generate TRANSACTION + AUTHORIZATION rows via `generate_daily_transactions()`
6. Emit STATEMENT rows for accounts where `run_date.day == account.cycle_day`
7. Generate new DISPUTE rows via `generate_new_disputes()`
8. Generate new FRAUD_ALERT rows via `generate_new_fraud_alerts()`
9. Run DisputeStateMachine on all open disputes
10. Run FraudAlertStateMachine on all open fraud alerts
11. Run ChargebackStateMachine on all open chargebacks
12. Run CollectionCaseStateMachine on all open collection cases
13. Apply SideEffects (card blocks, balance adjustments)
14. Persist everything to SQLite (bulk_insert new rows, update_row changed rows)
15. Write delta files via DeltaWriter
16. Update simulation_meta and run_log

---

## 12. Transaction generation (generators/transaction.py)

Per active account per day, draw n_tx ~ max(0, Gauss(mean=1.8, std=1.2)).

Transaction types (by weight): PURCHASE 60%, PAYMENT 20%, REFUND 8%, FEE 4%,
INTEREST 3%, CASH_ADVANCE 5%.  CLOSED and CHARGEOFF accounts are skipped.

Amount logic:
- PAYMENT: max(10, payment_due_amount × Uniform(0.5, 2.0))
- FEE/INTEREST: Uniform(2, 50)
- CASH_ADVANCE: Uniform(20, 500)
- else: Uniform(1, 800)

Authorization: 92% approval rate for non-fraud; fraud transactions are declined.
Declined auth codes: 05/51/54/57.  Post date = transaction_date + 0–2 days.

Fraud flag: `_is_fraud = (type==PURCHASE and random < fraud_rate)` — stored as
internal key, stripped from output files.

Disputes: for each PURCHASE/CASH_ADVANCE transaction, random < dispute_rate(0.3%)
→ open a DISPUTE.  Type = FRAUD if transaction was flagged, else random.

Fraud alerts: generated for every `_is_fraud` transaction plus random < 0.1% of
clean transactions.

---

## 13. Output (output/delta_writer.py)

```
output/deltas/YYYY-MM-DD/
    inserts/TRANSACTION.csv
    inserts/TRANSACTION.json
    inserts/AUTHORIZATION.csv
    ...
    updates/ACCOUNT.csv
    ...
```

Strip any key prefixed with `_` before writing.  Write both CSV (via pandas) and
JSON (pretty-printed, `default=str`) unless formats config excludes a format.

---

## 14. init_runner.py sequence

1. `db_path.parent.mkdir(parents=True, exist_ok=True)`
2. Delete existing DB file if present
3. `create_all_tables(db_path)`
4. `StateStore(db_path)`
5. `store.init_sequence(tbl, 1)` for all 15 entity tables
6. `store.bulk_insert(table, rows)` for all REF_DATA tables
7. Seed clients → providers → products → merchants → accounts/customers/cards
8. `store.bulk_upsert("account_temporal_state", temp_states)`
9. `store.set_simulation_meta(run_date, run_id, total_runs=0)`
10. `store.record_run(..., run_mode="init")`
