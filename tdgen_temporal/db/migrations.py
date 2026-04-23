"""
DDL runner — creates all TS2 tables + temporal state tables in state.db.
Uses CREATE TABLE IF NOT EXISTS; safe to call on an existing DB.
"""

import sqlite3
from pathlib import Path


_DDL = [
    # ── Simulation control ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS simulation_meta (
        id              INTEGER PRIMARY KEY CHECK (id = 1),
        current_run_date TEXT NOT NULL,
        last_run_id     TEXT NOT NULL,
        total_runs      INTEGER DEFAULT 0,
        created_at      TEXT NOT NULL,
        updated_at      TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_log (
        run_id              TEXT PRIMARY KEY,
        run_date            TEXT NOT NULL,
        run_mode            TEXT NOT NULL,
        accounts_processed  INTEGER,
        inserts_json        TEXT,
        updates_json        TEXT,
        duration_seconds    REAL,
        created_at          TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS pk_sequences (
        table_name  TEXT PRIMARY KEY,
        next_id     INTEGER NOT NULL DEFAULT 1
    )
    """,

    # ── REF tables ─────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS REF_ACCOUNT_STATUS (
        status_code         TEXT PRIMARY KEY,
        status_description  TEXT,
        is_active           INTEGER,
        weight_pct          REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_CARD_STATUS (
        status_code         TEXT PRIMARY KEY,
        status_description  TEXT,
        is_usable           INTEGER,
        weight_pct          REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_CARD_BRAND (
        brand_code          TEXT PRIMARY KEY,
        brand_name          TEXT,
        bin_prefix          TEXT,
        weight_pct          REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_TRANSACTION_TYPE (
        type_code           TEXT PRIMARY KEY,
        type_description    TEXT,
        is_debit            INTEGER,
        weight_pct          REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_DISPUTE_TYPE (
        type_code           TEXT PRIMARY KEY,
        type_description    TEXT,
        weight_pct          REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_CHARGEBACK_STAGE (
        stage_code          TEXT PRIMARY KEY,
        stage_description   TEXT,
        stage_order         INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_FRAUD_ALERT_TYPE (
        alert_type_code     TEXT PRIMARY KEY,
        alert_type_description TEXT,
        detection_system    TEXT,
        weight_pct          REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_AUTH_RESPONSE (
        response_code       TEXT PRIMARY KEY,
        response_description TEXT,
        is_approved         INTEGER,
        weight_pct          REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_POS_ENTRY_MODE (
        entry_mode_code     TEXT PRIMARY KEY,
        entry_mode_description TEXT,
        is_card_present     INTEGER,
        weight_pct          REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_CHANNEL (
        channel_code        TEXT PRIMARY KEY,
        channel_description TEXT,
        weight_pct          REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_DELINQUENCY_BUCKET (
        bucket_code         TEXT PRIMARY KEY,
        bucket_description  TEXT,
        min_days            INTEGER,
        max_days            INTEGER,
        bucket_order        INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_CURRENCY (
        currency_code       TEXT PRIMARY KEY,
        currency_name       TEXT,
        symbol              TEXT,
        decimal_places      INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_COUNTRY (
        country_code        TEXT PRIMARY KEY,
        country_name        TEXT,
        region              TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_MCC (
        mcc_code            TEXT PRIMARY KEY,
        mcc_description     TEXT,
        mcc_group           TEXT,
        weight_pct          REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_STATUS_REASON (
        reason_code         TEXT PRIMARY KEY,
        reason_description  TEXT,
        applies_to_status   TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS REF_SENSITIVITY_LEVEL (
        sensitivity_code    TEXT PRIMARY KEY,
        sensitivity_description TEXT,
        generation_rule     TEXT,
        applicable_regulations TEXT
    )
    """,

    # ── Core entity tables ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS CLIENT (
        client_id           INTEGER PRIMARY KEY,
        client_name         TEXT,
        bin_range           TEXT,
        base_currency       TEXT,
        region              TEXT,
        processing_mode     TEXT,
        association_id      TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS PROVIDER (
        provider_id         INTEGER PRIMARY KEY,
        client_id           INTEGER,
        provider_name       TEXT,
        portfolio_type      TEXT,
        reporting_group     TEXT,
        status              TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS PRODUCT_DEFINITION (
        tsys_product_code   INTEGER,
        client_product_code INTEGER,
        client_id           INTEGER,
        product_description TEXT,
        card_brand          TEXT,
        default_credit_limit INTEGER,
        annual_fee          REAL,
        apr_purchase        REAL,
        apr_cash_advance    REAL,
        grace_period_days   INTEGER,
        rewards_program     TEXT,
        billing_cycle_type  TEXT,
        fee_schedule_id     TEXT,
        PRIMARY KEY (tsys_product_code, client_product_code)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS MERCHANT (
        merchant_id         INTEGER PRIMARY KEY,
        merchant_name       TEXT,
        dba_name            TEXT,
        mcc_code            TEXT,
        terminal_id         TEXT,
        acquirer_id         TEXT,
        city                TEXT,
        state_province      TEXT,
        country_code        TEXT,
        postal_zip          TEXT,
        merchant_url        TEXT,
        risk_tier           TEXT,
        is_online           INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ACCOUNT (
        account_id              INTEGER PRIMARY KEY,
        provider_id             INTEGER,
        tsys_product_code       INTEGER,
        client_product_code     INTEGER,
        account_number          TEXT,
        credit_limit            REAL,
        current_balance         REAL DEFAULT 0,
        available_credit        REAL,
        cash_advance_limit      REAL,
        cash_advance_balance    REAL DEFAULT 0,
        payment_due_amount      REAL DEFAULT 0,
        payment_due_date        TEXT,
        last_payment_date       TEXT,
        last_payment_amount     REAL,
        open_date               TEXT,
        closed_date             TEXT,
        account_status          TEXT DEFAULT 'ACTIVE',
        status_reason           TEXT,
        currency_code           TEXT DEFAULT 'CAD',
        cycle_day               INTEGER,
        days_delinquent         INTEGER DEFAULT 0,
        block_code              TEXT,
        risk_score              REAL,
        last_monetary_date      TEXT,
        last_non_monetary_date  TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS CUSTOMER (
        customer_id         INTEGER PRIMARY KEY,
        account_id          INTEGER,
        first_name          TEXT,
        last_name           TEXT,
        name_line_1         TEXT,
        date_of_birth       TEXT,
        ssn_sin             TEXT,
        address_line_1      TEXT,
        address_line_2      TEXT,
        city                TEXT,
        state_province      TEXT,
        postal_zip          TEXT,
        country_code        TEXT,
        phone_home          TEXT,
        phone_work          TEXT,
        phone_mobile        TEXT,
        email               TEXT,
        language_preference TEXT,
        relationship_type   TEXT,
        id_type             TEXT,
        id_number           TEXT,
        employer_name       TEXT,
        annual_income       REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS CARD (
        card_id             INTEGER PRIMARY KEY,
        account_id          INTEGER,
        card_number         TEXT,
        card_sequence_number INTEGER,
        cardholder_name     TEXT,
        expiry_date         TEXT,
        issue_date          TEXT,
        card_status         TEXT DEFAULT 'ACTIVE',
        card_type           TEXT,
        chip_enabled        INTEGER,
        contactless_enabled INTEGER,
        pin_offset          TEXT,
        card_design_id      TEXT,
        digital_wallet_token TEXT,
        token_requestor     TEXT,
        last_used_date      TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS AUTHORIZATION (
        auth_id             INTEGER PRIMARY KEY,
        account_id          INTEGER,
        card_id             INTEGER,
        merchant_id         INTEGER,
        auth_timestamp      TEXT,
        auth_amount         REAL,
        currency_code       TEXT,
        auth_response_code  TEXT,
        auth_approval_code  TEXT,
        decline_reason      TEXT,
        pos_entry_mode      TEXT,
        pos_condition_code  TEXT,
        channel             TEXT,
        terminal_id         TEXT,
        network             TEXT,
        avs_response        TEXT,
        cvv_response        TEXT,
        risk_score          REAL,
        three_ds_result     TEXT,
        ip_address          TEXT,
        device_fingerprint  TEXT,
        available_after_auth REAL,
        auth_type           TEXT,
        auth_hold_days      INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS "TRANSACTION" (
        transaction_id      INTEGER PRIMARY KEY,
        account_id          INTEGER,
        card_id             INTEGER,
        merchant_id         INTEGER,
        auth_id             INTEGER,
        transaction_date    TEXT,
        post_date           TEXT,
        transaction_amount  REAL,
        billing_amount      REAL,
        transaction_currency TEXT,
        conversion_rate     REAL,
        transaction_type    TEXT,
        transaction_status  TEXT,
        description         TEXT,
        mcc_code            TEXT,
        pos_entry_mode      TEXT,
        channel             TEXT,
        reference_number    TEXT,
        batch_id            TEXT,
        is_recurring        INTEGER,
        is_international    INTEGER,
        interchange_qualifier TEXT,
        interchange_fee     REAL,
        statement_id        INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS STATEMENT (
        statement_id        INTEGER PRIMARY KEY,
        account_id          INTEGER,
        statement_date      TEXT,
        payment_due_date    TEXT,
        opening_balance     REAL,
        closing_balance     REAL,
        total_credits       REAL,
        total_debits        REAL,
        minimum_payment     REAL,
        interest_charged    REAL,
        fees_charged        REAL,
        transaction_count   INTEGER,
        available_credit    REAL,
        cycle_id            TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS DISPUTE (
        dispute_id          INTEGER PRIMARY KEY,
        transaction_id      INTEGER,
        account_id          INTEGER,
        dispute_opened_date TEXT,
        dispute_type        TEXT,
        dispute_status      TEXT DEFAULT 'OPEN',
        dispute_reason_code TEXT,
        disputed_amount     REAL,
        cardholder_explanation TEXT,
        assigned_analyst    TEXT,
        response_due_date   TEXT,
        resolution          TEXT,
        resolved_date       TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS CHARGEBACK (
        chargeback_id       INTEGER PRIMARY KEY,
        dispute_id          INTEGER,
        transaction_id      INTEGER,
        chargeback_date     TEXT,
        chargeback_amount   REAL,
        chargeback_reason_code TEXT,
        chargeback_stage    TEXT,
        representment_status TEXT,
        representment_date  TEXT,
        recovered_amount    REAL,
        network_case_id     TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS FRAUD_ALERT (
        alert_id            INTEGER PRIMARY KEY,
        account_id          INTEGER,
        transaction_id      INTEGER,
        alert_timestamp     TEXT,
        alert_source        TEXT,
        alert_type          TEXT,
        risk_score          INTEGER,
        alert_status        TEXT DEFAULT 'OPEN',
        action_taken        TEXT,
        analyst_id          TEXT,
        resolved_date       TEXT,
        case_link           TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS SCORE_RECORD (
        score_id            INTEGER PRIMARY KEY,
        account_id          INTEGER,
        score_date          TEXT,
        score_type          TEXT,
        score_value         INTEGER,
        score_band          TEXT,
        model_version       TEXT,
        decision            TEXT,
        action_code         TEXT,
        result_code         TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS COLLECTION_CASE (
        case_id             INTEGER PRIMARY KEY,
        account_id          INTEGER,
        case_opened_date    TEXT,
        delinquency_bucket  TEXT,
        amount_past_due     REAL,
        total_owed          REAL,
        case_status         TEXT DEFAULT 'ACTIVE',
        assigned_collector  TEXT,
        contact_method      TEXT,
        last_contact_date   TEXT,
        next_action_date    TEXT,
        next_action         TEXT,
        recovered_amount    REAL DEFAULT 0,
        chargeoff_reason    TEXT,
        chargeoff_date      TEXT
    )
    """,

    # ── Temporal state tables (simulation-internal) ─────────────────────────
    """
    CREATE TABLE IF NOT EXISTS account_temporal_state (
        account_id                  INTEGER PRIMARY KEY,
        current_state               TEXT NOT NULL DEFAULT 'ACTIVE',
        days_delinquent             INTEGER NOT NULL DEFAULT 0,
        consecutive_missed_payments INTEGER NOT NULL DEFAULT 0,
        last_payment_date           TEXT,
        last_statement_date         TEXT,
        payment_due_date            TEXT,
        cycle_day                   INTEGER NOT NULL,
        as_of_date                  TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS card_temporal_state (
        card_id             INTEGER PRIMARY KEY,
        current_state       TEXT NOT NULL DEFAULT 'ACTIVE',
        days_to_expiry      INTEGER,
        replacement_issued  INTEGER DEFAULT 0,
        as_of_date          TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dispute_temporal_state (
        dispute_id          INTEGER PRIMARY KEY,
        current_state       TEXT NOT NULL DEFAULT 'OPEN',
        days_open           INTEGER NOT NULL DEFAULT 0,
        resolved_date       TEXT,
        as_of_date          TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fraud_alert_temporal_state (
        alert_id            INTEGER PRIMARY KEY,
        current_state       TEXT NOT NULL DEFAULT 'OPEN',
        days_open           INTEGER NOT NULL DEFAULT 0,
        reviewed_date       TEXT,
        as_of_date          TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS chargeback_temporal_state (
        chargeback_id       INTEGER PRIMARY KEY,
        current_state       TEXT NOT NULL DEFAULT 'FIRST_CHARGEBACK',
        days_open           INTEGER NOT NULL DEFAULT 0,
        as_of_date          TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS collection_case_temporal_state (
        case_id             INTEGER PRIMARY KEY,
        current_state       TEXT NOT NULL DEFAULT 'ACTIVE',
        current_bucket      TEXT NOT NULL DEFAULT 'B1',
        days_in_bucket      INTEGER NOT NULL DEFAULT 0,
        as_of_date          TEXT NOT NULL
    )
    """,
]


def create_all_tables(db_path: Path) -> None:
    """Create all tables in state.db. Safe to call on an existing database."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        for ddl in _DDL:
            conn.execute(ddl)
        conn.commit()
    finally:
        conn.close()
