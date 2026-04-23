"""
TDGen-Temporal — Streamlit UI
Tabs: Configuration & Controls | Dashboard | Data Explorer | Validation
"""

import subprocess
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import yaml

# ── Project root (one level above ui/)
ROOT = Path(__file__).parent.parent
DEFAULT_DB = ROOT / "output" / "state.db"
DEFAULT_CONFIG = ROOT / "config" / "scenario.yaml"
DEFAULT_OUTPUT = ROOT / "output"

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="TDGen-Temporal",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
    .metric-card { background:#1e293b; border-radius:8px; padding:12px 16px; }
    .stTabs [data-baseweb="tab"] { font-size:15px; padding:8px 20px; }
    .stDataFrame { font-size:13px; }
</style>
""",
    unsafe_allow_html=True,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


@st.cache_data(ttl=8)
def _query(db_str: str, sql: str, params: tuple = ()) -> pd.DataFrame:
    import sqlite3

    conn = sqlite3.connect(db_str)
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    return _query(str(db_path), sql, params)


def db_ready(p: Path) -> bool:
    return p.exists() and p.stat().st_size > 100


def get_meta() -> dict | None:
    if not db_ready(db_path):
        return None
    try:
        import sqlite3

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM simulation_meta LIMIT 1").fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception:
        return None


def table_count(tbl: str) -> int:
    df = query(f"SELECT COUNT(*) AS n FROM {tbl}")
    return int(df["n"].iloc[0]) if not df.empty else 0


def run_cli(*args: str) -> tuple[int, str, str]:
    """Run a tdgen-temporal CLI command and return (rc, stdout, stderr)."""
    result = subprocess.run(
        [sys.executable, "-m", "tdgen_temporal.cli", *args],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    return result.returncode, result.stdout, result.stderr


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🏦 TDGen-Temporal")
    st.divider()

    db_path_str = st.text_input("Database", value=str(DEFAULT_DB), key="sb_db")
    config_path_str = st.text_input("Config", value=str(DEFAULT_CONFIG), key="sb_cfg")

    db_path = Path(db_path_str)
    config_path = Path(config_path_str)

    st.divider()
    meta = get_meta()
    if meta:
        st.metric("Simulation date", str(meta.get("current_run_date", "—")))
        st.metric("Days simulated", f"{meta.get('total_runs', 0):,}")
        st.metric("Accounts", f"{table_count('ACCOUNT'):,}")
        st.metric("Transactions", f"{table_count('TRANSACTION'):,}")
        st.metric("Open disputes", f"{table_count('DISPUTE'):,}")
    else:
        st.info("No database yet — use **Configuration** tab to initialise.")

    st.divider()
    if st.button("🔄 Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_cfg, tab_dash, tab_exp, tab_val = st.tabs(
    [
        "⚙️  Configuration",
        "📊  Dashboard",
        "🔍  Data Explorer",
        "✅  Validation",
    ]
)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Configuration & Controls
# ══════════════════════════════════════════════════════════════════════════════
with tab_cfg:
    # ── What is this tool?
    st.markdown(
        """
**TDGen-Temporal** is a synthetic credit-card data generator.
It seeds a realistic portfolio of accounts, customers, cards, and merchants, then
advances the simulation day-by-day — producing transactions, disputes, fraud alerts,
chargebacks, score records, and collection cases that evolve over time.

Use it to build test datasets, train ML models, or explore data pipelines without
touching production data.
"""
    )

    with st.expander("How to get started", expanded=not db_ready(db_path)):
        st.markdown(
            """
**Step 1 — Configure** (optional)
Edit `scenario.yaml` on the left to control population size, transaction rates, and
lifecycle timings. Click **Save config** when done.

**Step 2 — Initialise**
Open the *Initialise (Day 0)* panel on the right. Pick a start date and click
**Initialise**. This creates the database and seeds the opening population.

**Step 3 — Advance**
Use the *Advance* panel to move the simulation forward. Enter the number of days and
click **Advance**. Repeat as many times as you like — each run appends new rows.

**Step 4 — Explore**
Switch to the **Dashboard** tab for charts, the **Data Explorer** tab to browse and
drill into individual records, or the **Validation** tab to check data integrity.
"""
        )

    st.divider()
    left, right = st.columns([3, 2], gap="large")

    # ── YAML editor
    with left:
        st.subheader("Scenario configuration")
        yaml_text = (
            config_path.read_text(encoding="utf-8") if config_path.exists() else "# file not found"
        )
        edited_yaml = st.text_area("scenario.yaml", value=yaml_text, height=460, key="yaml_editor")

        if st.button("💾  Save config", use_container_width=True):
            try:
                yaml.safe_load(edited_yaml)
                config_path.write_text(edited_yaml, encoding="utf-8")
                st.success("Saved.")
                st.cache_data.clear()
            except yaml.YAMLError as exc:
                st.error(f"Invalid YAML: {exc}")

    # ── Controls
    with right:
        st.subheader("Simulation controls")

        # Init
        with st.expander("🚀  Initialise (Day 0)", expanded=not db_ready(db_path)):
            init_date = st.date_input("Start date", value=date(2024, 1, 1), key="init_date")
            if st.button("Initialise", type="primary", use_container_width=True):
                with st.spinner("Seeding Day 0 population…"):
                    rc, out, err = run_cli(
                        "init",
                        "--db",
                        str(db_path),
                        "--config",
                        str(config_path),
                        "--date",
                        str(init_date),
                    )
                if rc == 0:
                    st.success("Done!")
                    st.code(out or "(no output)")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error("Init failed.")
                    st.code((out + "\n" + err).strip())

        # Advance
        with st.expander("⏩  Advance", expanded=True):
            adv_days = st.number_input("Days", min_value=1, max_value=365, value=7, key="adv_days")
            disabled = not db_ready(db_path)
            if st.button("Advance", type="primary", use_container_width=True, disabled=disabled):
                with st.spinner(f"Advancing {adv_days} day(s)…"):
                    rc, out, err = run_cli(
                        "advance",
                        "--db",
                        str(db_path),
                        "--config",
                        str(config_path),
                        "--output",
                        str(DEFAULT_OUTPUT),
                        "--days",
                        str(adv_days),
                    )
                if rc == 0:
                    st.success(f"Advanced {adv_days} day(s).")
                    st.code(out or "(no output)")
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error("Advance failed.")
                    st.code((out + "\n" + err).strip())

        # Backfill
        with st.expander("📅  Backfill date range"):
            bf_from = st.date_input("From", value=date(2024, 1, 1), key="bf_from")
            bf_to = st.date_input("To", value=date(2024, 1, 31), key="bf_to")
            if st.button("Run backfill", use_container_width=True, disabled=not db_ready(db_path)):
                if bf_to < bf_from:
                    st.error("'To' must be ≥ 'From'.")
                else:
                    n_days = (bf_to - bf_from).days + 1
                    with st.spinner(f"Backfilling {n_days} days…"):
                        rc, out, err = run_cli(
                            "backfill",
                            "--db",
                            str(db_path),
                            "--config",
                            str(config_path),
                            "--output",
                            str(DEFAULT_OUTPUT),
                            "--from",
                            str(bf_from),
                            "--to",
                            str(bf_to),
                        )
                    if rc == 0:
                        st.success("Backfill complete.")
                        st.code(out or "(no output)")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("Backfill failed.")
                        st.code((out + "\n" + err).strip())

        # Reset
        with st.expander("🗑️  Reset simulation"):
            st.warning("This will delete the database and all generated output.")
            if st.button(
                "Reset", use_container_width=True, type="secondary", disabled=not db_ready(db_path)
            ):
                db_path.unlink(missing_ok=True)
                st.cache_data.clear()
                st.success("Database deleted. Initialise a new simulation.")
                st.rerun()

    # ── Run log
    st.divider()
    st.subheader("Run log")
    if db_ready(db_path):
        log_df = query("""
            SELECT
                run_date,
                run_mode,
                accounts_processed,
                COALESCE(json_extract(inserts_json,'$.TRANSACTION'),0)   AS txns_inserted,
                COALESCE(json_extract(inserts_json,'$.DISPUTE'),0)        AS disputes_inserted,
                COALESCE(json_extract(inserts_json,'$.FRAUD_ALERT'),0)    AS fraud_inserted,
                COALESCE(json_extract(updates_json,'$.ACCOUNT'),0)        AS accounts_updated,
                ROUND(duration_seconds, 2)                                AS duration_s
            FROM run_log
            ORDER BY run_date DESC
            LIMIT 50
        """)
        st.dataframe(log_df, width="stretch", hide_index=True)
    else:
        st.info("No run log yet.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Dashboard
# ══════════════════════════════════════════════════════════════════════════════
with tab_dash:
    if not db_ready(db_path):
        st.info("Initialise the simulation first.")
    else:
        # ── KPIs
        st.subheader("Portfolio overview")
        k = st.columns(7)
        kpi_tables = [
            ("Accounts", "ACCOUNT"),
            ("Customers", "CUSTOMER"),
            ("Cards", "CARD"),
            ("Transactions", "TRANSACTION"),
            ("Disputes", "DISPUTE"),
            ("Fraud alerts", "FRAUD_ALERT"),
            ("Chargebacks", "CHARGEBACK"),
        ]
        for col, (label, tbl) in zip(k, kpi_tables):
            col.metric(label, f"{table_count(tbl):,}")

        st.divider()

        # ── Row 1: Transaction volume + Account states
        r1a, r1b = st.columns(2)

        with r1a:
            st.subheader("Daily transaction & auth volume")
            vol_df = query("""
                SELECT
                    run_date,
                    COALESCE(json_extract(inserts_json,'$.TRANSACTION'),0)    AS Transactions,
                    COALESCE(json_extract(inserts_json,'$.AUTHORIZATION'),0)  AS Authorizations
                FROM run_log ORDER BY run_date
            """)
            if not vol_df.empty:
                fig = px.line(
                    vol_df,
                    x="run_date",
                    y=["Transactions", "Authorizations"],
                    labels={"value": "Count", "run_date": "Date", "variable": ""},
                    color_discrete_map={"Transactions": "#3b82f6", "Authorizations": "#10b981"},
                )
                fig.update_layout(height=300, margin=dict(t=10, b=10), legend_title_text="")
                st.plotly_chart(fig, width="stretch")
            else:
                st.info("No run log data yet — advance the simulation.")

        with r1b:
            st.subheader("Account status distribution")
            acc_df = query(
                "SELECT account_status, COUNT(*) AS n FROM ACCOUNT GROUP BY account_status"
            )
            if not acc_df.empty:
                fig = px.pie(
                    acc_df,
                    values="n",
                    names="account_status",
                    color_discrete_sequence=px.colors.qualitative.Safe,
                    hole=0.35,
                )
                fig.update_layout(height=300, margin=dict(t=10, b=10))
                st.plotly_chart(fig, width="stretch")

        # ── Row 2: Disputes + Fraud alerts
        r2a, r2b = st.columns(2)

        with r2a:
            st.subheader("Dispute status")
            disp_df = query(
                "SELECT dispute_status, COUNT(*) AS n FROM DISPUTE GROUP BY dispute_status"
            )
            if not disp_df.empty:
                fig = px.bar(
                    disp_df,
                    x="dispute_status",
                    y="n",
                    color="dispute_status",
                    color_discrete_sequence=px.colors.qualitative.Pastel,
                    labels={"n": "Count", "dispute_status": ""},
                )
                fig.update_layout(height=300, margin=dict(t=10, b=10), showlegend=False)
                st.plotly_chart(fig, width="stretch")
            else:
                st.info("No disputes yet.")

        with r2b:
            st.subheader("Fraud alert status")
            fraud_df = query(
                "SELECT alert_status, COUNT(*) AS n FROM FRAUD_ALERT GROUP BY alert_status"
            )
            if not fraud_df.empty:
                fig = px.bar(
                    fraud_df,
                    x="alert_status",
                    y="n",
                    color="alert_status",
                    color_discrete_sequence=px.colors.qualitative.Antique,
                    labels={"n": "Count", "alert_status": ""},
                )
                fig.update_layout(height=300, margin=dict(t=10, b=10), showlegend=False)
                st.plotly_chart(fig, width="stretch")
            else:
                st.info("No fraud alerts yet.")

        # ── Row 3: Collection buckets + Daily risk events
        r3a, r3b = st.columns(2)

        with r3a:
            st.subheader("Collection cases by delinquency bucket")
            BUCKET_ORDER = ["B1", "B2", "B3", "B4", "CHARGEOFF", "RESOLVED"]
            BUCKET_COLORS = {
                "B1": "#86efac",
                "B2": "#fde68a",
                "B3": "#fdba74",
                "B4": "#f97316",
                "CHARGEOFF": "#ef4444",
                "RESOLVED": "#6b7280",
            }
            coll_df = query("""
                SELECT delinquency_bucket, COUNT(*) AS n
                FROM COLLECTION_CASE GROUP BY delinquency_bucket
            """)
            if not coll_df.empty:
                coll_df["delinquency_bucket"] = pd.Categorical(
                    coll_df["delinquency_bucket"], categories=BUCKET_ORDER, ordered=True
                )
                coll_df = coll_df.sort_values("delinquency_bucket")
                fig = px.bar(
                    coll_df,
                    x="delinquency_bucket",
                    y="n",
                    color="delinquency_bucket",
                    color_discrete_map=BUCKET_COLORS,
                    labels={"n": "Count", "delinquency_bucket": ""},
                )
                fig.update_layout(height=300, margin=dict(t=10, b=10), showlegend=False)
                st.plotly_chart(fig, width="stretch")
            else:
                st.info("No collection cases yet.")

        with r3b:
            st.subheader("Daily risk event volume")
            risk_df = query("""
                SELECT
                    run_date,
                    COALESCE(json_extract(inserts_json,'$.DISPUTE'),0)          AS Disputes,
                    COALESCE(json_extract(inserts_json,'$.FRAUD_ALERT'),0)      AS "Fraud alerts",
                    COALESCE(json_extract(inserts_json,'$.CHARGEBACK'),0)       AS Chargebacks,
                    COALESCE(json_extract(inserts_json,'$.COLLECTION_CASE'),0)  AS "Collection cases"
                FROM run_log ORDER BY run_date
            """)
            if not risk_df.empty:
                fig = px.area(
                    risk_df,
                    x="run_date",
                    y=["Disputes", "Fraud alerts", "Chargebacks", "Collection cases"],
                    labels={"value": "Count", "run_date": "Date", "variable": ""},
                )
                fig.update_layout(height=300, margin=dict(t=10, b=10), legend_title_text="")
                st.plotly_chart(fig, width="stretch")
            else:
                st.info("No run log data yet.")

        # ── Row 4: Credit score bands + Transaction types
        r4a, r4b = st.columns(2)

        with r4a:
            st.subheader("Credit score band distribution")
            score_df = query("""
                SELECT score_band, COUNT(*) AS n
                FROM SCORE_RECORD GROUP BY score_band ORDER BY score_band
            """)
            if not score_df.empty:
                fig = px.bar(
                    score_df,
                    x="score_band",
                    y="n",
                    color="score_band",
                    color_discrete_sequence=px.colors.sequential.Blues_r,
                    labels={"n": "Count", "score_band": "Score band"},
                )
                fig.update_layout(height=280, margin=dict(t=10, b=10), showlegend=False)
                st.plotly_chart(fig, width="stretch")
            else:
                st.info("No score records yet.")

        with r4b:
            st.subheader("Transaction type breakdown")
            txtype_df = query("""
                SELECT transaction_type, COUNT(*) AS n, ROUND(SUM(transaction_amount),2) AS total_amount
                FROM TRANSACTION GROUP BY transaction_type ORDER BY n DESC
            """)
            if not txtype_df.empty:
                fig = px.bar(
                    txtype_df,
                    x="transaction_type",
                    y="n",
                    color="transaction_type",
                    color_discrete_sequence=px.colors.qualitative.G10,
                    labels={"n": "Count", "transaction_type": ""},
                    hover_data={"total_amount": True},
                )
                fig.update_layout(height=280, margin=dict(t=10, b=10), showlegend=False)
                st.plotly_chart(fig, width="stretch")
            else:
                st.info("No transactions yet.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Data Explorer
# ══════════════════════════════════════════════════════════════════════════════

TABLES = {
    "Accounts": ("ACCOUNT", "account_id"),
    "Customers": ("CUSTOMER", "customer_id"),
    "Cards": ("CARD", "card_id"),
    "Transactions": ("TRANSACTION", "transaction_id"),
    "Authorizations": ("AUTHORIZATION", "auth_id"),
    "Statements": ("STATEMENT", "statement_id"),
    "Disputes": ("DISPUTE", "dispute_id"),
    "Chargebacks": ("CHARGEBACK", "chargeback_id"),
    "Fraud Alerts": ("FRAUD_ALERT", "alert_id"),
    "Score Records": ("SCORE_RECORD", "score_id"),
    "Collection Cases": ("COLLECTION_CASE", "case_id"),
    "Merchants": ("MERCHANT", "merchant_id"),
    "Clients": ("CLIENT", "client_id"),
    "Providers": ("PROVIDER", "provider_id"),
    "Products": ("PRODUCT_DEFINITION", "tsys_product_code"),
    "Run Log": ("run_log", "run_id"),
}

with tab_exp:
    if not db_ready(db_path):
        st.info("Initialise the simulation first.")
    else:
        top_left, top_mid, top_right = st.columns([2, 3, 1])

        with top_left:
            tbl_label = st.selectbox("Entity", list(TABLES.keys()), key="exp_table")

        tbl_name, pk_col = TABLES[tbl_label]
        full_df = query(f"SELECT * FROM {tbl_name}")

        with top_mid:
            search = st.text_input(
                "Search any column", placeholder="Type to filter…", key="exp_search"
            )

        with top_right:
            page_size = st.selectbox("Rows / page", [25, 50, 100, 250], index=1, key="exp_ps")

        # Filter
        if search:
            mask = full_df.apply(
                lambda col: col.astype(str).str.contains(search, case=False, na=False)
            ).any(axis=1)
            filtered = full_df[mask].reset_index(drop=True)
        else:
            filtered = full_df

        st.caption(f"{len(filtered):,} of {len(full_df):,} rows")

        # Pagination
        n_pages = max(1, (len(filtered) - 1) // page_size + 1)
        page = st.number_input(
            "Page", min_value=1, max_value=n_pages, value=1, step=1, key="exp_page"
        )
        start = (page - 1) * page_size
        page_df = filtered.iloc[start : start + page_size].reset_index(drop=True)

        # Selectable table
        sel = st.dataframe(
            page_df,
            width="stretch",
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row",
        )

        # ── Row detail & related data
        chosen_rows = (sel.get("selection") or {}).get("rows", [])
        if chosen_rows:
            row = page_df.iloc[chosen_rows[0]].to_dict()
            pk_val = row.get(pk_col, "—")
            st.subheader(f"Detail — {pk_col}: {pk_val}")

            detail_df = pd.DataFrame([{"Field": k, "Value": str(v)} for k, v in row.items()])
            st.dataframe(detail_df, width="stretch", hide_index=True)

            # Related-record drill-down
            if tbl_name == "ACCOUNT":
                acc_id = row.get("account_id")
                if acc_id:
                    with st.expander(f"Transactions ({acc_id})"):
                        st.dataframe(
                            query(
                                "SELECT * FROM TRANSACTION WHERE account_id=? ORDER BY post_date DESC LIMIT 100",
                                (acc_id,),
                            ),
                            width="stretch",
                            hide_index=True,
                        )
                    with st.expander(f"Cards ({acc_id})"):
                        st.dataframe(
                            query("SELECT * FROM CARD WHERE account_id=?", (acc_id,)),
                            width="stretch",
                            hide_index=True,
                        )
                    with st.expander(f"Disputes ({acc_id})"):
                        st.dataframe(
                            query(
                                "SELECT * FROM DISPUTE WHERE account_id=? ORDER BY dispute_opened_date DESC",
                                (acc_id,),
                            ),
                            width="stretch",
                            hide_index=True,
                        )
                    with st.expander(f"Fraud Alerts ({acc_id})"):
                        st.dataframe(
                            query("SELECT * FROM FRAUD_ALERT WHERE account_id=?", (acc_id,)),
                            width="stretch",
                            hide_index=True,
                        )
                    with st.expander(f"Collection Cases ({acc_id})"):
                        st.dataframe(
                            query("SELECT * FROM COLLECTION_CASE WHERE account_id=?", (acc_id,)),
                            width="stretch",
                            hide_index=True,
                        )
                    with st.expander(f"Score Records ({acc_id})"):
                        st.dataframe(
                            query(
                                "SELECT * FROM SCORE_RECORD WHERE account_id=? ORDER BY score_date DESC",
                                (acc_id,),
                            ),
                            width="stretch",
                            hide_index=True,
                        )

            elif tbl_name == "TRANSACTION":
                txn_id = row.get("transaction_id")
                if txn_id:
                    with st.expander(f"Disputes for transaction {txn_id}"):
                        st.dataframe(
                            query("SELECT * FROM DISPUTE WHERE transaction_id=?", (txn_id,)),
                            width="stretch",
                            hide_index=True,
                        )
                    with st.expander(f"Fraud Alerts for transaction {txn_id}"):
                        st.dataframe(
                            query("SELECT * FROM FRAUD_ALERT WHERE transaction_id=?", (txn_id,)),
                            width="stretch",
                            hide_index=True,
                        )

            elif tbl_name == "DISPUTE":
                disp_id = row.get("dispute_id")
                if disp_id:
                    with st.expander(f"Chargebacks for dispute {disp_id}"):
                        st.dataframe(
                            query("SELECT * FROM CHARGEBACK WHERE dispute_id=?", (disp_id,)),
                            width="stretch",
                            hide_index=True,
                        )

            elif tbl_name == "CUSTOMER":
                acc_id = row.get("account_id")
                if acc_id:
                    with st.expander(f"Account {acc_id}"):
                        st.dataframe(
                            query("SELECT * FROM ACCOUNT WHERE account_id=?", (acc_id,)),
                            width="stretch",
                            hide_index=True,
                        )

        st.divider()
        st.download_button(
            "⬇️  Export filtered rows as CSV",
            data=filtered.to_csv(index=False).encode("utf-8"),
            file_name=f"{tbl_name.lower()}_{date.today()}.csv",
            mime="text/csv",
            use_container_width=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Validation
# ══════════════════════════════════════════════════════════════════════════════

CAT_LABELS = {
    "referential": "Referential Integrity",
    "temporal": "Temporal Integrity",
    "state": "State Consistency",
}
CAT_ORDER = ["referential", "temporal", "state"]

with tab_val:
    if not db_ready(db_path):
        st.info("Initialise the simulation first.")
    else:
        v1, v2, v3 = st.columns([2, 2, 1])
        with v1:
            errors_only = st.checkbox("Errors only", value=False)
        with v2:
            hide_passing = st.checkbox("Hide passing checks", value=False)
        with v3:
            run_val = st.button("▶  Run", type="primary", use_container_width=True)

        if run_val:
            with st.spinner("Running validation suite…"):
                try:
                    sys.path.insert(0, str(ROOT))
                    from tdgen_temporal.validators import run_all as _run_all

                    val_checks, val_findings = _run_all(db_path)
                    st.session_state["val_checks"] = val_checks
                    st.session_state["val_findings"] = val_findings
                    st.session_state["val_exc"] = None
                except Exception as exc:
                    st.session_state["val_exc"] = str(exc)

        if st.session_state.get("val_exc"):
            st.error(f"Validation failed: {st.session_state['val_exc']}")

        elif "val_checks" in st.session_state:
            v_checks = st.session_state["val_checks"]
            v_findings = st.session_state["val_findings"]
            finding_map = {f.check.name: f for f in v_findings}

            total = len(v_checks)
            n_errors = sum(1 for f in v_findings if f.severity.value == "ERROR")
            n_warnings = sum(1 for f in v_findings if f.severity.value == "WARNING")
            n_pass = total - len(v_findings)
            n_rows = sum(f.count for f in v_findings)

            # ── Banner
            if n_errors == 0 and n_warnings == 0:
                st.success(f"All {total} checks passed — data integrity confirmed.")
            elif n_errors == 0:
                st.warning(f"{n_warnings} warning(s) across {n_rows:,} rows — no hard errors.")
            else:
                st.error(
                    f"{n_errors} error(s) and {n_warnings} warning(s) found "
                    f"across {n_rows:,} violating rows."
                )

            # ── KPIs
            kc = st.columns(5)
            kc[0].metric("Total checks", total)
            kc[1].metric("Passed", n_pass)
            kc[2].metric("Errors", n_errors)
            kc[3].metric("Warnings", n_warnings)
            kc[4].metric("Violating rows", f"{n_rows:,}")

            st.divider()

            # ── Build full results table
            all_rows = []
            for chk in v_checks:
                f = finding_map.get(chk.name)
                all_rows.append(
                    {
                        "_cat": chk.category,
                        "Description": chk.description,
                        "Table": chk.table,
                        "Severity": chk.severity.value,
                        "Status": "FAIL" if f else "PASS",
                        "Violating Rows": f.count if f else 0,
                        "Example PKs": (
                            ", ".join(str(e) for e in f.examples[:5]) if f and f.examples else ""
                        ),
                    }
                )

            full_df = pd.DataFrame(all_rows)

            # Apply filters
            view = full_df.copy()
            if errors_only:
                view = view[view["Severity"] == "ERROR"]
            if hide_passing:
                view = view[view["Status"] == "FAIL"]

            DISPLAY_COLS = [
                "Description",
                "Table",
                "Severity",
                "Status",
                "Violating Rows",
                "Example PKs",
            ]

            def _style(row: pd.Series) -> list[str]:
                if row["Status"] == "FAIL" and row["Severity"] == "ERROR":
                    return ["background-color:#fff1f2"] * len(row)
                if row["Status"] == "FAIL":
                    return ["background-color:#fffbeb"] * len(row)
                return [""] * len(row)

            # ── One expander per category
            for cat in CAT_ORDER:
                cat_df = view[view["_cat"] == cat][DISPLAY_COLS]
                if cat_df.empty:
                    continue
                cat_all = full_df[full_df["_cat"] == cat]
                n_cat_fail = (cat_all["Status"] == "FAIL").sum()
                n_cat_total = len(cat_all)
                label = (
                    f"{CAT_LABELS[cat]}  —  "
                    f"{n_cat_total - n_cat_fail}/{n_cat_total} passing"
                    + (f"  ·  {n_cat_fail} failing" if n_cat_fail else "")
                )
                with st.expander(label, expanded=n_cat_fail > 0):
                    st.dataframe(
                        cat_df.style.apply(_style, axis=1),
                        width="stretch",
                        hide_index=True,
                        column_config={
                            "Violating Rows": st.column_config.NumberColumn(
                                "Violating Rows", format="%d"
                            ),
                            "Example PKs": st.column_config.TextColumn(
                                "Example PKs", width="medium"
                            ),
                        },
                    )
