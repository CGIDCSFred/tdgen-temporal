"""
Schema extraction, export, and Graphviz ER-diagram generation for TDGen-Temporal.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

# ── Group metadata ─────────────────────────────────────────────────────────────

GROUPS: dict[str, dict] = {
    "core": {"label": "Core Entities", "header": "#3b82f6", "bg": "#0f1f3d"},
    "transaction": {"label": "Transactions", "header": "#10b981", "bg": "#0a2a1e"},
    "risk": {"label": "Risk Management", "header": "#ef4444", "bg": "#2a0f0f"},
    "temporal": {"label": "Temporal State", "header": "#f59e0b", "bg": "#2a1e0a"},
    "reference": {"label": "Reference Data", "header": "#8b5cf6", "bg": "#1a0f2d"},
    "control": {"label": "Simulation Control", "header": "#6b7280", "bg": "#0f1520"},
}

_TABLE_GROUPS: dict[str, str] = {
    "CLIENT": "core",
    "PROVIDER": "core",
    "PRODUCT_DEFINITION": "core",
    "MERCHANT": "core",
    "ACCOUNT": "core",
    "CUSTOMER": "core",
    "CARD": "core",
    "AUTHORIZATION": "transaction",
    "TRANSACTION": "transaction",
    "STATEMENT": "transaction",
    "DISPUTE": "risk",
    "CHARGEBACK": "risk",
    "FRAUD_ALERT": "risk",
    "SCORE_RECORD": "risk",
    "COLLECTION_CASE": "risk",
    "account_temporal_state": "temporal",
    "card_temporal_state": "temporal",
    "dispute_temporal_state": "temporal",
    "fraud_alert_temporal_state": "temporal",
    "chargeback_temporal_state": "temporal",
    "collection_case_temporal_state": "temporal",
    "simulation_meta": "control",
    "run_log": "control",
    "pk_sequences": "control",
}

_TABLE_DESCRIPTIONS: dict[str, str] = {
    "CLIENT": "Top-level issuing client (bank or financial institution)",
    "PROVIDER": "Portfolio provider belonging to a client",
    "PRODUCT_DEFINITION": "Card product with pricing, APR, limits, and rewards programme",
    "MERCHANT": "Counterparty merchant for card transactions",
    "ACCOUNT": "Credit card account — the central simulation entity",
    "CUSTOMER": "Primary cardholder linked to an account",
    "CARD": "Physical or virtual card issued against an account",
    "AUTHORIZATION": "Real-time authorisation request and response",
    "TRANSACTION": "Posted transaction (settled approved authorisations)",
    "STATEMENT": "Monthly billing statement for an account",
    "DISPUTE": "Cardholder dispute of a transaction",
    "CHARGEBACK": "Network-level chargeback escalated from a resolved dispute",
    "FRAUD_ALERT": "System-generated fraud flag on a transaction",
    "SCORE_RECORD": "Monthly credit score snapshot per account",
    "COLLECTION_CASE": "Active collections case for a delinquent account",
}

# Logical FK relationships (PRAGMA foreign_keys=OFF in this DB — enforced in app layer)
_FOREIGN_KEYS: dict[str, list[dict]] = {
    "PROVIDER": [
        {"col": "client_id", "ref_table": "CLIENT", "ref_col": "client_id"},
    ],
    "ACCOUNT": [
        {"col": "provider_id", "ref_table": "PROVIDER", "ref_col": "provider_id"},
        {
            "col": "tsys_product_code",
            "ref_table": "PRODUCT_DEFINITION",
            "ref_col": "tsys_product_code",
        },
    ],
    "CUSTOMER": [
        {"col": "account_id", "ref_table": "ACCOUNT", "ref_col": "account_id"},
    ],
    "CARD": [
        {"col": "account_id", "ref_table": "ACCOUNT", "ref_col": "account_id"},
    ],
    "AUTHORIZATION": [
        {"col": "account_id", "ref_table": "ACCOUNT", "ref_col": "account_id"},
        {"col": "card_id", "ref_table": "CARD", "ref_col": "card_id"},
        {"col": "merchant_id", "ref_table": "MERCHANT", "ref_col": "merchant_id"},
    ],
    "TRANSACTION": [
        {"col": "account_id", "ref_table": "ACCOUNT", "ref_col": "account_id"},
        {"col": "card_id", "ref_table": "CARD", "ref_col": "card_id"},
        {"col": "merchant_id", "ref_table": "MERCHANT", "ref_col": "merchant_id"},
        {"col": "auth_id", "ref_table": "AUTHORIZATION", "ref_col": "auth_id"},
        {"col": "statement_id", "ref_table": "STATEMENT", "ref_col": "statement_id"},
    ],
    "STATEMENT": [
        {"col": "account_id", "ref_table": "ACCOUNT", "ref_col": "account_id"},
    ],
    "DISPUTE": [
        {"col": "transaction_id", "ref_table": "TRANSACTION", "ref_col": "transaction_id"},
        {"col": "account_id", "ref_table": "ACCOUNT", "ref_col": "account_id"},
    ],
    "CHARGEBACK": [
        {"col": "dispute_id", "ref_table": "DISPUTE", "ref_col": "dispute_id"},
        {"col": "transaction_id", "ref_table": "TRANSACTION", "ref_col": "transaction_id"},
    ],
    "FRAUD_ALERT": [
        {"col": "account_id", "ref_table": "ACCOUNT", "ref_col": "account_id"},
        {"col": "transaction_id", "ref_table": "TRANSACTION", "ref_col": "transaction_id"},
    ],
    "SCORE_RECORD": [
        {"col": "account_id", "ref_table": "ACCOUNT", "ref_col": "account_id"},
    ],
    "COLLECTION_CASE": [
        {"col": "account_id", "ref_table": "ACCOUNT", "ref_col": "account_id"},
    ],
    "account_temporal_state": [
        {"col": "account_id", "ref_table": "ACCOUNT", "ref_col": "account_id"},
    ],
    "card_temporal_state": [
        {"col": "card_id", "ref_table": "CARD", "ref_col": "card_id"},
    ],
    "dispute_temporal_state": [
        {"col": "dispute_id", "ref_table": "DISPUTE", "ref_col": "dispute_id"},
    ],
    "fraud_alert_temporal_state": [
        {"col": "alert_id", "ref_table": "FRAUD_ALERT", "ref_col": "alert_id"},
    ],
    "chargeback_temporal_state": [
        {"col": "chargeback_id", "ref_table": "CHARGEBACK", "ref_col": "chargeback_id"},
    ],
    "collection_case_temporal_state": [
        {"col": "case_id", "ref_table": "COLLECTION_CASE", "ref_col": "case_id"},
    ],
}


# ── Extraction ─────────────────────────────────────────────────────────────────


def extract_from_db(db_path: Path) -> dict:
    """Read the live schema from SQLite and return a structured schema dict."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    tables = []
    master = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND sql IS NOT NULL ORDER BY name"
    ).fetchall()

    for row in master:
        tbl = row["name"]
        pragma = conn.execute(f'PRAGMA table_info("{tbl}")').fetchall()
        fk_map = {f["col"]: f for f in _FOREIGN_KEYS.get(tbl, [])}

        columns = []
        for col in pragma:
            fk_info = fk_map.get(col["name"])
            columns.append(
                {
                    "name": col["name"],
                    "type": col["type"] or "TEXT",
                    "pk": bool(col["pk"]),
                    "nullable": not bool(col["notnull"]) and not bool(col["pk"]),
                    "fk": (
                        {"table": fk_info["ref_table"], "column": fk_info["ref_col"]}
                        if fk_info
                        else None
                    ),
                }
            )

        if tbl.startswith("REF_"):
            group = "reference"
        else:
            group = _TABLE_GROUPS.get(tbl, "other")

        tables.append(
            {
                "name": tbl,
                "group": group,
                "description": _TABLE_DESCRIPTIONS.get(tbl, ""),
                "columns": columns,
            }
        )

    conn.close()

    return {
        "name": "TSYS TS2 Simulation Schema",
        "version": "1.0",
        "description": (
            "Synthetic credit card portfolio schema modelled on the TSYS TS2 "
            "card processing platform. Covers the full lifecycle: account "
            "origination, daily transactions, disputes, fraud management, "
            "collections, and credit scoring."
        ),
        "exported_at": datetime.now().isoformat(timespec="seconds"),
        "tables": tables,
    }


# ── Export ─────────────────────────────────────────────────────────────────────


def to_json(schema: dict) -> str:
    return json.dumps(schema, indent=2)


def to_sql_ddl(db_path: Path) -> str:
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL ORDER BY name"
    ).fetchall()
    conn.close()
    parts = ["-- TSYS TS2 Simulation Schema — generated by TDGen-Temporal", ""]
    for name, sql in rows:
        parts += [f"-- {name}", sql + ";", ""]
    return "\n".join(parts)


# ── Graphviz DOT ───────────────────────────────────────────────────────────────


def to_graphviz_dot(schema: dict, groups: set[str], keys_only: bool = True) -> str:
    """
    Generate a Graphviz DOT ER diagram.

    groups:    set of group names to include (e.g. {"core", "transaction", "risk"})
    keys_only: show only PK + FK columns; append a summary row for the rest
    """
    tables = [t for t in schema["tables"] if t["group"] in groups]
    table_names = {t["name"] for t in tables}

    lines = [
        "digraph TS2 {",
        "  rankdir=LR",
        '  graph [fontname="Helvetica" bgcolor="transparent"',
        '         pad="0.6" nodesep="0.5" ranksep="1.0"]',
        '  node  [fontname="Helvetica" fontsize="9" shape=none margin="0"]',
        '  edge  [fontname="Helvetica" fontsize="8" color="#475569"',
        "         arrowhead=open arrowsize=0.7 penwidth=1.1]",
        "",
    ]

    # Group tables into clusters
    by_group: dict[str, list[dict]] = {}
    for t in tables:
        by_group.setdefault(t["group"], []).append(t)

    for ci, (grp, grp_tables) in enumerate(by_group.items()):
        gm = GROUPS.get(grp, {"label": grp, "header": "#64748b", "bg": "#0f172a"})
        hdr_col = gm["header"]
        bg_col = gm["bg"]

        lines += [
            f"  subgraph cluster_{ci} {{",
            f'    label="{gm["label"]}"',
            f'    fontcolor="{hdr_col}" fontsize="11" fontname="Helvetica Bold"',
            f'    color="{hdr_col}" style="rounded,dashed" penwidth="1.5"',
            "",
        ]

        for tbl in grp_tables:
            name = tbl["name"]
            cols = tbl["columns"]

            if keys_only:
                visible = [c for c in cols if c["pk"] or c["fk"]]
                extra = len(cols) - len(visible)
            else:
                visible = cols
                extra = 0

            rows_html = ""
            for col in visible:
                if col["pk"]:
                    badge = "PK"
                    badge_color = "#fbbf24"
                elif col["fk"]:
                    badge = "FK"
                    badge_color = "#34d399"
                else:
                    badge = "  "
                    badge_color = "#94a3b8"

                rows_html += (
                    f"<TR>"
                    f'<TD ALIGN="LEFT" BGCOLOR="{bg_col}" BORDER="0" CELLPADDING="3">'
                    f'<FONT COLOR="{badge_color}"><B>{badge}</B></FONT></TD>'
                    f'<TD ALIGN="LEFT" BGCOLOR="{bg_col}" BORDER="0" CELLPADDING="3">'
                    f'<FONT COLOR="#e2e8f0">{col["name"]}</FONT></TD>'
                    f'<TD ALIGN="LEFT" BGCOLOR="{bg_col}" BORDER="0" CELLPADDING="3">'
                    f'<FONT COLOR="#64748b"><I>{col["type"]}</I></FONT></TD>'
                    f"</TR>"
                )

            if extra > 0:
                noun = "column" if extra == 1 else "columns"
                rows_html += (
                    f"<TR>"
                    f'<TD COLSPAN="3" ALIGN="CENTER" BGCOLOR="{bg_col}" BORDER="0" CELLPADDING="2">'
                    f'<FONT COLOR="#475569">···  {extra} more {noun}</FONT>'
                    f"</TD></TR>"
                )

            label = (
                f"<<TABLE BORDER='1' CELLBORDER='0' CELLSPACING='0' COLOR='#334155'>"
                f"<TR><TD COLSPAN='3' BGCOLOR='{hdr_col}' ALIGN='CENTER' CELLPADDING='5'>"
                f"<FONT COLOR='white'><B>{name}</B></FONT></TD></TR>"
                f"{rows_html}"
                f"</TABLE>>"
            )
            lines.append(f'    "{name}" [label={label}]')

        lines += ["  }", ""]

    # FK edges — only between visible tables
    drawn: set[tuple] = set()
    for tbl in tables:
        for col in tbl["columns"]:
            if col["fk"] and col["fk"]["table"] in table_names:
                key = (tbl["name"], col["fk"]["table"], col["name"])
                if key not in drawn:
                    drawn.add(key)
                    lines.append(
                        f'  "{tbl["name"]}" -> "{col["fk"]["table"]}" [tooltip="{col["name"]}"]'
                    )

    lines += ["", "}"]
    return "\n".join(lines)
