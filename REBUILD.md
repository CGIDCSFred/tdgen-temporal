# TDGen-Temporal — Rebuild Guide

This document gives a colleague everything needed to get TDGen-Temporal running on
a new Windows machine — either by cloning the existing repository, or by
regenerating the project from scratch using Claude Code.

---

## What is TDGen-Temporal?

A schema-driven synthetic test data generator with temporal simulation. It maintains
a SQLite state database and runs a day-by-day simulation — accounts age, transactions
post, disputes open and resolve, fraud alerts fire, collection cases progress. Every
simulated day emits CSV/JSON delta files for downstream use.

The web UI (Streamlit) lets non-engineers control the simulation, explore the data,
inspect the schema as an interactive ER diagram, and run validation checks — without
touching the CLI.

The bundled demo scenario is a TSYS TS2 credit-card portfolio (500 accounts,
200 merchants, ~40 tables). The engine is designed to work with any schema — the
TSYS scenario is the reference example, not a hardcoded assumption.

---

## Prerequisites

Install these on the new machine before proceeding:

| Tool | Version | Install |
|---|---|---|
| Python | 3.10 or later | https://www.python.org/downloads/ |
| Git | any recent | https://git-scm.com/download/win |
| Claude Code CLI | latest | `npm install -g @anthropic-ai/claude-code` |
| Graphviz | 12.x | https://graphviz.org/download/ — **add to PATH during install** |
| Node.js | 18+ | Required for Claude Code (https://nodejs.org/) |

Verify Graphviz is on PATH:
```
dot -V
```

---

## Option A — Clone the existing repository (fastest)

If you have access to the Git remote, this is the two-minute path.

```powershell
# 1. Clone
git clone <repo-url> tdgen-temporal
cd tdgen-temporal

# 2. Create and activate a virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3. Install dependencies
pip install -r requirements.txt

# 4. Seed the simulation (Day 0) and advance 30 days
python -m tdgen_temporal.cli init --date 2024-01-01
python -m tdgen_temporal.cli advance --days 30

# 5. Launch the web UI
.\launch_ui.bat
```

The browser opens automatically at http://localhost:8501.

---

## Option B — Rebuild from scratch with Claude Code

Use this path if you do not have repository access, or want to regenerate the
project on a clean machine with no prior code.

### Step 1 — Create a new project directory

```powershell
mkdir tdgen-temporal
cd tdgen-temporal
git init
```

### Step 2 — Copy the rebuild prompt

Copy the file `docs/REBUILD_PROMPT.md` from the original project into your new
directory as `REBUILD_PROMPT.md`.  That file is the complete technical specification
— it defines every table, state machine, generator, and CLI command in the engine.

### Step 3 — Start Claude Code and issue the rebuild command

```powershell
claude
```

Then paste this prompt:

```
Read REBUILD_PROMPT.md in full, then build the complete tdgen-temporal project
exactly as specified. Create every file described — pyproject.toml, requirements.txt,
config/scenario.yaml, all tdgen_temporal/ Python modules, and tests/test_smoke.py.
After creating all files, create a Python virtual environment, install dependencies,
run the smoke tests, initialise the simulation at 2024-01-01, and advance 7 days.
Report any errors and fix them before finishing.
```

Claude Code will scaffold the entire engine. It typically takes 10–15 minutes.

### Step 4 — Add the Streamlit UI

Once the engine passes smoke tests, paste this follow-up prompt into Claude Code:

```
Now add the Streamlit web UI. Create ui/app.py as a Streamlit application with
these tabs:

  ⚙️ Control Panel — scenario.yaml editor, simulation controls (Init / Advance /
  Backfill / Reset), sidebar showing live Entities / Events / Open cases counts

  🗂️ Schema — group-filter checkboxes, keys-only toggle, st.graphviz_chart() ER
  diagram (zoom/pan built in), JSON export button, SQL DDL export button, file
  uploader to load an external schema JSON, per-table expandable reference section

  📊 Dashboard — summary charts: account status distribution, daily transaction
  volume, account balance distribution, simulation timeline

  🔍 Data Explorer — table selector, row limit slider, st.dataframe with
  width="stretch"

  ✅ Validation — run validate CLI command, display pass/fail results

At the top, show a full-width HTML banner with a dark navy gradient, the text
"SCHEMA-DRIVEN SYNTHETIC DATA GENERATOR · TEMPORAL SIMULATION PLATFORM", and
three badge rows for ENTITIES/EVENTS/STATES.

Remove any credit-card-specific language — use domain-agnostic terms throughout
(Entities, Events, Open cases — not Accounts, Transactions, Disputes).

Also create launch_ui.bat that runs:
  start "" http://localhost:8501
  .venv\Scripts\streamlit run ui\app.py --server.headless=false
```

### Step 5 — Export the demo schema

```powershell
python -m tdgen_temporal.cli init --date 2024-01-01
python -m tdgen_temporal.cli advance --days 30
python -c "
from pathlib import Path
from tdgen_temporal.schema import extract_from_db, to_json
schema = extract_from_db(Path('output/state.db'))
Path('config/tsys_ts2_schema.json').write_text(to_json(schema), encoding='utf-8')
print('Exported', len(schema['tables']), 'tables')
"
```

This creates `config/tsys_ts2_schema.json` — the demo schema file used in the
Schema tab's "Load schema" uploader.

---

## Verifying the installation

Run these checks in order. All should pass before handing to users.

```powershell
# Activate venv
.\.venv\Scripts\Activate.ps1

# Unit tests
pytest tests/ -v

# End-to-end: init → advance → validate
python -m tdgen_temporal.cli init --date 2024-01-01
python -m tdgen_temporal.cli advance --days 7
python -m tdgen_temporal.cli status
python -m tdgen_temporal.cli validate --errors-only
```

Expected outcome: `status` shows 7 runs completed; `validate` reports 0 errors.

---

## Project structure

```
tdgen-temporal/
├── config/
│   ├── scenario.yaml          # Simulation parameters (edit before init)
│   └── tsys_ts2_schema.json   # Exported demo schema (load in Schema tab)
├── docs/
│   └── REBUILD_PROMPT.md      # Full technical specification for Claude Code
├── output/
│   └── state.db               # SQLite simulation database (created at init)
├── tdgen_temporal/            # Core Python package
│   ├── cli.py                 # Entry point: init / advance / backfill / status / validate
│   ├── schema.py              # Schema extraction, ER diagram, JSON/DDL export
│   ├── db/
│   │   ├── migrations.py      # DDL — all CREATE TABLE statements
│   │   └── state_store.py     # All DB access (no ORM)
│   ├── engine/
│   │   ├── init_runner.py     # Day 0 seeding
│   │   ├── daily_runner.py    # Per-day orchestration (16 steps)
│   │   └── backfill_runner.py # Date-range loop over daily_runner
│   ├── generators/
│   │   ├── field_generators.py  # Luhn cards, SIN, account numbers, Faker wrappers
│   │   ├── ref_tables.py        # Reference data rows
│   │   ├── seed.py              # Day 0 population (clients → providers → accounts)
│   │   ├── transaction.py       # Daily transaction + authorisation generation
│   │   └── statement.py         # Monthly statement generation
│   ├── output/
│   │   └── delta_writer.py    # CSV/JSON delta files per day
│   ├── state_machines/
│   │   ├── base.py            # AdvanceResult, SideEffect, StateMachine ABC
│   │   ├── account.py         # ACTIVE → DELINQUENT → CHARGEOFF → CLOSED
│   │   ├── card.py            # ACTIVE → EXPIRED / BLOCKED
│   │   ├── dispute.py         # OPEN → INVESTIGATING → RESOLVED → CLOSED
│   │   ├── fraud_alert.py     # OPEN → UNDER_REVIEW → CONFIRMED / FALSE_POSITIVE
│   │   ├── chargeback.py      # FIRST_CHARGEBACK → REPRESENTMENT → WON / LOST
│   │   ├── collection_case.py # ACTIVE → AGENCY_REFERRAL / CHARGEOFF → CLOSED
│   │   └── score_record.py    # Monthly credit score refresh
│   └── validators/
│       ├── temporal.py        # Checks events don't pre-date their entity's open_date
│       ├── referential.py     # FK integrity checks (app-layer, FK enforcement is OFF)
│       ├── state.py           # State machine invariants
│       └── results.py         # Validation result types
├── tests/
│   └── test_smoke.py          # Import checks, Luhn validation, config validation
├── ui/
│   └── app.py                 # Streamlit web dashboard
├── .github/workflows/
│   └── ci.yml                 # Lint → typecheck → matrix tests → smoke test
├── pyproject.toml             # Build, ruff, mypy, pytest config
├── requirements.txt           # pip install target
├── launch_ui.bat              # Double-click to start the web UI
└── REBUILD.md                 # This file
```

---

## CLI reference

```powershell
# Seed a fresh simulation (wipes any existing database)
python -m tdgen_temporal.cli init --date 2024-01-01

# Advance N days forward from the current simulation date
python -m tdgen_temporal.cli advance --days 30

# Fill an explicit date range (inclusive)
python -m tdgen_temporal.cli backfill --from 2024-01-01 --to 2024-03-31

# Print current date, total runs, and row counts for all tables
python -m tdgen_temporal.cli status

# Run all validation checks; --errors-only suppresses passing rows
python -m tdgen_temporal.cli validate --errors-only

# Non-default database or config
python -m tdgen_temporal.cli init --date 2024-01-01 --db other/path.db --config other/scenario.yaml
```

All commands default to `--db output/state.db` and `--config config/scenario.yaml`.

---

## Configuration (config/scenario.yaml)

The key knobs for a demo:

| Section | Parameter | What it controls |
|---|---|---|
| `simulation.initial_population` | `accounts` | Entities seeded on Day 0 |
| `simulation.initial_population` | `merchants` | Counterparty pool size |
| `simulation.seed` | integer | Reproducibility — change for a different but repeatable dataset |
| `rates` | `transactions_per_account_per_day_mean` | Volume of events per day |
| `rates` | `fraud_rate` / `dispute_rate` | Risk event frequency |
| `lifecycle` | `dispute_resolution_days` | How fast disputes cycle through |

Edit the file, then re-run `init` (or just `advance`) for the new values to take effect.
`init` parameters (population sizes) require a full re-initialise;
`rates` and `lifecycle` parameters apply to the next `advance` or `backfill` run.

---

## CI pipeline (GitHub Actions)

Defined in `.github/workflows/ci.yml`. Four jobs run on every push:

1. **Lint** — `ruff format --check` + `ruff check`
2. **Type check** — `mypy tdgen_temporal`
3. **Tests** — `pytest` on Python 3.10, 3.11, 3.12 in parallel
4. **Smoke test** — `init → advance 7 days → validate` end-to-end on Ubuntu

All four must pass before merging to `main`.

---

## Troubleshooting

**`dot: command not found` / ER diagram blank**
Graphviz is not on PATH. Re-install from https://graphviz.org/download/ and tick
"Add to system PATH" during setup. Restart PowerShell and verify with `dot -V`.

**`ModuleNotFoundError: No module named 'tdgen_temporal'`**
The venv is not activated. Run `.\.venv\Scripts\Activate.ps1` first.

**Validation reports temporal violations after init**
Run a fresh `init` to reseed — do not run `advance` on a database that was created
by an older version of the engine before the open_date guards were added.

**Streamlit shows stale data after Reset**
Click the browser refresh button (F5) after clicking Reset in the UI — Streamlit
caches state across reruns within a session.

**`pandera` or `openpyxl` not found**
Install from requirements.txt, not just pyproject.toml:
```powershell
pip install -r requirements.txt
```
