# DQ Autopilot (Dagster + Postgres)

A lightweight “data quality autopilot” that auto-discovers tables in Postgres, profiles every column (row count, null %, distinct %), runs core data-quality checks (duplicates + orphan keys), and logs results over time.  
Built to be demoable locally in minutes and extensible to Snowflake/BigQuery later.

**Outputs**
- `dq.dq_results` → profiling snapshots per run
- `dq.dq_failures` → actionable failed checks per run (with samples)

---

## Architecture (high level)
Postgres (raw tables) → Dagster assets (discover → profile → checks → persist → alert) → `dq.*` tables + optional GitHub Issues

---

## Quickstart (local)

### Requirements
- Docker Desktop
- Python 3.10+ (recommended 3.11/3.12)

### 1) Clone + install
```bash
git clone https://github.com/rsensarma22/dq-autopilot-dagster.git
cd dq-autopilot-dagster

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

cp .env.example .env