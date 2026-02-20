# DQ Autopilot (Dagster + Postgres)

A data-quality autopilot that automatically discovers Postgres tables, profiles columns (row count, null %, distinct %), writes results to `dq.dq_results`, and can open a GitHub Issue when it detects problems.

## Quickstart (local)
Requirements: Docker Desktop + Python 3.9+

```bash
cd /Users/roshnisensarma/dq-autopilot-dagster

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
docker compose up -d

docker exec -i dq_postgres psql -U dq -d dqdb < sql/seed.sql

dagster dev