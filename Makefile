.PHONY: up seed dev down reset-db

up:
	docker compose up -d

seed:
	docker exec -i dq_postgres psql -U dq -d dqdb < sql/seed.sql

dev:
	. .venv/bin/activate && dagster dev -m dq_autopilot.definitions

down:
	docker compose down

reset-db:
	docker compose down --remove-orphans
	docker volume rm dq-autopilot-dagster_dq_pgdata 2>/dev/null || true
	docker compose up -d