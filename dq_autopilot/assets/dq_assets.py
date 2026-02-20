import os
import json
import datetime as dt
import pandas as pd
import requests
from dagster import asset, AssetExecutionContext

def _github_issue(title: str, body: str) -> None:
    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("GITHUB_REPO")  # "user/repo"
    if not token or not repo:
        return
    url = f"https://api.github.com/repos/{repo}/issues"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}
    payload = {"title": title, "body": body}
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
    r.raise_for_status()

def _ensure_results_table(engine) -> None:
    sql = """
    CREATE SCHEMA IF NOT EXISTS dq;

    CREATE TABLE IF NOT EXISTS dq.dq_results (
        run_ts TIMESTAMP NOT NULL,
        schema_name TEXT NOT NULL,
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        row_count BIGINT NOT NULL,
        null_pct DOUBLE PRECISION NOT NULL,
        distinct_pct DOUBLE PRECISION NOT NULL
    );
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

@asset(required_resource_keys={"postgres"})
def discovered_tables(context: AssetExecutionContext) -> pd.DataFrame:
    engine = context.resources.postgres
    q = """
    SELECT table_schema AS schema_name, table_name
    FROM information_schema.tables
    WHERE table_type='BASE TABLE'
      AND table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY table_schema, table_name;
    """
    df = pd.read_sql(q, engine)
    context.log.info(f"Discovered {len(df)} tables")
    return df

@asset(required_resource_keys={"postgres"})
def table_profiles(context: AssetExecutionContext, discovered_tables: pd.DataFrame) -> pd.DataFrame:
    engine = context.resources.postgres
    results = []

    for _, row in discovered_tables.iterrows():
        schema = row["schema_name"]
        table = row["table_name"]
        full = f'"{schema}"."{table}"'

        rc = pd.read_sql(f"SELECT COUNT(*) AS c FROM {full};", engine)["c"].iloc[0]

        cols = pd.read_sql(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %(s)s AND table_name = %(t)s
            ORDER BY ordinal_position;
            """,
            engine,
            params={"s": schema, "t": table},
        )["column_name"].tolist()

        for col in cols:
            colq = f'"{col}"'
            nulls = pd.read_sql(f"SELECT COUNT(*) AS n FROM {full} WHERE {colq} IS NULL;", engine)["n"].iloc[0]
            null_pct = (nulls / rc) if rc else 0.0

            distincts = pd.read_sql(f"SELECT COUNT(DISTINCT {colq}) AS d FROM {full};", engine)["d"].iloc[0]
            distinct_pct = (distincts / rc) if rc else 0.0

            results.append({
                "schema_name": schema,
                "table_name": table,
                "column_name": col,
                "row_count": int(rc),
                "null_pct": float(null_pct),
                "distinct_pct": float(distinct_pct),
            })

        context.log.info(f"Profiled {schema}.{table} ({len(cols)} cols)")

    return pd.DataFrame(results)

@asset(required_resource_keys={"postgres"})
def write_dq_results(context: AssetExecutionContext, table_profiles: pd.DataFrame) -> int:
    engine = context.resources.postgres
    _ensure_results_table(engine)

    run_ts = dt.datetime.utcnow()
    out = table_profiles.copy()
    out.insert(0, "run_ts", run_ts)

    with engine.begin() as conn:
        out.to_sql("dq_results", conn, schema="dq", if_exists="append", index=False)

    context.log.info(f"Wrote {len(out)} rows into dq.dq_results at {run_ts.isoformat()}Z")
    return int(len(out))

@asset
def dq_alerts(context: AssetExecutionContext, table_profiles: pd.DataFrame) -> dict:
    bad_nulls = table_profiles[table_profiles["null_pct"] > 0.20]
    summary = {"bad_null_columns": int(len(bad_nulls))}

    if summary["bad_null_columns"] > 0:
        title = "DQ Autopilot found issues"
        body = bad_nulls.head(10)[["schema_name","table_name","column_name","null_pct","row_count"]].to_markdown(index=False)
        _github_issue(title, f"### High null% columns (top 10)\n\n{body}")
        context.log.warning("Issues found (opened GitHub Issue if configured).")
    else:
        context.log.info("No issues found ✅")

    return summary