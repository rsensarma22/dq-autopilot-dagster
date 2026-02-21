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
def dq_failures(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Writes real DQ checks into dq.dq_failures:
      1) Uniqueness: raw.customers.customer_id must be unique
      2) Orphan FK: raw.orders.customer_id must exist in raw.customers.customer_id
    """
    engine = context.resources.postgres
    _ensure_failures_table(engine)

    run_ts = dt.datetime.utcnow()
    failures = []

    # --- Check 1: Uniqueness (customers.customer_id) ---
    dup_sql = """
    SELECT customer_id, COUNT(*) AS cnt
    FROM raw.customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    LIMIT 10;
    """
    dup_df = pd.read_sql(dup_sql, engine)

    if not dup_df.empty:
        failures.append({
            "run_ts": run_ts,
            "check_name": "uniqueness_customers_customer_id",
            "schema_name": "raw",
            "table_name": "customers",
            "column_name": "customer_id",
            "severity": "HIGH",
            "failure_count": int(dup_df["cnt"].sum() - len(dup_df)),  # approx extra rows
            "sample": dup_df.to_dict(orient="records"),
        })

    # --- Check 2: Orphan FK (orders.customer_id not in customers) ---
    orphan_sql = """
    SELECT o.order_id, o.customer_id
    FROM raw.orders o
    LEFT JOIN raw.customers c
      ON o.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
    ORDER BY o.order_id
    LIMIT 10;
    """
    orphan_df = pd.read_sql(orphan_sql, engine)

    if not orphan_df.empty:
        # also get full count (not just sample)
        orphan_count_sql = """
        SELECT COUNT(*) AS cnt
        FROM raw.orders o
        LEFT JOIN raw.customers c
          ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL;
        """
        orphan_cnt = int(pd.read_sql(orphan_count_sql, engine)["cnt"].iloc[0])

        failures.append({
            "run_ts": run_ts,
            "check_name": "fk_orders_customer_id_exists_in_customers",
            "schema_name": "raw",
            "table_name": "orders",
            "column_name": "customer_id",
            "severity": "HIGH",
            "failure_count": orphan_cnt,
            "sample": orphan_df.to_dict(orient="records"),
        })

    out = pd.DataFrame(failures)

    if out.empty:
        context.log.info("No DQ failures ✅")
        return out
    
    out["sample"] = out["sample"].apply(lambda x: json.dumps(x) if x is not None else None)

    # Write failures
    with engine.begin() as conn:
        out.to_sql("dq_failures", conn, schema="dq", if_exists="append", index=False)

    context.log.warning(f"Wrote {len(out)} failure checks into dq.dq_failures")
    return out

def _ensure_failures_table(engine) -> None:
    sql = """
    CREATE SCHEMA IF NOT EXISTS dq;

    CREATE TABLE IF NOT EXISTS dq.dq_failures (
        run_ts TIMESTAMP NOT NULL,
        check_name TEXT NOT NULL,
        schema_name TEXT NOT NULL,
        table_name TEXT NOT NULL,
        column_name TEXT,
        severity TEXT NOT NULL,
        failure_count BIGINT NOT NULL,
        sample TEXT,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

# @asset(required_resource_keys={"postgres"})
# def discovered_tables(context: AssetExecutionContext) -> pd.DataFrame:
#     engine = context.resources.postgres
#     q = """
#     SELECT table_schema AS schema_name, table_name
#     FROM information_schema.tables
#     WHERE table_type='BASE TABLE'
#       AND table_schema NOT IN ('pg_catalog', 'information_schema')
#     ORDER BY table_schema, table_name;
#     """
#     df = pd.read_sql(q, engine)
#     context.log.info(f"Discovered {len(df)} tables")
#     return df

@asset(required_resource_keys={"postgres"})
def discovered_tables(context: AssetExecutionContext) -> pd.DataFrame:
    engine = context.resources.postgres

    # ✅ Only scan raw schema (prevents profiling dq.* tables like dq_results)
    q = """
    SELECT table_schema AS schema_name, table_name
    FROM information_schema.tables
    WHERE table_type='BASE TABLE'
      AND table_schema = 'raw'
    ORDER BY table_schema, table_name;
    """
    df = pd.read_sql(q, engine)
    context.log.info(f"Discovered {len(df)} tables (raw schema only)")
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

# @asset
# def dq_alerts(context: AssetExecutionContext, table_profiles: pd.DataFrame) -> dict:
#     bad_nulls = table_profiles[table_profiles["null_pct"] > 0.20]
#     summary = {"bad_null_columns": int(len(bad_nulls))}

#     if summary["bad_null_columns"] > 0:
#         title = "DQ Autopilot found issues"
#         body = bad_nulls.head(10)[["schema_name","table_name","column_name","null_pct","row_count"]].to_markdown(index=False)
#         _github_issue(title, f"### High null% columns (top 10)\n\n{body}")
#         context.log.warning("Issues found (opened GitHub Issue if configured).")
#     else:
#         context.log.info("No issues found ✅")

#     return summary

@asset
def dq_alerts(context: AssetExecutionContext, dq_failures: pd.DataFrame) -> dict:
    if dq_failures is None or dq_failures.empty:
        context.log.info("No alerts ✅")
        return {"failure_checks": 0}

    # Build GitHub issue body
    title = "DQ Autopilot found failures"
    body_lines = ["### Failure checks", f"- Checks failed: {len(dq_failures)}", "", "### Details"]

    # keep it readable
    display_cols = ["check_name", "schema_name", "table_name", "column_name", "severity", "failure_count"]
    body_lines.append(dq_failures[display_cols].to_markdown(index=False))

    # include small samples if present
    for _, row in dq_failures.iterrows():
        if row.get("sample"):
            body_lines.append("")
            body_lines.append(f"**Sample for {row['check_name']}**")
            body_lines.append("```")
            body_lines.append(json.dumps(row["sample"], indent=2))
            body_lines.append("```")

    body = "\n".join(body_lines)
    _github_issue(title, body)
    context.log.warning("Alerts fired (GitHub Issue created if configured).")

    return {"failure_checks": int(len(dq_failures))}