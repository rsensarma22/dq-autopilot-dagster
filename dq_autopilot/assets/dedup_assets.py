import hashlib
import datetime as dt
import pandas as pd
from dagster import asset, AssetExecutionContext
from dq_autopilot.assets.dq_assets import _github_issue 


def _sha256(text: str) -> str:
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()


def _ensure_tables(engine) -> None:
    ddl = """
    CREATE SCHEMA IF NOT EXISTS dq;

    CREATE TABLE IF NOT EXISTS dq.text_fingerprints (
      run_ts TIMESTAMP NOT NULL,
      dataset TEXT NOT NULL,
      split TEXT NOT NULL,
      row_id BIGINT NOT NULL,
      sha256 TEXT NOT NULL,
      text_len INT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dq.dedup_report (
      run_ts TIMESTAMP NOT NULL,
      dataset TEXT NOT NULL,
      sha256 TEXT NOT NULL,
      total_rows INT NOT NULL,
      splits TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dq.leakage_report (
      run_ts TIMESTAMP NOT NULL,
      dataset TEXT NOT NULL,
      split_a TEXT NOT NULL,
      split_b TEXT NOT NULL,
      overlap_rows INT NOT NULL,
      overlap_hashes INT NOT NULL
    );
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)


@asset(required_resource_keys={"postgres"})
def dataset_rows(context: AssetExecutionContext) -> pd.DataFrame:
    """Load the dataset we want to dedup/check."""
    engine = context.resources.postgres
    df = pd.read_sql(
        "SELECT row_id, dataset, split, text FROM raw.dataset_rows;",
        engine,
    )
    context.log.info(f"Loaded {len(df)} dataset rows")
    return df


@asset(required_resource_keys={"postgres"})
def text_fingerprints(context: AssetExecutionContext, dataset_rows: pd.DataFrame) -> int:
    """Compute SHA256 fingerprints and persist them."""
    engine = context.resources.postgres
    _ensure_tables(engine)

    run_ts = dt.datetime.utcnow()
    out = dataset_rows.copy()
    out["sha256"] = out["text"].apply(_sha256)
    out["text_len"] = out["text"].astype(str).apply(len)
    out = out[["dataset", "split", "row_id", "sha256", "text_len"]]
    out.insert(0, "run_ts", run_ts)

    with engine.begin() as conn:
        out.to_sql("text_fingerprints", conn, schema="dq", if_exists="append", index=False)

    context.log.info(f"Wrote {len(out)} fingerprints at {run_ts.isoformat()}Z")
    return int(len(out))


@asset(required_resource_keys={"postgres"})
def dedup_report(context: AssetExecutionContext, dataset_rows: pd.DataFrame) -> pd.DataFrame:
    """Exact duplicate groups within the dataset (same sha256 appears >1 times)."""
    engine = context.resources.postgres
    _ensure_tables(engine)

    run_ts = dt.datetime.utcnow()

    df = dataset_rows.copy()
    df["sha256"] = df["text"].apply(_sha256)

    grouped = (
        df.groupby(["dataset", "sha256"])
          .agg(total_rows=("row_id", "count"), splits=("split", lambda s: ",".join(sorted(set(s)))))
          .reset_index()
    )
    dupes = grouped[grouped["total_rows"] > 1].copy()
    dupes.insert(0, "run_ts", run_ts)

    if dupes.empty:
        context.log.info("No exact duplicate groups ✅")
        return dupes

    with engine.begin() as conn:
        dupes.to_sql("dedup_report", conn, schema="dq", if_exists="append", index=False)

    context.log.warning(f"Found {len(dupes)} exact duplicate groups")
    return dupes


@asset(required_resource_keys={"postgres"})
def leakage_report(context: AssetExecutionContext, dataset_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Exact leakage: same sha256 appears across splits (train vs val/test).
    """
    engine = context.resources.postgres
    _ensure_tables(engine)

    run_ts = dt.datetime.utcnow()

    df = dataset_rows.copy()
    df["split"] = df["split"].astype(str).str.lower().str.strip()
    df["sha256"] = df["text"].apply(_sha256)

    # Count overlaps between train and other splits
    train_hashes = set(df[df["split"] == "train"]["sha256"].tolist())

    def _overlap(split_b: str):
        b = df[df["split"] == split_b]
        overlap_b = b[b["sha256"].isin(train_hashes)]
        return {
            "run_ts": run_ts,
            "dataset": "demo_llm",
            "split_a": "train",
            "split_b": split_b,
            "overlap_rows": int(len(overlap_b)),
            "overlap_hashes": int(overlap_b["sha256"].nunique()),
        }

    # Handle common split aliases to avoid brittle failures.
    if "validation" in set(df["split"].tolist()) and "val" not in set(df["split"].tolist()):
        df.loc[df["split"] == "validation", "split"] = "val"

    rows = [_overlap("val"), _overlap("test")]
    out = pd.DataFrame(rows)

    with engine.begin() as conn:
        out.to_sql("leakage_report", conn, schema="dq", if_exists="append", index=False)

    context.log.warning(f"Leakage report written: val={out.loc[0,'overlap_rows']}, test={out.loc[1,'overlap_rows']}")
    return out

@asset
def dedup_alerts(context: AssetExecutionContext, dedup_report: pd.DataFrame, leakage_report: pd.DataFrame) -> dict:
    dup_groups = 0 if dedup_report is None else len(dedup_report)

    def _get_overlap_rows(report: pd.DataFrame, split_b: str) -> int:
        match = report[report["split_b"] == split_b]
        if match.empty:
            return 0
        return int(match["overlap_rows"].iloc[0])

    leak_val = _get_overlap_rows(leakage_report, "val")
    leak_test = _get_overlap_rows(leakage_report, "test")

    # simple thresholds for demo
    if dup_groups > 0 or leak_val > 0 or leak_test > 0:
        title = "Dataset dedup/leakage issues detected"
        body = (
            f"### Summary\n"
            f"- Exact duplicate groups: {dup_groups}\n"
            f"- Train↔Val exact leakage rows: {leak_val}\n"
            f"- Train↔Test exact leakage rows: {leak_test}\n"
        )
        _github_issue(title, body)
        context.log.warning("Dedup alerts fired (GitHub Issue created if configured).")

    return {"duplicate_groups": dup_groups, "leak_val_rows": leak_val, "leak_test_rows": leak_test}