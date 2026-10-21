"""Prefect flow: the whole pipeline, one batch of deliveries at a time.

For each batch:
  1. land the batch's files in S3
  2. load only new S3 objects into DuckDB (manifest-based, so reruns read nothing)
  3. dbt build: staging, identity matching, marts, and every data test
  4. score match quality against the ground truth
  5. monitor: alert if a source's match rate drops more than 5 points from the
     previous batch, and record what the run cost (bytes read, rows processed, time)
Then export the marts as JSON for the audience service and dashboard.

    python flows/pipeline_flow.py            # fresh run over batches 1 and 2
"""
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import duckdb
from prefect import flow, get_run_logger, task

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ingest"))
sys.path.insert(0, str(ROOT / "evaluate"))
import s3_ingest  # noqa: E402
import score_matches  # noqa: E402

DBT_DIR = ROOT / "dbt"
DRIFT_ALERT_POINTS = 0.05


@task
def generate_batch(batch: int):
    subprocess.run([sys.executable, str(ROOT / "generate" / "make_signals.py"), "--batch", str(batch)], check=True)


@task(retries=2, retry_delay_seconds=5)
def ingest(batch: int):
    s3 = s3_ingest.s3_client()
    s3_ingest.upload_landing_files(s3)
    with duckdb.connect(s3_ingest.warehouse_path()) as con:
        stats = s3_ingest.load_new_objects(s3, con)
    get_run_logger().info(f"batch {batch}: loaded {len(stats)} new S3 objects")
    return stats


@task
def dbt_build(full_refresh: bool = False):
    from dbt.cli.main import dbtRunner
    args = ["build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR), "--quiet"]
    if full_refresh:
        args.append("--full-refresh")
    start = time.perf_counter()
    res = dbtRunner().invoke(args)
    seconds = round(time.perf_counter() - start, 2)
    if not res.success:
        failed = [r.node.name for r in (res.result or []) if str(r.status) in ("fail", "error")]
        raise RuntimeError(f"dbt build failed: {failed}")
    tests = sum(1 for r in res.result if r.node.resource_type == "test")
    return {"seconds": seconds, "nodes": len(res.result), "tests_passed": tests}


@task
def evaluate():
    return score_matches.main()


def detect_drift(rates, batch, threshold=DRIFT_ALERT_POINTS):
    """rates: DataFrame with source, batch, person_match_rate. Returns alert strings."""
    alerts = []
    for src, g in rates.groupby("source"):
        g = g.set_index("batch")["person_match_rate"]
        if batch in g and batch - 1 in g and g[batch - 1] - g[batch] > threshold:
            alerts.append(f"{src}: match rate fell from {g[batch-1]:.1%} to {g[batch]:.1%}")
    return alerts


@task
def monitor(batch: int, ingest_stats, build_stats, fct_rows_before: int):
    log = get_run_logger()
    with duckdb.connect(s3_ingest.warehouse_path()) as con:
        rates = con.execute("SELECT source, batch, person_match_rate FROM metrics_match_rate").df()
        fct_rows_after = con.execute("SELECT count(*) FROM fct_signals").fetchone()[0]
    alerts = detect_drift(rates, batch) if batch > 1 else []
    for a in alerts:
        log.warning(f"MATCH RATE ALERT {a}")
    cost = {
        "batch": batch,
        "s3_objects_read": len(ingest_stats),
        "s3_bytes_read": sum(s["bytes"] for s in ingest_stats),
        "rows_loaded": sum(s["rows"] for s in ingest_stats),
        "fct_signals_rows_processed": fct_rows_after - fct_rows_before,
        "dbt_seconds": build_stats["seconds"],
        "dbt_tests_passed": build_stats["tests_passed"],
    }
    return {"alerts": alerts, "cost": cost}


@task
def fct_row_count():
    try:
        with duckdb.connect(s3_ingest.warehouse_path()) as con:
            return con.execute("SELECT count(*) FROM fct_signals").fetchone()[0]
    except Exception:
        return 0


@task
def full_refresh_benchmark():
    """What the same run costs without incremental models: rebuild everything from scratch."""
    stats = dbt_build.fn(full_refresh=True)
    with duckdb.connect(s3_ingest.warehouse_path()) as con:
        rows = con.execute("SELECT count(*) FROM fct_signals").fetchone()[0]
    return {"fct_signals_rows_processed": rows, "dbt_seconds": stats["seconds"]}


@task
def export_for_apps():
    out = ROOT / "exports"
    out.mkdir(exist_ok=True)
    with duckdb.connect(s3_ingest.warehouse_path()) as con:
        q = {
            "people": "SELECT person_key, email_sha256, first_name, last_name, title, current_company_domain, changed_jobs, source_count, in_crm FROM dim_person",
            "accounts": "SELECT * FROM mart_account_engagement",
            "match_rates": "SELECT * FROM metrics_match_rate",
            "person_signals": """SELECT person_key, count(*) AS signals,
                   count(*) FILTER (WHERE signal_type IN ('visit:/pricing','visit:/demo')) AS high_intent_visits
                   FROM fct_signals WHERE person_key IS NOT NULL GROUP BY person_key""",
        }
        for name, sql in q.items():
            con.execute(sql).df().to_json(out / f"{name}.json", orient="records", indent=1)
    return sorted(p.name for p in out.glob("*.json"))


def write_report(runs, quality, benchmark):
    lines = ["# Pipeline run report", "",
             "## Match quality (scored against ground truth)", "",
             "| Metric | Value |", "| --- | --- |"]
    for k in ("true_people", "resolved_people", "pairwise_precision", "pairwise_recall", "f1",
              "clusters_with_two_people", "records_left_unmatched"):
        lines.append(f"| {k.replace('_', ' ').capitalize()} | {quality[k]} |")
    lines += ["", "## Cost per batch", "",
              "| Batch | S3 objects read | S3 bytes read | Rows loaded | fct_signals rows processed | dbt seconds | Tests passed |",
              "| --- | --- | --- | --- | --- | --- | --- |"]
    for r in runs:
        c = r["cost"]
        lines.append(f"| {c['batch']} | {c['s3_objects_read']} | {c['s3_bytes_read']:,} | {c['rows_loaded']:,} | "
                     f"{c['fct_signals_rows_processed']:,} | {c['dbt_seconds']} | {c['dbt_tests_passed']} |")
    if benchmark:
        inc = runs[-1]["cost"]["fct_signals_rows_processed"]
        full = benchmark["fct_signals_rows_processed"]
        lines += ["", f"Incremental vs full refresh on the last batch: **{inc:,} rows** processed instead of "
                  f"**{full:,}** ({1 - inc / full:.0%} less work). Rerunning a batch reads 0 bytes from S3 "
                  "because the load manifest skips files it has already loaded."]
    alerts = [a for r in runs for a in r["alerts"]]
    lines += ["", "## Match rate alerts", ""] + ([f"- {a}" for a in alerts] or ["None. Every source stayed within 5 points of the previous batch."])
    (ROOT / "reports").mkdir(exist_ok=True)
    (ROOT / "reports" / "run_report.md").write_text("\n".join(lines) + "\n")
    (ROOT / "reports" / "run_metrics.json").write_text(json.dumps({"runs": runs, "quality": quality, "benchmark": benchmark}, indent=2))


@flow(name="identity-resolution-pipeline")
def pipeline(batches: list[int] = [1, 2], fresh: bool = True, benchmark: bool = True):
    if fresh:
        shutil.rmtree(ROOT / "data", ignore_errors=True)
        shutil.rmtree(ROOT / "warehouse", ignore_errors=True)
    os.environ.setdefault("DUCKDB_PATH", s3_ingest.warehouse_path())
    runs, quality = [], None
    for b in batches:
        generate_batch(b)
        before = fct_row_count()
        stats = ingest(b)
        build = dbt_build()
        quality = evaluate()
        runs.append(monitor(b, stats, build, before))
    rerun = ingest(batches[-1])            # idempotency check: should load nothing
    assert rerun == [], "rerun loaded files twice"
    bench = full_refresh_benchmark() if benchmark else None
    export_for_apps()
    write_report(runs, quality, bench)
    return {"runs": runs, "quality": quality, "benchmark": bench}


if __name__ == "__main__":
    if os.environ.get("S3_BUCKET"):
        result = pipeline()
    else:
        from moto import mock_aws
        with mock_aws():
            result = pipeline()
    print(json.dumps({"quality": result["quality"], "costs": [r["cost"] for r in result["runs"]],
                      "benchmark": result["benchmark"]}, indent=1, default=str))
