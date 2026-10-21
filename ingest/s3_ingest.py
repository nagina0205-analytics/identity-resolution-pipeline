"""Land signal files in S3, then load only the new ones into the warehouse.

Two steps, the way provider deliveries usually work:
  1. upload_landing_files: push each file from data/landing/ to
     s3://<bucket>/raw/<source>/batch=<n>/<file>
  2. load_new_objects: list the bucket, skip any object whose key and ETag are
     already in the load manifest, and load the rest into raw_* tables in DuckDB.

Loading only new objects is what keeps reruns cheap: a second run over the same
bucket reads zero bytes. The manifest also makes the load idempotent, so a retry
after a failure never double counts.

S3: set S3_BUCKET (and normal AWS credentials) to use a real bucket. Without
them, the pipeline runs against an in-memory S3 from the moto library, so it
works anywhere, including CI.
Warehouse: a local DuckDB file, or MotherDuck when MOTHERDUCK_TOKEN is set.
"""
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import duckdb

ROOT = Path(__file__).resolve().parent.parent
LANDING = ROOT / "data" / "landing"
BUCKET = os.environ.get("S3_BUCKET", "vector-signals-demo")

SOURCES = {  # file name -> (source, raw table, DuckDB reader)
    "crm_contacts.csv": ("crm", "raw_crm_contacts", "read_csv_auto('{f}', all_varchar=true)"),
    "web_visits.jsonl": ("web", "raw_web_visits", "read_json_auto('{f}', format='newline_delimited')"),
    "linkedin_engagement.csv": ("linkedin", "raw_linkedin_engagement", "read_csv_auto('{f}', all_varchar=true)"),
    "job_changes.csv": ("job_change", "raw_job_changes", "read_csv_auto('{f}', all_varchar=true)"),
    "provider_delivery.parquet": ("provider", "raw_provider_delivery", "read_parquet('{f}')"),
}


def warehouse_path():
    if os.environ.get("MOTHERDUCK_TOKEN"):
        return "md:identity_graph"
    (ROOT / "warehouse").mkdir(exist_ok=True)
    return str(ROOT / "warehouse" / "identity.duckdb")


def s3_client():
    return boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)


def upload_landing_files(s3, landing=LANDING):
    """Upload every landing file. Returns the keys written."""
    ensure_bucket(s3)
    keys = []
    for path in sorted(landing.glob("batch=*/*")):
        if path.name not in SOURCES:
            continue
        source = SOURCES[path.name][0]
        key = f"raw/{source}/{path.parent.name}/{path.name}"
        s3.upload_file(str(path), BUCKET, key)
        keys.append(key)
    return keys


def load_new_objects(s3, con):
    """Load objects not yet in the manifest. Returns a list of per-file load stats."""
    con.execute("""CREATE TABLE IF NOT EXISTS load_manifest (
        s3_key VARCHAR, etag VARCHAR, raw_table VARCHAR, rows_loaded BIGINT,
        bytes_read BIGINT, seconds DOUBLE, loaded_at TIMESTAMP)""")
    done = {(k, e) for k, e in con.execute("SELECT s3_key, etag FROM load_manifest").fetchall()}

    stats = []
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="raw/")
    objects = [o for page in pages for o in page.get("Contents", [])]
    for obj in sorted(objects, key=lambda o: o["Key"]):
        key, etag, size = obj["Key"], obj["ETag"].strip('"'), obj["Size"]
        name = key.rsplit("/", 1)[1]
        if name not in SOURCES or (key, etag) in done:
            continue
        _, table, reader = SOURCES[name]
        batch = key.split("/")[2].replace("batch=", "")
        start = time.perf_counter()
        with tempfile.TemporaryDirectory() as tmp:
            local = Path(tmp) / name
            s3.download_file(BUCKET, key, str(local))
            select = f"SELECT *, '{key}' AS _s3_key, {int(batch)} AS _batch, now() AS _loaded_at FROM {reader.format(f=local)}"
            exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]).fetchone()[0]
            if exists:
                n = con.execute(f"INSERT INTO {table} BY NAME {select}").fetchone()[0]
            else:
                con.execute(f"CREATE TABLE {table} AS {select}")
                n = con.execute(f"SELECT count(*) FROM {table} WHERE _s3_key = ?", [key]).fetchone()[0]
        secs = round(time.perf_counter() - start, 3)
        con.execute("INSERT INTO load_manifest VALUES (?,?,?,?,?,?,?)",
                    [key, etag, table, n, size, secs, datetime.now(timezone.utc)])
        stats.append({"key": key, "table": table, "rows": n, "bytes": size, "seconds": secs})
    return stats


if __name__ == "__main__":
    from moto import mock_aws
    if os.environ.get("S3_BUCKET"):
        s3 = s3_client()
        upload_landing_files(s3)
        with duckdb.connect(warehouse_path()) as con:
            print(load_new_objects(s3, con))
    else:
        with mock_aws():
            s3 = s3_client()
            upload_landing_files(s3)
            with duckdb.connect(warehouse_path()) as con:
                for s in load_new_objects(s3, con):
                    print(s)
