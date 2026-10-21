"""Unit tests for the matcher, the scorer, S3 ingestion and drift alerts.

    pytest -q
"""
import importlib.util
import sys
from pathlib import Path

import boto3
import duckdb
import pandas as pd
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / "ingest"), str(ROOT / "evaluate"), str(ROOT / "flows")]


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


matcher = load(ROOT / "dbt/models/intermediate/int_identity_clusters.py", "matcher")
import s3_ingest  # noqa: E402
import score_matches  # noqa: E402


def rec(rid, src, sha=None, first=None, last=None, company=None):
    return {"record_id": rid, "source": src, "email_sha256": sha, "first_key": first,
            "last_key": last, "company_key": company, "batch": 1}


def resolve(rows):
    return matcher.resolve(pd.DataFrame(rows)).set_index("record_id")


# ---------- matcher ----------
def test_same_email_hash_is_one_person():
    r = resolve([rec("crm-1", "crm", "h1", "ana", "silva", "acme"), rec("web-1", "web", "h1")])
    assert r.loc["crm-1", "person_key"] == r.loc["web-1", "person_key"] is not None


def test_name_and_company_links_records_without_email():
    r = resolve([rec("crm-1", "crm", "h1", "robert", "kim", "acme"), rec("li-1", "linkedin", None, "robert", "kim", "acme")])
    assert r.loc["li-1", "person_key"] == r.loc["crm-1", "person_key"]
    assert r.loc["li-1", "match_rule"] == "name_company"


def test_name_collision_is_not_merged():
    r = resolve([rec("crm-1", "crm", "h1", "wei", "chen", "acme"),
                 rec("crm-2", "crm", "h2", "wei", "chen", "acme"),
                 rec("li-1", "linkedin", None, "wei", "chen", "acme")])
    assert r.loc["crm-1", "person_key"] != r.loc["crm-2", "person_key"]
    assert r.loc["li-1", "match_rule"] == "ambiguous"
    assert pd.isna(r.loc["li-1", "person_key"])


def test_unknown_record_gets_no_invented_identity():
    r = resolve([rec("li-9", "linkedin", None, "zara", "novak", "tidal")])
    assert pd.isna(r.loc["li-9", "person_key"]) and r.loc["li-9", "match_rule"] == "unresolved"


def test_person_key_is_stable_when_new_records_arrive():
    a = resolve([rec("crm-1", "crm", "h1", "ana", "silva", "acme")])
    b = resolve([rec("crm-1", "crm", "h1", "ana", "silva", "acme"), rec("aaa-0", "web", "h1")])
    assert a.loc["crm-1", "person_key"] == b.loc["crm-1", "person_key"]


# ---------- scorer ----------
def test_scorer_counts_a_wrong_merge():
    truth = pd.DataFrame({"record_id": ["a", "b", "c"], "person_id": ["P1", "P1", "P2"]})
    pred = pd.DataFrame({"record_id": ["a", "b", "c"], "person_key": ["k", "k", "k"]})
    s = score_matches.score(pred, truth)
    assert s["pairwise_recall"] == 1.0 and s["pairwise_precision"] == pytest.approx(1 / 3, abs=1e-3)
    assert s["clusters_with_two_people"] == 1


# ---------- S3 ingestion ----------
@mock_aws
def test_s3_load_is_incremental_and_idempotent(tmp_path, monkeypatch):
    landing = tmp_path / "landing" / "batch=1"
    landing.mkdir(parents=True)
    (landing / "job_changes.csv").write_text("change_id,first_name\njc-1,Ana\n")
    monkeypatch.setattr(s3_ingest, "BUCKET", "test-bucket")
    s3 = boto3.client("s3", region_name="us-east-1")
    s3_ingest.upload_landing_files(s3, landing=tmp_path / "landing")
    con = duckdb.connect()
    first = s3_ingest.load_new_objects(s3, con)
    second = s3_ingest.load_new_objects(s3, con)
    assert len(first) == 1 and first[0]["rows"] == 1
    assert second == []                      # nothing new, nothing read
    assert con.execute("SELECT count(*) FROM raw_job_changes").fetchone()[0] == 1


# ---------- monitoring ----------
def test_drift_alert_fires_on_a_drop():
    import pipeline_flow
    rates = pd.DataFrame({"source": ["crm", "crm", "web", "web"], "batch": [1, 2, 1, 2],
                          "person_match_rate": [0.98, 0.80, 0.90, 0.89]})
    alerts = pipeline_flow.detect_drift(rates, 2)
    assert len(alerts) == 1 and alerts[0].startswith("crm")
