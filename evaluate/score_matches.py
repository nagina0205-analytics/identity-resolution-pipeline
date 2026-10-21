"""Score the identity matcher against the ground truth.

Pairwise metrics, the standard way to grade entity resolution:
  precision  of all record pairs the matcher put in the same person, how many truly are one person
             (low precision = different people merged, which sends ads to the wrong person)
  recall     of all record pairs that truly are one person, how many the matcher linked
             (low recall = one person split into several, which undercounts engagement)

Anonymous website visits are excluded: they can only be matched to an account.

    python evaluate/score_matches.py
"""
import json
import os
from collections import Counter
from itertools import combinations
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent


def pairs(groups):
    out = set()
    for members in groups.values():
        out.update(combinations(sorted(members), 2))
    return out


def score(pred: pd.DataFrame, truth: pd.DataFrame) -> dict:
    """pred: record_id, person_key (null = unmatched). truth: record_id, person_id."""
    df = pred.merge(truth, on="record_id", how="inner")
    df = df[df["person_id"] != "anonymous"]
    true_groups = df.groupby("person_id")["record_id"].apply(list).to_dict()
    pred_groups = df[df["person_key"].notna()].groupby("person_key")["record_id"].apply(list).to_dict()
    tp_pairs, pred_pairs = pairs(true_groups), pairs(pred_groups)
    tp = len(tp_pairs & pred_pairs)
    precision = tp / len(pred_pairs) if pred_pairs else 1.0
    recall = tp / len(tp_pairs) if tp_pairs else 1.0

    # Merged people: predicted clusters that contain more than one true person
    merged = sum(1 for m in pred_groups.values() if df.set_index("record_id").loc[m, "person_id"].nunique() > 1)
    return {
        "records_scored": int(len(df)),
        "true_people": int(len(true_groups)),
        "resolved_people": int(len(pred_groups)),
        "pairwise_precision": round(precision, 4),
        "pairwise_recall": round(recall, 4),
        "f1": round(2 * precision * recall / (precision + recall), 4) if precision + recall else 0.0,
        "clusters_with_two_people": int(merged),
        "records_left_unmatched": int(df["person_key"].isna().sum()),
    }


def by_rule(pred, truth):
    df = pred.merge(truth, on="record_id")
    df = df[df["person_id"] != "anonymous"]
    return dict(Counter(df["match_rule"]))


def main():
    db = os.environ.get("DUCKDB_PATH", str(ROOT / "warehouse" / "identity.duckdb"))
    with duckdb.connect(db) as con:
        pred = con.execute("SELECT record_id, person_key, match_rule FROM int_identity_clusters").df()
        web = con.execute("SELECT record_id, NULL AS person_key, 'anonymous' AS match_rule FROM stg_web_visits WHERE email_sha256 IS NULL").df()
    truth = pd.read_csv(ROOT / "data" / "ground_truth.csv")
    pred = pd.concat([pred, web])
    result = score(pred, truth)
    result["records_by_rule"] = by_rule(pred, truth)
    (ROOT / "reports").mkdir(exist_ok=True)
    (ROOT / "reports" / "match_quality.json").write_text(json.dumps(result, indent=2))
    for k, v in result.items():
        print(f"{k:26} {v}")
    return result


if __name__ == "__main__":
    main()
