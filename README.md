# Identity Resolution Pipeline

B2B buying signals arrive from everywhere: website visits, LinkedIn engagement, CRM contacts, job changes, and large deliveries from data providers. Each source names people differently. The CRM says Robert Kim at Acme Inc. LinkedIn says Bob Kim at ACME. The website only has a hash of his email. A provider file has robert.kim@acme.io.

This pipeline lands those signals in S3, models them with dbt on DuckDB, works out which records belong to the same real person, and tracks how accurate that matching is and what each run costs. Prefect runs it end to end.

All people, companies and emails are synthetic. `generate/make_signals.py` creates them along with the right answers, so match quality can be measured instead of guessed.

## Results (latest run)

| Metric | Value |
| --- | --- |
| Pairwise precision | 99.9% |
| Pairwise recall | 94.4% |
| F1 | 0.97 |
| People resolved | 422 of 425 |
| Records left unmatched on purpose | 117 (ambiguous or no evidence) |
| dbt data tests | 10 passing |
| Unit tests | 8 passing |

Full numbers: [reports/run_report.md](reports/run_report.md). GitHub Actions reruns the flow daily and on every push.

## How it works

```
data provider / web / CRM / LinkedIn files
        |
        v
S3  raw/<source>/batch=<n>/<file>          ingest/s3_ingest.py (boto3)
        |   load only objects not in the manifest
        v
DuckDB raw_* tables                         local file, or MotherDuck with a token
        |
        v
dbt  staging    clean emails, map nicknames, reduce company names to one key
     intermediate  int_identity_clusters (Python model): who is who
     marts      dim_person, fct_signals (incremental), mart_account_engagement, metrics_match_rate
        |
        v
Prefect flow  score vs ground truth, match-rate drift alerts, cost per batch, JSON exports
```

### Matching rules

1. **Email first.** Records with the same email, or the same SHA-256 hash of an email, are one person.
2. **Name and company second.** Same first name (Bob and Robert count as the same), same last name, same company key. Acme Inc., ACME and acme.io all reduce to `acme`.
3. **Collision guard.** If two people with different emails share a name at the same company, records without an email are marked `ambiguous` and left unmatched. Attaching them to the wrong person is worse than not attaching them at all.
4. **Job changes** move a person to their new company without creating a new person.
5. **Stable keys.** A person's key comes from their email, so it doesn't change when new signals arrive.

### The tradeoff, in numbers

The pipeline chooses precision over recall. 56 records were left `ambiguous` and 61 `unresolved`, which costs about 5 points of recall. The alternative is merging different people, which sends ads to the wrong person and corrupts account scores.

One wrong merge is still in the results, and it's left there on purpose. Two different people named Margaret Kim are linked to the same company, and only one has an email anywhere in the data. Name and company can't tell them apart. The fix would be a stronger signal, such as a LinkedIn profile URL or a provider person ID. It's a known limit, not something to hide.

### Cost

| | Batch 1 | Batch 2 |
| --- | --- | --- |
| S3 objects read | 5 | 4 (only the new delivery) |
| fct_signals rows processed | 2,172 | 2,172 (a full rebuild would process 4,344) |

`fct_signals` is an incremental dbt model, so each run processes only batches it hasn't seen: half the work on the second batch, and the savings grow with every delivery. Rerunning a batch reads 0 bytes from S3, because the load manifest records each object's key and ETag. That also makes the load safe to retry.

### Monitoring

- `metrics_match_rate` tracks match rate per source and batch.
- The flow raises an alert if any source drops more than 5 points from the previous batch. A sudden drop usually means a provider changed its format.
- dbt tests fail the run if CRM or provider match rate falls below 95%, or if any person ends up with two different CRM emails (a wrong merge).

## Run it

```
pip install -r requirements.txt
pytest -q tests
python flows/pipeline_flow.py
```

S3 runs against an in-memory mock (moto) by default, so it works anywhere. To use a real bucket, set `S3_BUCKET` and your AWS credentials. To run the warehouse on MotherDuck instead of a local DuckDB file, set `MOTHERDUCK_TOKEN`. The MotherDuck path is configured but not tested in CI, since it needs an account.

## Project layout

```
generate/make_signals.py        synthetic signals + ground truth
ingest/s3_ingest.py             S3 upload and manifest-based incremental load
dbt/models/staging              one cleaning model per source
dbt/models/intermediate         company keys, identity records, the matcher (Python model)
dbt/models/marts                people, signals, account engagement, match rates
dbt/tests                       wrong-merge and match-rate-floor tests
evaluate/score_matches.py       pairwise precision and recall vs ground truth
flows/pipeline_flow.py          Prefect flow, monitoring, cost report, exports
exports/                        JSON used by the audience service and dashboard
```
