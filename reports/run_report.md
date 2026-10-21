# Pipeline run report

## Match quality (scored against ground truth)

| Metric | Value |
| --- | --- |
| True people | 425 |
| Resolved people | 422 |
| Pairwise precision | 0.999 |
| Pairwise recall | 0.9441 |
| F1 | 0.9708 |
| Clusters with two people | 1 |
| Records left unmatched | 117 |

## Cost per batch

| Batch | S3 objects read | S3 bytes read | Rows loaded | fct_signals rows processed | dbt seconds | Tests passed |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 5 | 320,793 | 2,464 | 2,172 | 2.99 | 10 |
| 2 | 4 | 296,378 | 2,172 | 2,172 | 0.85 | 10 |

Incremental vs full refresh on the last batch: **2,172 rows** processed instead of **4,344** (50% less work). Rerunning a batch reads 0 bytes from S3 because the load manifest skips files it has already loaded.

## Match rate alerts

None. Every source stayed within 5 points of the previous batch.
