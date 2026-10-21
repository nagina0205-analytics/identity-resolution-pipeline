-- Alert threshold: CRM and provider records carry emails, so nearly all should match.
-- A drop below 95% usually means a source changed format upstream.
select source, batch, person_match_rate
from {{ ref('metrics_match_rate') }}
where source in ('crm', 'provider') and person_match_rate < 0.95
