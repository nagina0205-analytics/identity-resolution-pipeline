{{ config(materialized='incremental', unique_key='record_id') }}
-- Every signal, tied to a person when we know who it is and always to an account.
-- Incremental: after the first build, each run only processes batches it has not seen.
with signals as (
    select record_id, source, 'visit:' || page as signal_type, ts, company_key, batch from {{ ref('stg_web_visits') }}
    union all
    select record_id, source, 'linkedin:' || action, ts, company_key, batch from {{ ref('stg_linkedin_engagement') }}
    union all
    select record_id, source, 'job_change', cast(changed_on as timestamp), company_key, batch from {{ ref('stg_job_changes') }}
    union all
    select record_id, source, 'intent:' || intent_topic, null::timestamp, company_key, batch from {{ ref('stg_provider_delivery') }}
)
select s.record_id, s.source, s.signal_type, s.ts, s.batch,
       c.person_key,
       coalesce(c.match_rule, 'anonymous') as match_rule,
       d.domain as account_domain
from signals s
left join {{ ref('int_identity_clusters') }} c using (record_id)
left join {{ ref('int_company_domains') }} d on d.company_key = s.company_key
{% if is_incremental() %}
where s.batch > (select coalesce(max(batch), 0) from {{ this }})
{% endif %}
