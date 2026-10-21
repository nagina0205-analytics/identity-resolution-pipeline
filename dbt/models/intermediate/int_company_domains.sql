-- One row per company key with its web domain, built from every source that carries a domain.
with domains as (
    select split_part(email, '@', 2) as domain from {{ ref('stg_crm_contacts') }} where email is not null
    union all select company_domain from {{ ref('stg_provider_delivery') }}
    union all select ip_company_domain from {{ ref('stg_web_visits') }} where ip_company_domain is not null
    union all select new_company_domain from {{ ref('stg_job_changes') }}
)
select {{ company_key('domain') }} as company_key, min(domain) as domain, count(*) as evidence
from domains
group by 1
