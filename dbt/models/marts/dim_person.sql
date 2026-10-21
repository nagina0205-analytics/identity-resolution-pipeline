-- One row per resolved person: best email, name, title and current company.
-- A job change overrides the company the person was first seen at.
-- Some people are known only by an email hash from a form fill; that is enough to target them.
with linked as (
    select c.person_key, r.*
    from {{ ref('int_identity_clusters') }} c
    join {{ ref('int_identity_records') }} r using (record_id)
    where c.person_key is not null
),
latest_job as (
    select person_key, new_company_domain,
           row_number() over (partition by person_key order by batch desc, record_id desc) as rn
    from linked where new_company_domain is not null
),
best as (
    select person_key,
        -- prefer the CRM email, then the provider's
        coalesce(min(email) filter (where source = 'crm'), min(email))            as email,
        min(email_sha256)                                                         as email_sha256,
        min(first_key)                                                            as first_name,
        min(last_key)                                                             as last_name,
        mode(company_key)                                                         as first_company_key,
        count(distinct source)                                                    as source_count,
        count(*)                                                                  as record_count,
        bool_or(source = 'crm')                                                   as in_crm
    from linked group by person_key
)
select b.person_key, b.email, b.email_sha256, (b.email is null) as hashed_email_only, b.first_name, b.last_name,
       coalesce(j.new_company_domain, d.domain)  as current_company_domain,
       (j.new_company_domain is not null)        as changed_jobs,
       b.source_count, b.record_count, b.in_crm,
       t.title
from best b
left join latest_job j on j.person_key = b.person_key and j.rn = 1
left join {{ ref('int_company_domains') }} d on d.company_key = b.first_company_key
left join (
    select person_key, min(title) as title from (
        select c.person_key, s.title from {{ ref('int_identity_clusters') }} c
        join {{ ref('stg_crm_contacts') }} s using (record_id)
        union all
        select c.person_key, s.title from {{ ref('int_identity_clusters') }} c
        join {{ ref('stg_provider_delivery') }} s using (record_id)
    ) where person_key is not null group by person_key
) t on t.person_key = b.person_key
