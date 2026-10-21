-- Match rate per source and batch: the number to watch for drift.
-- For web visits, only form fills (with an email hash) can match a person.
select
    s.source,
    s.batch,
    count(*)                                                              as records,
    count(*) filter (where s.person_key is not null)                     as matched_to_person,
    count(*) filter (where s.match_rule = 'ambiguous')                   as ambiguous,
    count(*) filter (where s.match_rule = 'anonymous')                   as anonymous,
    round(count(*) filter (where s.person_key is not null)::double
          / nullif(count(*) filter (where s.match_rule <> 'anonymous'), 0), 4) as person_match_rate,
    round(count(*) filter (where s.account_domain is not null)::double / count(*), 4) as account_match_rate
from (
    select record_id, source, batch, person_key, match_rule, account_domain from {{ ref('fct_signals') }}
    union all
    select c.record_id, c.source, c.batch, c.person_key, c.match_rule, d.domain
    from {{ ref('int_identity_clusters') }} c
    join {{ ref('stg_crm_contacts') }} r using (record_id)
    left join {{ ref('int_company_domains') }} d on d.company_key = r.company_key
) s
group by 1, 2
order by 1, 2
