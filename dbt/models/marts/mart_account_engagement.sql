-- Account-based view: which companies are showing buying signals, and who is engaged.
select
    account_domain,
    count(*)                                                         as signals,
    count(distinct person_key)                                       as known_people_engaged,
    count(*) filter (where signal_type in ('visit:/pricing', 'visit:/demo'))  as high_intent_visits,
    count(*) filter (where match_rule = 'anonymous')                 as anonymous_visits,
    count(*) filter (where signal_type like 'linkedin:%')            as linkedin_engagements,
    round(
        3 * count(*) filter (where signal_type in ('visit:/pricing', 'visit:/demo'))
      + 1 * count(*) filter (where signal_type like 'linkedin:%')
      + 2 * count(distinct person_key)
      + 0.5 * count(*) filter (where match_rule = 'anonymous'), 1)  as engagement_score
from {{ ref('fct_signals') }}
where account_domain is not null
group by account_domain
order by engagement_score desc
