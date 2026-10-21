-- LinkedIn engagement: a display name (often a nickname) and free-text company name.
with split as (
    select *,
        {{ name_key("split_part(full_name, ' ', 1)") }} as first_raw,
        {{ name_key("split_part(full_name, ' ', -1)") }} as last_raw
    from {{ source('raw', 'raw_linkedin_engagement') }}
)
select
    engagement_id                            as record_id,
    'linkedin'                               as source,
    coalesce(n.canonical, first_raw)         as first_key,
    last_raw                                 as last_key,
    {{ company_key('company_name') }}        as company_key,
    action,
    cast(ts as timestamp)                    as ts,
    _batch                                   as batch
from split
left join {{ ref('nicknames') }} n on n.nickname = split.first_raw
