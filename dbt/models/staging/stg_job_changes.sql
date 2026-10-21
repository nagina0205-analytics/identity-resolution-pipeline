-- Job changes: the person is found at the old company, then moves to the new one.
select
    change_id                                as record_id,
    'job_change'                             as source,
    {{ name_key('first_name') }}             as first_key,
    {{ name_key('last_name') }}              as last_key,
    {{ company_key('old_company') }}         as company_key,
    lower(trim(new_company_domain))          as new_company_domain,
    cast(changed_on as date)                 as changed_on,
    _batch                                   as batch
from {{ source('raw', 'raw_job_changes') }}
