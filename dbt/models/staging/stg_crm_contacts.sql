-- CRM contacts: the system of record. Clean the email and reduce the company to a key.
select
    crm_id                                   as record_id,
    'crm'                                    as source,
    {{ clean_email('email') }}               as email,
    sha256({{ clean_email('email') }})       as email_sha256,
    coalesce(n.canonical, {{ name_key('first_name') }}) as first_key,
    {{ name_key('last_name') }}              as last_key,
    {{ company_key('company') }}             as company_key,
    title,
    _batch                                   as batch
from {{ source('raw', 'raw_crm_contacts') }} c
left join {{ ref('nicknames') }} n on n.nickname = {{ name_key('first_name') }}
