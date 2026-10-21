-- Website visits. Form fills carry a SHA-256 of the email; most visits are anonymous
-- and only tell us the company (from the IP address).
select
    event_id                                 as record_id,
    'web'                                    as source,
    cookie_id,
    ts,
    page,
    nullif(email_sha256, '')                 as email_sha256,
    ip_company_domain,
    {{ company_key('ip_company_domain') }}   as company_key,
    _batch                                   as batch
from {{ source('raw', 'raw_web_visits') }}
