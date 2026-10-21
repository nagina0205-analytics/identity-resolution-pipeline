-- Third-party data provider delivery (Parquet on S3), with intent scores.
select
    provider_id                              as record_id,
    'provider'                               as source,
    {{ clean_email('email') }}               as email,
    sha256({{ clean_email('email') }})       as email_sha256,
    {{ name_key('first_name') }}             as first_key,
    {{ name_key('last_name') }}              as last_key,
    lower(company_domain)                    as company_domain,
    {{ company_key('company_domain') }}      as company_key,
    title,
    intent_topic,
    intent_score,
    _batch                                   as batch
from {{ source('raw', 'raw_provider_delivery') }}
