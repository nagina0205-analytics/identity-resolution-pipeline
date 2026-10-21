-- Every record that can be tied to a person, in one shape.
-- Anonymous web visits are left out here; they attach at the account level only.
select record_id, source, email, email_sha256, first_key, last_key, company_key, null::varchar as new_company_domain, batch
from {{ ref('stg_crm_contacts') }}
union all
select record_id, source, email, email_sha256, first_key, last_key, company_key, null, batch
from {{ ref('stg_provider_delivery') }}
union all
select record_id, source, null, email_sha256, null, null, company_key, null, batch
from {{ ref('stg_web_visits') }} where email_sha256 is not null
union all
select record_id, source, null, null, first_key, last_key, company_key, null, batch
from {{ ref('stg_linkedin_engagement') }}
union all
select record_id, source, null, null, first_key, last_key, company_key, new_company_domain, batch
from {{ ref('stg_job_changes') }}
