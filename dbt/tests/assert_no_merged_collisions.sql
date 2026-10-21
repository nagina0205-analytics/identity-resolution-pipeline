-- A person must never hold two different CRM emails. If one does, two real people
-- were merged into one, which is the worst identity resolution error.
select c.person_key, count(distinct r.email) as crm_emails
from {{ ref('int_identity_clusters') }} c
join {{ ref('stg_crm_contacts') }} r using (record_id)
where c.person_key is not null and r.email is not null
group by c.person_key
having count(distinct r.email) > 1
