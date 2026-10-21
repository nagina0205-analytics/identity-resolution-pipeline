{# Company names, domains and emails arrive in many forms. These macros reduce
   each one to a comparable key. "Acme Inc.", "ACME", "acme.io" all become "acme". #}

{% macro company_key(col) -%}
regexp_replace(
  regexp_replace(
    regexp_replace(lower(trim({{ col }})), '\.(com|io|ai|co|net)$', ''),
    '[^a-z0-9 ]', '', 'g'),
  '\s+(inc|labs|software|systems|group|ai|llc|corp)$', '')
{%- endmacro %}

{% macro email_domain_key(col) -%}
{{ company_key("split_part(" ~ col ~ ", '@', 2)") }}
{%- endmacro %}

{% macro clean_email(col) -%}
nullif(lower(trim({{ col }})), '')
{%- endmacro %}

{% macro name_key(col) -%}
regexp_replace(lower(trim({{ col }})), '[^a-z]', '', 'g')
{%- endmacro %}
