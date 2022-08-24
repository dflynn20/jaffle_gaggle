{% macro extract_email_domain_bq(email) %}
{# This is the SQL to extract the email domain in the BigQuery Flavor of SQL #}
REGEXP_EXTRACT(lower({{ email }}), r'@(.*)')
{% endmacro %}
