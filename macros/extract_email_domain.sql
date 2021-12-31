{% macro extract_email_domain(email) %}
{# This is the SQL to extract the email domain in the Snowflake Flavor of SQL #}
regexp_substr(lower({{ email }}), '@(.*)', 1, 1, 'e',1)
{% endmacro %}