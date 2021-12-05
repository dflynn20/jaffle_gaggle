{% macro extract_email_domain(column_name) %}
{# This is the SQL to extract the email domain in the Snowflake Flavor of SQL #}
regexp_substr({{ column_name }}, '@(.*)', 1, 1, 'e',1)
{% endmacro %}