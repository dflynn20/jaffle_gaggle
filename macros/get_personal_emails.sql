{% macro get_personal_emails() %}
{{ return(('gmail.com', 'outlook.com', 'yahoo.com', 'icloud.com', 'hotmail.com')) }}
{% endmacro %}