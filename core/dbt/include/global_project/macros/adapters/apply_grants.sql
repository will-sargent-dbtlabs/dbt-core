{% macro get_show_grant_sql(relation) %}
{{ return(adapter.dispatch('get_show_grant_sql', 'dbt')(relation)) }}
{% endmacro %}

{% macro default__get_show_grant_sql(relation) %}
{{ return('show grants on table'(relation.schema)) }}
{% endmacro %}

{% macro get_grant_sql(relation, grant_config) %}
{{ return(adapter.dispatch('get_grant_sql', 'dbt')(relation, grant_config)) }}
{% endmacro %}

{% macro default__get_grant_sql(relation, grant_config) %}
{{ return(relation) }}
{% endmacro %}

{% macro get_revoke_sql(relation, grant_config) %}
{{ return(adapter.dispatch('get_revoke_sql', 'dbt')(relation, grant_config)) }}
{% endmacro %}

{% macro default__get_revoke_sql(relation, grant_config) %}
return
{% endmacro %}


{% macro apply_grants(revoke, relation, grant_config) %}
{{ return(adapter.dispatch('apply_grant', 'dbt')(revoke, relation, grant_config)) }}
{% endmacro %}

{% macro default__apply_grants(revoke, relation, grant_config) %}
{{ get_show_grant_sql() }}
{% return %}
{% endmacro %}
