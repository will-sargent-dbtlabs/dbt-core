{% macro get_show_grant_sql(relation) %}
{{ return(adapter.dispatch("get_show_grant_sql", "dbt")(relation)) }}
{% endmacro %}

{% macro default__get_show_grant_sql(relation) %}
show grants on {{ relation.type }} {{ relation }}
{% endmacro %}

{% macro get_grant_sql(relation, grant_config) %}
{{ return(adapter.dispatch('get_grant_sql', 'dbt')(relation, grant_config)) }}
{% endmacro %}

{% macro default__get_grant_sql(relation, grant_config) %}
    {% for privilege in grant_config.keys() %}
        {% set grantees = grant_config[privilege] %}
        {% if grantees %}
            {% for grantee in grantees %}
                grant {{ privilege }} on {{ relation.type }} {{ relation }} to {{ grantee}};
            {% endfor %}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro get_revoke_sql(relation, grant_config) %}
{{ return(adapter.dispatch("get_revoke_sql", "dbt")(relation, grant_config)) }}
{% endmacro %}

{% macro default__get_revoke_sql(relation, grant_config) %}
    {% for privilege in grant_config.keys() %}
        {% set grantees = [] %}
        {% set all_grantees = grant_config[privilege] %}
        {% for grantee in all_grantees %}
            {% if grantee != target.user %}
                {% do grantees.append(grantee) %}
            {% endif %}
        {% endfor%}
        {% if grantees %}
                {% for grantee in grantees %}
                    revoke {{ privilege }} on {{ relation.type }} {{ relation }} from {{ grantee }};
                {% endfor %}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro apply_grants(relation, grant_config, should_revoke) %}
{{ return(adapter.dispatch("apply_grants", "dbt")(relation, grant_config, should_revoke)) }}
{% endmacro %}

{% macro default__apply_grants(relation, grant_config, should_revoke=True) %}
    {% if grant_config %}
            {% if should_revoke %}
                {% set current_grants_table =  run_query(get_show_grant_sql(relation)) %}
                {% set current_grants_dict = adapter.standardize_grants_dict(current_grants_table) %}
                {% set needs_granting = diff_of_two_dicts(grant_config, current_grants_dict) %}
                {% set needs_revoking = diff_of_two_dicts(current_grants_dict, grant_config) %}
                {% if not (needs_granting or needs_revoking) %}
                    {{ log('All grants are in place, no revocation or granting needed.')}}
                {% endif %}
            {% else %}
                {% set needs_revoking = {} %}
                {% set needs_granting = grant_config %}
            {% endif %}
            {% if needs_granting or needs_revoking %}
                {% call statement('grants') %}
                    {{ get_revoke_sql(relation, needs_revoking) }}
                    {{ get_grant_sql(relation, needs_granting) }}
                {% endcall %}
            {% endif %}
    {% endif %}
{% endmacro %}
