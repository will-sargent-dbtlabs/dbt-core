{% macro get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
  {{ adapter.dispatch('get_test_sql', 'dbt')(main_sql, fail_calc, warn_if, error_if, limit) }}
{%- endmacro %}

{% macro default__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
    {% set model_name = model.file_key_name.split('.')[1] %}
    {% set warn_if, warn_pct_division = get_pct_division(warn_if, model_name) %}
    {% set error_if, error_pct_division = get_pct_division(error_if, model_name) %}
    
    select
      {{ fail_calc }} as failures,
      {{ fail_calc }} {{ warn_pct_division }} {{ warn_if }} as should_warn,
      {{ fail_calc }} {{ error_pct_division }} {{ error_if }} as should_error
    from (
      {{ main_sql }}
      {{ "limit " ~ limit if limit != none }}
    ) dbt_internal_test
{%- endmacro %}


{% macro get_pct_division(threshold, model_name) %}
  {% if threshold[-1] == '%' %}
    {% set threshold = threshold[:-1] %}
    {% set pct_division = '/ (select count(*) from ' ~ ref(model_name) ~ ') * 100' %}
  {% else %}
    {% set pct_division = '' %}
  {% endif %}
    {% do return((threshold, pct_division)) %}
{% endmacro %}
