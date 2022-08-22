import pytest


models__dependent_sql = """

-- multiple ephemeral refs should share a cte
select * from {{ref('base')}} where gender = 'Male'
union all
select * from {{ref('base')}} where gender = 'Female'

"""

models__double_dependent_sql = """

-- base_copy just pulls from base. Make sure the listed
-- graph of CTEs all share the same dbt_cte__base cte
select * from {{ref('base')}} where gender = 'Male'
union all
select * from {{ref('base_copy')}} where gender = 'Female'

"""

models__super_dependent_sql = """
select * from {{ref('female_only')}}
union all
select * from {{ref('double_dependent')}} where gender = 'Male'

"""

models__base__female_only_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ ref('base_copy') }} where gender = 'Female'

"""

models__base__base_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ this.schema }}.seed

"""

models__base__base_copy_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ ref('base') }}

"""

ephemeral_errors__dependent_sql = """
-- base copy is an error
select * from {{ref('base_copy')}} where gender = 'Male'

"""

ephemeral_errors__base__base_sql = """
{{ config(materialized='ephemeral') }}

select * from {{ this.schema }}.seed

"""

ephemeral_errors__base__base_copy_sql = """
{{ config(materialized='ephemeral') }}

{{ adapter.invalid_method() }}

select * from {{ ref('base') }}

"""

models_n__ephemeral_level_two_sql = """
{{
  config(
    materialized = "ephemeral",
  )
}}
select * from {{ ref('source_table') }}

"""

models_n__root_view_sql = """
select * from {{ref("ephemeral")}}

"""

models_n__ephemeral_sql = """
{{
  config(
    materialized = "ephemeral",
  )
}}
select * from {{ref("ephemeral_level_two")}}

"""

models_n__source_table_sql = """
{{ config(materialized='table') }}

with source_data as (

    select 1 as id
    union all
    select null as id

)

select *
from source_data

"""


@pytest.fixture(scope="class")
def models():
    return {
        "dependent.sql": models__dependent_sql,
        "double_dependent.sql": models__double_dependent_sql,
        "super_dependent.sql": models__super_dependent_sql,
        "base": {
            "female_only.sql": models__base__female_only_sql,
            "base.sql": models__base__base_sql,
            "base_copy.sql": models__base__base_copy_sql,
        },
    }


@pytest.fixture(scope="class")
def ephemeral_errors():
    return {
        "dependent.sql": ephemeral_errors__dependent_sql,
        "base": {
            "base.sql": ephemeral_errors__base__base_sql,
            "base_copy.sql": ephemeral_errors__base__base_copy_sql,
        },
    }


@pytest.fixture(scope="class")
def models_n():
    return {
        "ephemeral_level_two.sql": models_n__ephemeral_level_two_sql,
        "root_view.sql": models_n__root_view_sql,
        "ephemeral.sql": models_n__ephemeral_sql,
        "source_table.sql": models_n__source_table_sql,
    }
