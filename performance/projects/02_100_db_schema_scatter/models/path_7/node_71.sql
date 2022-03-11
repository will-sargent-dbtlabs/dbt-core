
    {{
    config(
        enabled=True,
        database=('bigdb0' if target.type in ('snowflake', 'bigquery') else target.get('database')),
        schema='bigschema0',
        materialized='table'
    )
    }}
    
    select 1 as id
    
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_2') }}
union all
select * from {{ ref('node_13') }}
union all
select * from {{ ref('node_38') }}
union all
select * from {{ ref('node_59') }}
union all
select * from {{ ref('node_63') }}