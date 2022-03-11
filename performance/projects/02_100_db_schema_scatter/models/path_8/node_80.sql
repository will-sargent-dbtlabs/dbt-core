
    {{
    config(
        enabled=True,
        database=('bigdb0' if target.type in ('snowflake', 'bigquery') else target.get('database')),
        schema='bigschema0',
        materialized='view'
    )
    }}
    
    select 1 as id
    
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_3') }}
union all
select * from {{ ref('node_6') }}
union all
select * from {{ ref('node_8') }}
union all
select * from {{ ref('node_10') }}
union all
select * from {{ ref('node_21') }}