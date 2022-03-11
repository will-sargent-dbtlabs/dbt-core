
    {{
    config(
        enabled=True,
        database=('bigdb4' if target.type in ('snowflake', 'bigquery') else target.get('database')),
        schema='bigschema3',
        materialized='view'
    )
    }}
    
    select 1 as id
    
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_24') }}
union all
select * from {{ ref('node_30') }}