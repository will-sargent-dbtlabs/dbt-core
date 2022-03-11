
    {{
    config(
        enabled=True,
        database=('bigdb2' if target.type in ('snowflake', 'bigquery') else target.get('database')),
        schema='bigschema1',
        materialized='table'
    )
    }}
    
    select 1 as id
    
union all
select * from {{ ref('node_0') }}
union all
select * from {{ ref('node_11') }}