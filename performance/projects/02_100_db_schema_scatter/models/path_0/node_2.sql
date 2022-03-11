
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