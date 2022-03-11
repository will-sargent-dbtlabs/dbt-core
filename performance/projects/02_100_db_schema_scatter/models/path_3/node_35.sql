
    {{
    config(
        enabled=True,
        database=('bigdb2' if target.type in ('snowflake', 'bigquery') else target.get('database')),
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
select * from {{ ref('node_7') }}
union all
select * from {{ ref('node_15') }}
union all
select * from {{ ref('node_19') }}