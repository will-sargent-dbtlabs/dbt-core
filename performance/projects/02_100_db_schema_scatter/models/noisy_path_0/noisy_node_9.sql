
    {{
    config(
        enabled=var('add_noise', True),
        database=('bigdb3' if target.type in ('snowflake', 'bigquery') else target.get('database')),
        schema='bigschema4',
        materialized='table'
    )
    }}
    
    select 1 as id
    