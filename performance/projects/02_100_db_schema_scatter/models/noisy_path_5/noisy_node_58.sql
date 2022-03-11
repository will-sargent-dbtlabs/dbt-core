
    {{
    config(
        enabled=var('add_noise', True),
        database=('bigdb4' if target.type in ('snowflake', 'bigquery') else target.get('database')),
        schema='bigschema2',
        materialized='table'
    )
    }}
    
    select 1 as id
    