
    {{
    config(
        enabled=var('add_noise', True),
        database=('bigdb0' if target.type in ('snowflake', 'bigquery') else target.get('database')),
        schema='bigschema2',
        materialized='view'
    )
    }}
    
    select 1 as id
    