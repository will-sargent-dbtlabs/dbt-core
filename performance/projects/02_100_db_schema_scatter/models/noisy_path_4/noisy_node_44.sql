
    {{
    config(
        enabled=var('add_noise', True),
        database=('bigdb4' if target.type in ('snowflake', 'bigquery') else target.get('database')),
        schema='bigschema5',
        materialized='view'
    )
    }}
    
    select 1 as id
    