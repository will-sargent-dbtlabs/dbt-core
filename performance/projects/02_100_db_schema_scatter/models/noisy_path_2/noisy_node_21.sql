
    {{
    config(
        enabled=var('add_noise', True),
        database=('bigdb2' if target.type in ('snowflake', 'bigquery') else target.get('database')),
        schema='bigschema3',
        materialized='view'
    )
    }}
    
    select 1 as id
    