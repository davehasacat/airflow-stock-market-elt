{{
    config(
        materialized='incremental',
        unique_key='stock_bar_id',
        incremental_strategy='delete+insert'
    )
}}

with source as (
    select * from {{ ref('int_polygon__stock_bars_enriched') }}
)

select * from source
