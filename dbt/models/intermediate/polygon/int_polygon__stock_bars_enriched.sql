{{
    config(
        materialized='incremental',
        unique_key='stock_bar_id',
        incremental_strategy='delete+insert'
    )
}}

with source as (
    select * from {{ ref('stg_polygon__stock_bars_casted') }}
),

enriched as (
select
 t1.*,
 avg(close_price) over (partition by ticker order by trade_date rows between 19 preceding and current row) as moving_avg_20d,
 avg(close_price) over (partition by ticker order by trade_date rows between 49 preceding and current row) as moving_avg_50d,
 (close_price - lag(close_price, 1) over (partition by ticker order by trade_date)) as price_change_1d,
 (high_price - low_price) as daily_price_range
from source t1

{% if is_incremental() %}
-- this filter will limit the data scanned to only the new records
where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
)

select * from enriched
