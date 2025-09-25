{{
    config(
        materialized='incremental',
        unique_key='stock_bar_id',
        incremental_strategy='delete+insert'
    )
}}

with stg_stock_bars as (
    select * from {{ ref('stg_polygon__stock_bars_daily') }}
),

enriched as (
select
-- Passthrough columns
stock_bar_id,
ticker,
trade_date,
loaded_at,
open_price,
high_price,
low_price,
close_price,
volume,
transactions,

-- Calculated metrics
avg(close_price) over (partition by ticker order by trade_date rows between 19 preceding and current row) as moving_avg_20d,
avg(close_price) over (partition by ticker order by trade_date rows between 49 preceding and current row) as moving_avg_50d
from stg_stock_bars

{% if is_incremental() %}
-- this filter will limit the data scanned to only the new records
where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
)

select * from enriched
