{{
    config(
        materialized='incremental',
        unique_key='stock_bar_id',
        incremental_strategy='delete+insert'
    )
}}

with source as (
    select * from {{ ref('stg_polygon__stock_bars_casted') }}
      -- this filter will limit the data scanned to only the new records
      {% if is_incremental() %}
        where loaded_at > (select max(loaded_at) from {{ this }})
      {% endif %}
),

add_calculated_metrics as (
select
  loaded_at,
  stock_bar_id,
  ticker,
  trade_date,
  open_price,
  high_price,
  low_price,
  close_price,
  volume_weighted_average_price,
  avg(close_price) over (partition by ticker order by trade_date rows between 19 preceding and current row) as moving_avg_20d,
  avg(close_price) over (partition by ticker order by trade_date rows between 49 preceding and current row) as moving_avg_50d,
  (close_price - lag(close_price, 1) over (partition by ticker order by trade_date)) as price_change_1d,
  (high_price - low_price) as daily_price_range,
  volume,
  transactions
from source
),

enriched as (
  loaded_at,
  stock_bar_id,
  ticker,
  trade_date,
  open_price,
  high_price,
  low_price,
  close_price,
  volume_weighted_average_price,
  case
    when moving_avg_20d >= 1.0000 then round(moving_avg_20d, 2)
    else round(moving_avg_20d, 4)
  end as moving_avg_20d,
  case
    when moving_avg_50d >= 1.0000 then round(moving_avg_50d, 2)
    else round(moving_avg_50d, 4)
  end as moving_avg_50d,
  price_change_1d,
  daily_price_range,
  volume,
  transactions
)

select * from enriched
