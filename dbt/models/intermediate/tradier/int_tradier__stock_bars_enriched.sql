{{
    config(
        materialized='incremental',
        unique_key='stock_bar_id',
        incremental_strategy='delete+insert'
    )
}}

with source as (
    select * from {{ ref('stg_tradier__stock_bars_casted') }}
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
  avg(close_price) over (partition by ticker order by trade_date rows between 19 preceding and current row) as moving_avg_20d,
  avg(close_price) over (partition by ticker order by trade_date rows between 49 preceding and current row) as moving_avg_50d,
  avg(close_price) over (partition by ticker order by trade_date rows between 119 preceding and current row) as moving_avg_120d,
  (close_price - lag(close_price, 1) over (partition by ticker order by trade_date)) as price_change_1d,
  (high_price - low_price) as daily_price_range,
  volume
from source
)

select * from add_calculated_metrics
