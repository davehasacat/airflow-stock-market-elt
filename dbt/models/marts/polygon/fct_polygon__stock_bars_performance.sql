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

select
  stock_bar_id,
  ticker,
  trade_date,
  open_price,
  high_price,
  low_price,
  close_price,
  volume_weighted_average_price,
  moving_avg_20d,
  moving_avg_50d,
  price_change_1d,
  daily_price_range,
  volume,
  transactions
from source
