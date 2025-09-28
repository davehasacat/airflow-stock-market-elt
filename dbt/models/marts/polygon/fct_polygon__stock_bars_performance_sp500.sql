{{
    config(
        materialized='incremental',
        unique_key='stock_bar_id',
        incremental_strategy='delete+insert'
    )
}}

with source as ( select * from {{ ref('int_polygon__stock_bars_enriched') }} ),
sp500_tickers as ( select ticker from {{ ref('sp500_tickers') }} ),

filtered_source as (
select
t1.*
from source t1
inner join sp500_tickers t2
on t1.ticker = t2.ticker
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
  moving_avg_120d,
  price_change_1d,
  daily_price_range,
  volume,
  transactions
from filtered_source
