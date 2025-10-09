{{
    config(
        materialized='incremental',
        unique_key='option_bar_id',
        incremental_strategy='delete+insert'
    )
}}

with options as (select * from {{ ref('int_polygon__options_bars_enriched') }}),
stocks as (select * from {{ ref('int_polygon__stock_bars_enriched') }}),

joined as (
select
  t1.option_bar_id,
  t1.option_symbol,
  t1.underlying_ticker,
  t1.trade_date,
  t1.expiration_date,
  t1.strike_price,
  t1.option_type,
  t1.open_price as option_open,
  t1.high_price as option_high,
  t1.low_price as option_low,
  t1.close_price as option_close,
  t1.volume_weighted_average_price as option_vwap,
  t1.volume as option_volume,
  t1.transactions as option_transactions,
  t1.days_to_expiration,
  t2.close_price as underlying_close_price,
  t2.moving_avg_20d as underlying_moving_avg_20d,
  t2.moving_avg_50d as underlying_moving_avg_50d,
  t2.moving_avg_120d as underlying_moving_avg_120d,
  case
    when (t1.strike_price = t2.close_price) then 'at-the-money'
    when (t1.option_type = 'call' and t1.strike_price < t2.close_price)
      or (t1.option_type = 'put' and t1.strike_price > t2.close_price)
      then 'in-the-money'
    else 'out-of-the-money'
  end as moneyness
  
from options t1
left join stocks t2

on t1.underlying_ticker = t2.ticker
and t1.trade_date = t2.trade_date
)

select * from joined
