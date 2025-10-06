{{
    config(
        materialized='incremental',
        unique_key='option_bar_id',
        incremental_strategy='delete+insert'
    )
}}

with options as (select * from {{ ref('int_tradier__options_bars_enriched') }}),
stocks as (select * from {{ ref('int_tradier__stock_bars_enriched') }}),

joined as (
select
  t1.option_bar_id,
  t1.option_symbol,
  t1.underlying_ticker,
  t1.trade_date,
  t1.expiration_date,
  t1.strike_price,
  t1.option_type,
  t1.open_price,
  t1.high_price,
  t1.low_price,
  t1.close_price,
  t1.volume_weighted_average_price,
  t1.volume,
  t1.days_to_expiration,
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
on t1.underlying_ticker = t2.ticker and t1.trade_date = t2.trade_date
)

select * from joined
