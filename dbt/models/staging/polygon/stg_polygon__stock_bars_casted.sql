{{
    config(
        materialized='table',
        unique_key='stock_bar_id'
    )
}}

with source as (
    select * from {{ source('public', 'source_polygon_stock_bars_daily') }}
),

renamed_and_casted as (
select
  ticker || '_' || trade_date as stock_bar_id,    -- use as primary key
  ticker,
  trade_date,
  inserted_at as loaded_at,
  open as open_price,
  high as high_price,
  low as low_price,
  close as close_price,
  vwap as volume_weighted_average_price,
  volume,
  transactions
from source
)

select * from renamed_and_casted
