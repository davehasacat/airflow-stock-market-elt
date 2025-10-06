{{
    config(
        materialized='table',
        unique_key='option_bar_id'
    )
}}

with source as (
    select * from {{ ref('snapshot_tradier_options_bars') }} where dbt_valid_to is null
),

renamed_and_casted as (
select
  option_symbol || '_' || trade_date as option_bar_id,
  option_symbol,
  underlying_ticker,
  trade_date,
  expiration_date,
  strike_price,
  option_type,
  open as open_price,
  high as high_price,
  low as low_price,
  close as close_price,
  volume,
  vwap as volume_weighted_average_price,
  inserted_at as loaded_at
from source
)

select * from renamed_and_casted
