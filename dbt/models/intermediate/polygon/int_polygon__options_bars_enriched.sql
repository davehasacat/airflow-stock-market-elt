{{
    config(
        materialized='incremental',
        unique_key='option_bar_id',
        incremental_strategy='delete+insert'
    )
}}

with source as (
    select * from {{ ref('stg_polygon__options_bars_casted') }}
    -- This filter limits the data scanned to only new records on incremental runs
    {% if is_incremental() %}
        where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

add_calculated_metrics as (
select
  option_bar_id,
  option_symbol,
  underlying_ticker,
  trade_date,
  expiration_date,
  (expiration_date::date - trade_date::date) as days_to_expiration,
  strike_price,
  option_type,
  open_price,
  high_price,
  low_price,
  close_price,
  volume,
  volume_weighted_average_price,
  transactions,
  loaded_at
from source
)

select * from add_calculated_metrics
