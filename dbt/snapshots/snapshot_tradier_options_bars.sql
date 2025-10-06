{% snapshot snapshot_tradier_options_bars %}

{{
    config(
      target_schema='public',
      unique_key="option_symbol || '_' || trade_date",
      strategy='timestamp',
      updated_at='inserted_at'
    )
}}

select
  option_symbol,
  trade_date,
  underlying_ticker,
  expiration_date,
  strike_price,
  option_type,
  open,
  high,
  low,
  close,
  volume,
  vwap,
  cast(inserted_at as timestamp) as inserted_at
from {{ source('public', 'source_tradier_options_bars_daily') }}

{% endsnapshot %}
