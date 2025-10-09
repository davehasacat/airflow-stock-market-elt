{% snapshot snapshot_polygon_options_bars %}

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
  transactions,
  cast(inserted_at as timestamp) as inserted_at
from {{ source('public', 'source_polygon_options_bars_daily') }}

{% endsnapshot %}
