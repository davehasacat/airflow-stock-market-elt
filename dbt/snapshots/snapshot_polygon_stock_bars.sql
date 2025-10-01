{% snapshot snapshot_polygon_stock_bars %}

{{
    config(
      target_schema='public',
      unique_key="ticker || '_' || trade_date",
      strategy='timestamp',
      updated_at='inserted_at'
    )
}}

select
  ticker,
  trade_date,
  cast(inserted_at as timestamp) as inserted_at,
  open,
  high,
  low,
  close,
  vwap,
  volume,
  transactions
from {{ source('public', 'source_polygon_stock_bars_daily') }}

{% endsnapshot %}
