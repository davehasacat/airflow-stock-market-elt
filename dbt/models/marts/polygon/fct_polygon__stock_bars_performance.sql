-- dbt/models/marts/mart_polygon__daily_stock_performance.sql

{{
    config(
        materialized='incremental',
        unique_key='stock_bar_id',
        incremental_strategy='delete+insert'
    )
}}

with enriched_prices as (
    select * from {{ ref('int_polygon__stock_prices_enriched') }}
),

final as (
select
-- Identifiers
stock_bar_id,
ticker,
trade_date,
-- Performance Metrics
close_price,
moving_avg_20d,
moving_avg_50d,
(close_price - lag(close_price, 1) over (partition by ticker order by trade_date)) as price_change_1d,

-- Volume and Volatility
volume,
(high_price - low_price) as daily_price_range
from enriched_prices
{% if is_incremental() %}
-- Assuming 'loaded_at' is available from the intermediate model.
-- If not, you could also filter on 'trade_date'.
where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
)

select * from final
