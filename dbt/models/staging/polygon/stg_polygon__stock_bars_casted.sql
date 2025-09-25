-- dbt/models/staging/stg_polygon__stock_bars_daily.sql

with source as (
    select * from {{ source('public', 'source_polygon_stock_bars_daily') }}
),

renamed_and_casted as (
select
-- Primary Key
ticker || '_' || trade_date as stock_bar_id,

-- Foreign Keys
ticker,

-- Timestamps
trade_date,
inserted_at as loaded_at,

-- Price Information
open as open_price,
high as high_price,
low as low_price,
close as close_price,
vwap as volume_weighted_average_price,

-- Trade Information
volume,
transactions
from source
)

select * from renamed_and_casted
