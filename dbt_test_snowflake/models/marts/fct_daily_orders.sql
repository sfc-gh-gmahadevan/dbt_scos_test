-- Incremental mart: daily order rollup.
--
-- First run  -> builds the whole table.
-- Later runs -> the is_incremental() block adds a WHERE filter so only new days
--               are scanned, and delete+insert replaces any day already present
--               (so a partially-loaded day gets corrected rather than doubled).
--
-- Use `dbt run --full-refresh` to rebuild from scratch.

{{
    config(
        materialized = 'incremental',
        unique_key = 'order_date',
        incremental_strategy = 'delete+insert'
    )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

    {% if is_incremental() %}
    -- {{ this }} refers to the existing table this model already built.
    -- Re-process the latest day too, in case it was incomplete.
    where order_date >= (select coalesce(max(order_date), '1900-01-01') from {{ this }})
    {% endif %}

)

select
    order_date,
    count(*)                                as order_count,
    count(distinct customer_key)            as customer_count,
    sum(order_total)                        as gross_revenue,
    cast(avg(order_total) as number(18, 2))  as avg_order_value,
    sum(case when order_status = 'F' then 1 else 0 end) as fulfilled_orders

from orders
group by order_date
