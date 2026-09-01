-- Mart: one row per customer with lifetime order metrics.
-- Materialized as a table (set in dbt_project.yml). ref() builds the DAG edge
-- to the staging models, so dbt knows to build those first.

with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customer') }}

),

order_metrics as (

    select
        customer_key,
        count(*)                as order_count,
        sum(order_total)        as lifetime_value,
        min(order_date)         as first_order_date,
        max(order_date)         as most_recent_order_date

    from orders
    group by customer_key

)

select
    c.customer_key,
    c.customer_name,
    c.market_segment,
    c.account_balance,
    -- Customers with no orders in range keep a 0 rather than a NULL.
    coalesce(m.order_count, 0)     as order_count,
    coalesce(m.lifetime_value, 0)  as lifetime_value,
    m.first_order_date,
    m.most_recent_order_date,
    datediff('day', m.first_order_date, m.most_recent_order_date) as active_days

from customers c
left join order_metrics m
    on c.customer_key = m.customer_key
