-- Staging layer: rename, cast, and lightly clean. No business logic, no joins.
-- Materialized as a view (set in dbt_project.yml) so it is always fresh and free to store.

with source as (

    select * from {{ source('tpch', 'orders') }}

),

renamed as (

    select
        o_orderkey                          as order_key,
        o_custkey                           as customer_key,
        o_orderstatus                       as order_status,
        cast(o_totalprice as number(18, 2))  as order_total,
        cast(o_orderdate as date)            as order_date,
        o_orderpriority                     as order_priority,
        o_clerk                             as clerk,
        o_shippriority                      as ship_priority

    from source
    -- var() reads from dbt_project.yml and can be overridden on the CLI.
    where o_orderdate >= '{{ var("start_date") }}'

)

select * from renamed
