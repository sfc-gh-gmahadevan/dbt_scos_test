-- Staging layer for customers, enriched only with its own lookup keys.

with source as (

    select * from {{ source('tpch', 'customer') }}

),

renamed as (

    select
        c_custkey                           as customer_key,
        c_name                              as customer_name,
        c_nationkey                         as nation_key,
        c_mktsegment                        as market_segment,
        cast(c_acctbal as number(18, 2))     as account_balance

    from source

)

select * from renamed
