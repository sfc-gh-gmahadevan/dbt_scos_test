# dbt PYTHON MODEL (Snowpark)
#
# This runs INSIDE Snowflake as a Python stored procedure. dbt wraps this file,
# uploads it, and executes it on the warehouse.
#
# The API is `snowflake.snowpark`, NOT `pyspark`. The DataFrame API is
# deliberately Spark-shaped, so most PySpark code translates near 1:1. The
# PySpark equivalent of each step is noted in comments.
#
# Contract:
#   - Must define exactly one function named `model(dbt, session)`
#   - Must return a DataFrame (Snowpark or pandas); dbt materializes the result
#   - Use dbt.ref(...) / dbt.source(...) instead of writing table names

import snowflake.snowpark.functions as F
from snowflake.snowpark import Window


def model(dbt, session):
    dbt.config(
        materialized="table",
        # Only these packages are available; they come from the Snowflake Anaconda
        # channel, no pip install at runtime.
        packages=["snowflake-snowpark-python"],
    )

    # PySpark: spark.table("dev_marts.fct_customer_orders")
    customers = dbt.ref("fct_customer_orders")

    # Drop customers with no orders in the window. Identical in PySpark.
    active = customers.filter(F.col("ORDER_COUNT") > 0)

    # --- Rank customers into quintiles on two axes -------------------------
    # PySpark: Window.orderBy(F.col("LIFETIME_VALUE").desc())
    value_window = Window.order_by(F.col("LIFETIME_VALUE").desc())
    freq_window = Window.order_by(F.col("ORDER_COUNT").desc())

    scored = active.with_column(
        "VALUE_QUINTILE", F.ntile(F.lit(5)).over(value_window)
    ).with_column(
        "FREQUENCY_QUINTILE", F.ntile(F.lit(5)).over(freq_window)
    )

    # --- Segment via a chained conditional ---------------------------------
    # PySpark: F.when(...).when(...).otherwise(...) — same shape, same names.
    combined = F.col("VALUE_QUINTILE") + F.col("FREQUENCY_QUINTILE")

    segmented = scored.with_column(
        "SEGMENT",
        F.when(combined <= 3, F.lit("Champion"))
        .when(combined <= 5, F.lit("Loyal"))
        .when(combined <= 7, F.lit("Potential"))
        .when(combined <= 9, F.lit("At Risk"))
        .otherwise(F.lit("Dormant")),
    )

    # --- Share of total revenue, as a window aggregate ---------------------
    # PySpark: F.sum(...).over(Window.partitionBy()) — Snowpark needs an
    # explicit empty partition_by to express "over the whole result set".
    whole_table = Window.partition_by(F.lit(1))

    final = segmented.with_column(
        "PCT_OF_TOTAL_REVENUE",
        F.round(
            F.col("LIFETIME_VALUE") / F.sum("LIFETIME_VALUE").over(whole_table) * 100,
            6,
        ),
    ).select(
        "CUSTOMER_KEY",
        "CUSTOMER_NAME",
        "MARKET_SEGMENT",
        "ORDER_COUNT",
        "LIFETIME_VALUE",
        "MOST_RECENT_ORDER_DATE",
        "VALUE_QUINTILE",
        "FREQUENCY_QUINTILE",
        "SEGMENT",
        "PCT_OF_TOTAL_REVENUE",
    )

    return final
