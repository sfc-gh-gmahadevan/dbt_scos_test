"""Run REAL PySpark code against Snowflake using Snowpark Connect (SCOS).

This is the honest answer to "run PySpark with dbt". dbt Python models use the
Snowpark API, not PySpark. If you need the literal `pyspark` API surface, you
run it through Snowpark Connect, which speaks the Spark Connect protocol and
pushes the plan down to a Snowflake warehouse. No Spark cluster exists.

The important part: this reads the tables dbt built, so PySpark sits downstream
of the dbt DAG rather than replacing it.
"""

from snowflake import snowpark_connect

DATABASE = "DBT_LEARN"
MARTS_SCHEMA = "DEV_MARTS"


def get_spark():
    """Start the embedded Snowpark Connect server and return a SparkSession.

    SCOS creates its own Snowflake session from the connection named in
    SNOWFLAKE_CONNECTION_NAME (see run.sh). Do NOT pre-create a Snowpark
    session and hand it over — this SCOS version expects to own it.
    """
    snowpark_connect.start_session()
    return snowpark_connect.get_session()


def main():
    spark = get_spark()

    # ---- Ordinary PySpark from here down --------------------------------
    # Import from pyspark.sql.connect.* — with a Spark Connect session the
    # classic pyspark.sql.functions module tries to reach a local JVM and
    # fails with a bare AssertionError.
    from pyspark.sql.connect import functions as F
    from pyspark.sql.connect.window import Window

    # Reads the table the dbt SQL model built.
    orders = spark.table(f"{DATABASE}.{MARTS_SCHEMA}.FCT_DAILY_ORDERS")
    print(f"daily rows: {orders.count()}")

    monthly = (
        orders.withColumn("MONTH", F.date_trunc("month", F.col("ORDER_DATE")))
        .groupBy("MONTH")
        .agg(
            F.sum("GROSS_REVENUE").alias("REVENUE"),
            F.sum("ORDER_COUNT").alias("ORDERS"),
        )
    )

    # Month-over-month growth via a lag window — classic PySpark idiom.
    w = Window.orderBy("MONTH")
    growth = (
        monthly.withColumn("PREV_REVENUE", F.lag("REVENUE").over(w))
        .withColumn(
            "MOM_PCT",
            F.round(
                (F.col("REVENUE") - F.col("PREV_REVENUE"))
                / F.col("PREV_REVENUE")
                * 100,
                2,
            ),
        )
        .orderBy(F.col("MONTH").desc())
    )

    growth.show(6, truncate=False)

    # Write the result back so dbt (or anything else) can consume it.
    target = f"{DATABASE}.{MARTS_SCHEMA}.SPARK_MONTHLY_GROWTH"
    growth.write.mode("overwrite").saveAsTable(target)
    print(f"wrote {target}")


if __name__ == "__main__":
    main()
