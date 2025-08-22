import dlt
from pyspark.sql.functions import sum, col


@dlt.table(name="agg_sales")
def agg_sales():
    fact_sales = spark.read.table("fact_sales")
    dim_cust = spark.read.table("dim_customers")
    dim_prod = spark.read.table("dim_products")

    df_join = (fact_sales.join(dim_cust, 
                               (fact_sales.customer_id == dim_cust.customer_id) & dim_cust.__END_AT.isNull())
                .join(dim_prod, 
                      (fact_sales.product_id == dim_prod.product_id) & dim_prod.__END_AT.isNull()))
    
    df_prune = df_join.select("region", "category", "total_amt")

    df_agg = df_prune.groupBy("region", "category").agg(sum(col("total_amt")).alias("total_sales"))

    return df_agg
