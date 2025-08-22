import dlt
from pyspark.sql.functions import col, lit, round


@dlt.view(name="view_products_transformed")
def view_products_transformed():
    df = spark.readStream.table("bronze_products")
    df = df.withColumn("price", round(col("price"), 1))
    return df

dlt.create_streaming_table(
    name="silver_products", 
    comment="Silver table for products"
)

dlt.create_auto_cdc_flow(
    target = "silver_products",
    source = "view_products_transformed",
    keys = ["product_id"],
    sequence_by = "updated_at",
    ignore_null_updates = False,
    apply_as_deletes = None,
    apply_as_truncates = None,
    column_list = None,
    except_column_list = None,
    stored_as_scd_type = 1,
    track_history_column_list = None,
    track_history_except_column_list = None,
    name = None,
    once = False
)
