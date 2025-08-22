import dlt
from pyspark.sql.functions import col


@dlt.view(name="view_sales_transformed")
def view_sales_tranformed():
    df = spark.readStream.table("bronze_sales")
    df = df.withColumn("total_amt", col("price_per_unit") * col("quantity"))
    return df

dlt.create_streaming_table(name="silver_sales")

dlt.create_auto_cdc_flow(
    target = "silver_sales",
    source = "view_sales_transformed",
    keys = ["sales_id"],
    sequence_by = "created_at",
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
