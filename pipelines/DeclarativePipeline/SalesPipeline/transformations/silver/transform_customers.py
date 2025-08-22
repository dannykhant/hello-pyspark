import dlt
from pyspark.sql.functions import col, lit, concat


@dlt.view(name="view_customers_transformed")
def view_customers_transformed():
    df = spark.readStream.table("bronze_customers")
    df = df.withColumn("region", concat(col("region"), lit(" region")))
    return df

dlt.create_streaming_table(
    name="silver_customers", 
    comment="Silver table for customers"
)

dlt.create_auto_cdc_flow(
    target = "silver_customers",
    source = "view_customers_transformed",
    keys = ["customer_id"],
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
