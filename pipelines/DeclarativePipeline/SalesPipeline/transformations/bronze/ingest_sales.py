import dlt


rules = {
    "valid_id": "sales_id is not null",
    "valid_price": "price_per_unit > 0",
    "valid_quanity": "quantity > 0"
}

dlt.create_streaming_table(
    name="bronze_sales",
    expect_all_or_drop=rules
)

@dlt.append_flow(target="bronze_sales")
def ingest_sales_east():
    df = spark.readStream.table("cat.src.sales_east")
    return df

@dlt.append_flow(target="bronze_sales")
def ingest_sales_west():
    df = spark.readStream.table("cat.src.sales_west")
    return df
