import dlt


rules = {
    "valid_id": "product_id is not null"
}

@dlt.table(name="bronze_products")
@dlt.expect_all_or_drop(rules)
def bronze_products():
    df = spark.readStream.table("cat.src.products")
    return df
