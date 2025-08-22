import dlt


rules = {
    "valid_id": "customer_id is not null"
}

@dlt.table(name="bronze_customers")
@dlt.expect_all_or_drop(rules)
def bronze_customers():
    df = spark.readStream.table("cat.src.customers")
    return df
