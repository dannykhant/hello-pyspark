# Declarative Pipeline

- Declarative Pipeline
    - Known as Delta Live Tables
    - Modern way to build pipelines with less manual orchestration and more automation
    - Declare what you want, DLT takes care of how it should be done
- Benefits
    - Quality Checks
        - Using Expectations, we can define rules
        - If a record brakes a rule, DLT will drop or sent it to quarantine
        - No need to write extra code to handle data quality
    - Auto Dependency Management
        - No need to worry the order of transformations
        - DLT figures out which table depends on which and runs in the correct order
    - Incremental Processing
        - Change Data Capture is supported
    - Unified Batch & Streaming
        - We can use same code for both and DLT handles it accordingly
- LakeFlow Pipeline Editor
    - Pipeline Assets Browser - for pipeline logic
        - Left panel is call pipeline assets browser
        - File actions such as Moving root folder, should be taken only in it
        - Root Folder - main directory
            - transformations - default source code folder
                - We can change it in the pipeline settings
            - explorations - non-source code folder, used to explore the data
            - utilities - for utility functions
    - Pipeline graph - for pipeline visualization
    - Tables/ Performance - for pipeline results
- Key Components (DLT Objects)
    - Streaming Table
        - For dealing with stream data and incremental loading
        - It fetches only the incremental data when it runs
    - Materialized View
        - Results of a query that is stored
        - It fetches all the data every time it runs
    - Views
        - A query that is stored
        - Batch Views vs Streaming Views
- Streaming Table Creation
    
    ```python
    @dlt.table(
    	name="st_tbl"
    )
    def st_tbl():
    	df = spark.readStream.table("cat.sch.orders")
    	return df
    ```
    
- Materialized Views (For Batch Processing)
    
    ```python
    @dlt.table(
    	name="m_view"
    )
    def m_view():
    	df = spark.read.table("cat.sch.order")
    	return df
    ```
    
- Views
    - Batch View
        
        ```python
        @dlt.view(
        	name="b_view"
        )
        def b_view()
        	df = spark.read.table("cat.sch.order")
        	return df
        ```
        
    - Stream View
        
        ```python
        @dlt.view(
        	name="s_view"
        )
        def s_view()
        	df = spark.readStream.table("cat.sch.order")
        	return df
        ```
        
- Dependency Creation
    
    ```python
    @dlt.table(
    	name="staging"
    )
    def staging():
    	df = spark.readStream.table("cat.sch.orders")
    	return df
    	
    @dlt.view(
    	name="transformation"
    )
    def transformation():
    	df = spark.readStream.table("LIVE.staging")
    	df = df.withColumn("status", lower(col("status")))
    	return df
    	
    @dlt.table(
    	name="agg"
    )
    def agg():
    	df = spark.readStream.table("LIVE.transformation")
    	df = df.groupBy("status").count()
    	return df
    ```
    
- Append Flow
    
    ```python
    rules = {
    	"valid_id": "order_id is not null"
    }
    
    dlt.create_streaming_table(
    	name="sales_staging",
    	expect_all_or_drop=rules
    )
    
    @dlt.append_flow(target="sales_staging")
    def tbl1():
    	df = spark.readStream.table("cat.sch.sales_east")
    	return df
    	
    @dlt.append_flow(target="sales_staging")
    def tbl2():
    	df = spark.readStream.table("cat.sch.sales_west")
    	return df
    ```
    
- Expectations
    - Used for data quality checks
    
    ```python
    rules = {
    	"valid_id": "product_id is not null",
    	"valid_price": "price >= 0"
    }
    
    @dlt.table(
    	name="products_staging"
    )
    @dlt.expect_all_or_drop(rules)
    def products_staging():
    	df = spark.readStream.table("cat.sch.products")
    	return df
    ```
    
- Auto CDC
    
    ```python
    dlt.create_streaming_table(
    	name="sales_enriched"
    )
    
    @dlt.view(
    	name="view_sales_transformed"
    )
    def view_sales_transformed():
    	df = spark.readStream.table("sales_staging")
    	df = df.withColumn("total_amt", col("qty") * col("amount"))
    	return df
    
    dlt.create_auto_cdc_flow(
    	target = "sales_enriched",
      source = "view_sales_transformed",
      keys = ["order_id"],
      sequence_by = "sales_timestamp",
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
    ```
    
- SCD Type-2
    
    ```python
    dlt.create_streaming_table(name="dim_products")
    
    dlt.create_auto_cdc_flow(
    	target = "dim_products",
      source = "view_products_enriched",
      keys = ["product_id"],
      sequence_by = "last_updated",
      ignore_null_updates = False,
      apply_as_deletes = None,
      apply_as_truncates = None,
      column_list = None,
      except_column_list = None,
      stored_as_scd_type = 2,
      track_history_column_list = None,
      track_history_except_column_list = None,
      name = None,
      once = False
    )
    ```
    
- Settings & Alert
    - We can add Email Notifications for failure
    - Pipeline Settings are available in JSON and YAML
    - To add parameter, we can use Configuration
    - We can embed DLT Pipeline within a Job.