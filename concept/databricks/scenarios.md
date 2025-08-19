# Databricks Scenarios

- Volumes
    - When we need to add data **governance** over non-tabular datasets
    - 2 types - Managed volume and External volume
    - External Volume creation
        
        ```sql
        -- step 1: Add a external location in UI
        -- step 2: Create volume
        create external volume cat.sch.myvol
        location 'abfss://path/folder';
        ```
        
    - Query CSV
        
        ```sql
        select * from csv.'/Volume/path/file.csv';
        ```
        
- Job Workflows
    - When we need read multiple files one after another
    - Parameter creation (param notebook)
        
        ```python
        # 1. Create text widget
        dbutils.widgets.text("name", "")
        
        # 2. Get the text
        name = dbutils.widgets.get("name")
        
        # 3. Read file
        df = spark.read.format("csv")\
        				.option("header", "true")\
        				.load(f"abfss://path/folder/{name}.csv")
        ```
        
    - Pass the parameter (input notebook)
        
        ```python
        file_arr = [
        	{"name": "first"},
        	{"name": "second"}
        ]
        
        dbutils.jobs.taskValues.set("param", file_arr)
        ```
        
    - In jobs, below will be created
        - input notebook
        - param notebook - {{input.name}}
        - Loop over (param notebook) - {{tasks.input.values.param}}
- Lakehouse Federation
    - When we need to directly query against multiple remote sources without migrating data
- Autoloader
    - It processes the data incrementally with idempotency ability
    - Src → Autoloader(SparkStream)→ Dst
    - Autoloader store the metadata in Rocks_DB once it reads the files in Src
    - By checking the metadata in Rocks_DB, Autoloader add files incrementally
    - Schema Location can be used for the Src schema changes
        - Add New Columns - Add all the new columns
        - Rescue - Add all the new columns in the dictionary
        - Fail - Fail the pipeline
    
    ```python
    df = spark.readStream.format("cloudFiles")\
    							.option("cloudFiles.format", "parquet")\
    							.option("cloudFiles.schemaLocation", "abfss://.../checkpoint")\
    							.load("abfss://.../src")
    							
    df.writeStream.format("parquet")\
    					.outputMode("append")\
    					.option("checkpointLocation", "abfss://.../checkpoint")\
    					.trigger(processingTime="5 seconds")\
    					.start("abfss://.../dest")							
    ```
    
- Unity Catalog Access Control
    - This can be used to control access of the Databricks users
    - There are - catalog-level permission, schema-level and table-level permission
- Compute Policy
    - Policy about cluster compute resource that users can use
    - Personal Compute policy can be disabled in the account console
- Workspace Files
    - All the files from the workspace can be accessed in DB notebooks using Python
    
    ```python
    import pandas as pd
    
    df = pd.read_json("/Workspace/cur/file.json")
    sp_df = spark.createDataframe(df)
    sp_df.display()
    ```
    
- Delta Sharing
    - Data can be shared easily between workspaces attached to the same Unity Metastore
    - Delta Sharing can be used to share data between different Metastores
- Persistent UDFs in Unity Catalog
    
    ```sql
    create or replace function cat.sch.func(param1 string)
    returns string
    language sql
    return concat("Mr.", upper(param1));
    ```
    
    ```sql
    create or replace function cat.sch.func(param1 string)
    returns string
    language python
    as
    $$
    	return "Mr." + param1.upper()
    $$
    ```
    
- Monitoring & Alerts
    - Alerts can be created with SQL query
    - Steps:
        - Write a SQL query and save it
        - Go to Alerts and select the query file
- Delta Live Table
    - For building and managing reliable, maintainable, and testable data pipelines
    - Expectation is used for the data quality control
    - Steps:
        - Create a notebook with dlt codes
        - Create the dlt pipeline with the notebook