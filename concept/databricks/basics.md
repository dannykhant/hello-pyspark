# Databricks Basics

- Spark Clusters
    - Multiple nodes used for distributed data processing
    - Distribute the work (processing subset of the whole data) across nodes
    - The nodes work on it parallelly
- Spark Architecture
    - Cluster Manager
        - Responsible for managing resources
    - Driver
        - Responsible for creating spark job plans
    - Worker (Executor)
        - Responsible for doing actual work
    - The workflow (Cluster Mode)
        1. When a job is submitted, it will goes to the cluster manager
        2. The cluster manager launches a driver on one node
        3. Driver creates a plan and breaks the job into stages and tasks
            1. Logical plan → Optimized plan → Physical plan (By Catalyst Optimizer)
            2. Stages (Shuffle boundaries) → Tasks (One data partition)
        4. Driver sends the resource requirements to the cluster manager
        5. The cluster manager launches executors on worker nodes as per the requirement
        6. The executors process the data and return the result to the driver
- Databricks
    - A platform that providing service by managing the Spark clusters
    - Features
        - Workspace
            - A place to work with notebooks
        - Catalog
            - Metadata to view schema, table
        - Jobs & Pipelines
            - For orchestrations and data pipelines
        - Compute
            - To create the Spark clusters
- Magic CMDs
    - %python
    - %sql
    - %r
    - %md
    - %fs
- DBFS
    - Stands for Databricks File System
    - Abstraction layer on top of data lake
    - Supports to use File Systems paths instead of data lake URLs
- Accessing Data Lake
    - Azure Databricks → Service-Principle → DataLake Gen2
- Databricks Utilities
    - dbutils.fs.ls()
        - List the files in the directory
            
            ```sql
            dbuitls.fs.ls("abfss://<container>@<storage-account>.dfs.core.windows.net/")
            ```
            
    - dbutils.widgets
        - text()
            - To create user prompt for parameterized notebooks
                
                ```sql
                dbutils.widgets.text("<var-name>", "<default-value>")
                ```
                
        - get()
            - To access the variable
                
                ```sql
                dbutils.widgets.get("<var-name>")
                ```
                
    - dbutils.secret
        - Can be used to connect to the secret manager (eg., Azure KeyVault)
        - To list all the secret keys
            
            ```sql
            dbutils.secrets.list(scope="keyscope")
            ```
            
        - To get a key
            
            ```sql
            dbutils.secrets.get(scope="keyscope", key="app-secret")
            ```
            
- %run
    - To run any databricks notebook in a notebook
        
        ```sql
        %run "/Workspace_one/notebook01"
        ```
        
- Metastore
    - Metadata layer to store all the definitions - database definition, schema definition, table definition
- Managed Delta Table
    - Databricks own both Metastore and Data Storage
    - Removing a table will delete data from both Metastore and Data Lake
- External Delta Table
    - Databricks own only the Metastore
    - Removing a table will only delete data from Metastore
- Auto Loader
    - Incremental data will be loaded into destination using Streaming Dataframe
    - Uses a checkpoint to remember what’s processed
    - ReadStream
        
        ```sql
        df = spark.readStream.format("cloudFiles")\
        							.option("cloudFiles.format", "parquet")\
        							.option("cloudFiles.schemaLocation", "abfss://.../checkpoint")\
        							.load("abfss://.../src")
        ```
        
    - WriteStream
        
        ```sql
        df.writeStream.format("delta")\
        					.option("checkpointLocation", "abfss://.../checkpoint")\
        					.trigger(processingTime="5 seconds")\
        					.start("abfss://.../dest")
        ```
        
- Workflows
    - Used to orchestrate the Databricks notebooks