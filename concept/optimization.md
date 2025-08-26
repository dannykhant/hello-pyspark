# Optimization

- Scanning Optimization
    - Partition Pruning
        - 128 MB is default partition size
        - Get number of partitions
            
            ```python
            df.rdd.getNumPartitions()
            ```
            
        - Set default partition size
            
            ```python
            spark.conf.set("spark.sql.files.maxPartitionBytes", 131072)
            ```
            
        - Repartitioning
            - To recreate partitions for the parallelism
                
                ```python
                df = df.repartition(10)
                ```
                
        - Get partition info
            
            ```python
            df.withColumn("parition_id", spark_partition_id()).display()
            ```
            
        - Scanning Optimization for Partition Pruning
            
            ```python
            df.write.format("parquet")\
            			.mode("append")\
            			.partitionBy("column_name")\
            			.option("path", "/path/folder")\
            			.save()
            ```
            
        - {Date} is the best candidate for the partition pruning
- Joins Optimization
    - Join creates 200 partitions and does the joining of tables in each partition
    - Broadcast Join
        - One of the tables must be small enough
        - The small table will be broadcast to all executors which eliminates the shuffling
        - SQL Hints
            
            ```sql
            select
            	/*+ broadcast(t2) */ *
            from t1
            join t2
            on t1.key = t2.key
            ```
            
- Caching & Persistence
    - Spark Executor Memory
        - Storage memory - used for caching
        - Executor Memory - used for transformation
    - pyspark supports -
        - Disk and memory
        - Disk only
        - Memory only
- Dynamic Resource Allocation
    - After the certain idle time, the resource will be released
    - minExecutors - The resource remains all the time
    - maxExecutors - The maximum resource can go up
    - initialExecutors - To kick-start the application
- AQE
    - Dynamically coalesce the partitions while shuffling
        - When apply wide transformations,
            - It discards the usage of 200 partitions
            - It coalesces the partitions remaining into less ones depending on size
    - Optimizing the Join strategy during run-time
        - Apply the suitable joins when required
            - Re-examine the logical plan and update the join
    - Optimizing the skewness
        - Break the skew partition into pieces
    
    ```python
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    ```
    
- Dynamic Partition Pruning
    - We are doing a join of two df
    - df1 is fact data partitioned by category
    - df2 is category data without any partitioning
    - We apply a filter on df2 and do the join operation with df1
    - The data scanning only reads the relevant partition of df1 instead of reading all the partitions
    - Because it uses the same filter on the df1 as well by waiting the result of df2
    - It is called as dynamic partition pruning
    
    ```python
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
    ```
    
- Broadcast Variable
    - Read-only variable that is cached and shared across all executors instead of being sent with every task
    
    ```python
    b_var = spark.sparkContext.broadcast(var)
    ```
    
- Salting
    - Used to solve OOM of data skew (Can spill only whole partition to disk)
    
    ```python
    df = df.withColumn("salt_col", floor(rand()*3))
    ```
    
- Delta Lake Optimization
    - Optimize
        - This will coalesce multiple small partitions into less bigger ones
    - Z-Ordering
        - This will sort all the partitions by key to let Spank can go to the relevant partition by reading column statistics