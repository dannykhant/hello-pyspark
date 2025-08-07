# PySpark

- Load all required functions and types
    
    ```python
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    ```
    
- Data Reading
    - To load a file
        
        ```python
        # CSV
        df = spark.read.format("csv").option("inferSchema", True) \
        						.option("header", True) \
        						.load("/filepath/files/file_name.csv")
        # Json
        df_json = spark.read.format(”json”).option("inferSchema", True) \
        						.option("header", True) \
        						.option("multiLine", False) \
        						.load("/filepath/files/file_name.json")
        ```
        
    - To list all files in a directory
        
        ```python
        dbutils.fs.ls(”/filepath/files”)
        ```
        
    - To view Data
        
        ```python
        df.show()
        ```
        
        ```python
        # better output
        df.display()
        ```
        
- Schema DDL and StructType()
    - Schema Definition
        - To define DDL
            
            ```python
            ddl = """
            				field_1 String,
            				field_2 Double
            			"""
            df = spark.read.format("csv").schema(ddl) \
            						.option("header", True) \
            						.load("/filepath/files/file_name.csv")
            ```
            
        - To view the schema
            
            ```python
            df.printSchema()
            ```
            
        - To define StructType() Schema
            
            ```python
            struct_ddl = StructType([
            			StructField("field_1", StringType, True),
            			StructField("field_2", StringType, True)
            ])
            df = spark.read.format("csv").schema(struct_ddl) \
            						.option("header", True) \
            						.load("/filepath/files/file_name.csv")
            ```
            
- Basics Transformation
    - Select
        
        ```python
        # option 1
        df.select("field_1", "field_2", "field_3").display()
        # option 2
        df.select(col("field_1"), col("field_2"), col("field_3")).display()
        ```
        
    - Alias
        
        ```python
        df.select(col("field_1").alias("new_field_name")).display()
        ```
        
    - Filter/ Where
        
        ```python
        #1
        df.filter(col("field_1") == "Regular").display()
        #2 - & for AND
        df.filter((col("type") == "Soft Drinks") & (col("weight") > 5)).display()
        #3 - isNull for null check and isin to check multiple values
        df.filter((col("size").isNull()) & (col("tier").isin("1", "2")))
        ```
        
    - withColumnRenamed - DF-level column renaming
        
        ```python
        df.withColumnRenamed("field_1", "new_field_name").display()
        ```
        
    - withColumn - To create new column
        
        ```python
        #1 - for constant value, use lit()
        df.withColumn("flag", lit("new"))
        #2
        df.withColumn("area", col("width") * col("height"))
        #3 - update values of a existing column with regexp_replace()
        df.withColumn("existing_col_name", regexp_replace(col("existing_col_name"), "Regular", "Reg")) \
        	.withColumn("existing_col_name", regexp_replace(col("existing_col_name"), "Low Fat", "LF"))
        ```
        
    - Type Casting
        
        ```python
        df.withColumn("field_1", col("field_1").cast(StringType()))
        ```
        
    - Sort/ orderBy
        
        ```python
        #1 descending
        df.sort(col("field_1").desc())
        #2 ascending
        df.sort(col("field_1").asc())
        #3
        df.sort(["field_1", "field_2"], ascending=[0, 0])
        #4
        df.sort(["field_1", "field_2"], ascending=[0, 1])
        ```
        
    - Limit
        
        ```python
        df.limit(10).display()
        ```
        
- Advanced Transformation
    - Drop
        
        ```python
        #1
        df.drop("field_1").display()
        #2 - multiple cols
        df.drop("field_1", "field_2").display()
        ```
        
    - Drop_Duplicates
        
        ```python
        #1
        df.dropDuplicates().display() # df.drop_duplicates()
        #2 - specific column
        df.dropDuplicates(subset=["field_1"]).display()
        #3
        df.distinct().display()
        ```
        
    - Union & UnionByName
        
        ```python
        # union
        df1.union(df2).display()
        # unionByName for mismatched - col position
        df1.unionByName(df2).display()
        ```
        
    - String Functions
        
        ```python
        # initcap()
        df.select(initcap("field_1"))
        # upper()
        df.select(upper("field_1"))
        # lower()
        df.select(lower("field_1"))
        ```
        
    - Date Functions
        
        ```python
        # current_date()
        df.withColumn("date", current_date())
        # date_add()
        df.withColumn("week_after", data_add("date", 7))
        # date_sub()
        df.withColumn("week_before", date_sub("date", 7))
        # date_sub() alternative
        df.withColumn("week_before", date_add("date", -7))
        ```
        
        - Datediff - to get the num of days between
            
            ```python
            df.withColumn("days", datediff("date", "week_before"))
            ```
            
        - Date_Format
            
            ```python
            df.withColumn("date", date_format("date", "dd-MM-yyyy"))
            ```
            
    - Handling Nulls
        
        ```python
        # drop nulls - options: any, all, subset
        df.dropna("any").display()
        # drop nulls - subset
        df.dropna(subset=["field_1"]).display()
        # fill nulls
        df.fillna("Not Available").display()
        # fill nulls - subset
        df.fillna("Not Available", subset=["field_1"]).display()
        ```
        
    - Split & Indexing - to split the text
        
        ```python
        # split
        df.withColumn("field_1", split("field_1", " "))
        # indexing
        df.withColumn("field_1", split("field_1", " ")[1])
        ```
        
    - **Explode - array to rows**
        
        ```python
        df = df.withColumn("field_1", split("field_1", " "))
        df.withColumn("field_1", explode("field_1")).display()
        ```
        
    - Array_Contains - check values in array
        
        ```python
        df.withColumn("flag", array_contains("field_1", "Type 1")).display()
        ```
        
    - **Group_By**
        
        ```python
        #1 - sum(), avg()
        df.groupBy("field_1").agg(sum("measure_1")).display()
        #2 - multiple cols
        df.groupBy("field_1", "field_2").agg(avg("measure_1").alias("Average"))
        #3
        df.groupBy("field_1", "field_2").agg(avg("measure_1"), sum("measure_2"))
        ```
        
    - Collect_List
        
        ```python
        df.groupBy("field_1").agg(collect_list("field_2"))
        ```
        
    - Pivot
        
        ```python
        df.groupBy("field_1").pivot("field_2").agg(avg("measure_1"))
        ```
        
    - When-Otherwise
        
        ```python
        #1
        df.withColumn("flag", when(col("field_1") == "Meat", "Non-veg").otherwise("Veg"))
        #2
        df.withColumn("flag", when((col("field_1") == "Veg") & (col("field_2") > 100), "Exp_Veg") \
        	.when((col("field_1") == "Veg") & (col("field_2") < 100), "Inexp_Veg") \
        	.otherwise("Non-Veg"))
        ```
        
    - **Joins**
        
        ```python
        # inner
        df1.join(df2, df1["field_1"] == df2["field_1"], "inner")
        # left
        df1.join(df2, df1["field_1"] == df2["field_1"], "left")
        # right
        df1.join(df2, df1["field_1"] == df2["field_1"], "right")
        # anti join - the records that don't match from the left df
        df1.join(df2, df1["field_1"] == df2["field_1"], "anti")
        ```
        
- Window Functions
    
    ```python
    from pyspark.sql.window import Window
    ```
    
    - Row Number
        
        ```python
        df.withColumn("row", row_number().over(Window.orderBy("id")))
        ```
        
    - Rank
        
        ```python
        df.withColumn("rank", rank().over(Window.orderBy("id")))
        ```
        
    - Dense Rank
        
        ```python
        df.withColumn("d_rank", dense_rank().over(Window.orderBy(col("id").desc())))
        ```
        
    - Cumulative Sum
        
        ```python
        # unboundedPreceding, curentRow, unboundedFollowing
        df.withColumn("cum_sum", sum("field_1").over(Window.orderBy("field_2") \
        	 .rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        ```
        
- User Defined Functions
    
    ```python
    def my_func(x):
    	return x * x
    
    my_udf = udf(my_func)
    
    df.withColumn("x2", my_udf("field_1"))
    ```
    
- Data Writing
    - CSV
        
        ```python
        df.write.format("csv") \
        	.save("/filepath/filename.csv")
        ```
        
    - Writing Modes
        
        ```python
        # Append
        df.write.format("csv") \
        	.mode("append").option("path", "/filepath/filename.csv").save()
        # Overwrite
        df.write.format("csv") \
        	.mode("overwrite").save("/filepath/filename.csv")
        # Error - To raise error if file exists
        df.write.format("csv") \
        	.mode("error").save("/filepath/filename.csv")
        # Ignore - To ignore if file exists
        df.write.format("csv") \
        	.mode("ignore").save("/filepath/filename.csv")
        ```
        
- Parquet Format
    - Columnar storage
    - Run-length compression
    
    ```python
    df.write.format("parquet") \
    	.mode("overwrite").save("/filepath/filename.csv")
    ```
    
    - Save as Table
        
        ```python
        df.write.format("parquet") \
        	.mode("overwrite").saveAsTable("my_table")
        ```
        
- Managed vs External Tables
    - Managed Table
        - Table managed by Databricks
        - It can delete the data
    - External Table
        - Table managed by Users
        - It cannot delete the data but can drop schema
- Spark SQL
    
    ```python
    # create a temp view
    df.createTempView("my_view")
    ```
    
    ```sql
    -- query the data
    select * from my_view
    where field_1 = 'Regular'
    ```
    
    ```python
    query = spark.sql("""
    	select * from my_view
    	where field_1 = 'Regular'
    """)
    
    query.display()
    ```