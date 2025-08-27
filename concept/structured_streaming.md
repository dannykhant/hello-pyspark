# Streaming

- Continuous processing of incoming unbounded data in real-time or near real-time
- Micro-batching
    - Instead of processing one record at a time, Spark processes the records within a small period called micro-batch.
- Structured Streaming
    - To overcome the difficulties with the first Streaming version such as RDD based API, not well integrated with SQL/DF API, not unified with batch code.
    - Stream = Unbounded Table
    - New data will be added into the stream table as new record
    - It maintains the state of changes
- Stateless Transformation
    - Spark doesn’t need to worry about the data written previously
    - eg., select, filter
- Stateful Transformation
    - Spark needs to track of the state of data written previously by looking at the data stored in executor memory
    - eg., groupBy
- Output Modes
    - Append
        - Support only Stateless Transformations
        
        ```python
        df = spark.readStream.format("json")\
        				.option("multiLine", True)\
        				.option("cleanSource", "archive")\
        				.option("sourceArchiveDir", "/Volumes/path/archive")\
        				.schema(my_schema)\
        				.load("/Volumes/path/data")
        				
        df.writeStream.format("delta")\
        			.outputMode("append")\
        			.trigger(once=True)\
        			.option("path", "/Volumes/path/data")\
        			.option("checkpointLocation", "Volumes/path/checkpoint")\
        			.start()
        ```
        
    - Update
        - Generate the only additional keys in the aggregation output
    - Complete
        - Generate all the keys in the aggregation output
- Checkpoint
    - metadata - Stores streaming query ID
    - sources - Take cares of idempotency
    - offsets - Assign the index
    - commits - Mark the processed files
- explode() vs explode_outer()
    - explode() - drop the nulls and explode
    - explode_outer() - just explode
        - Suitable for JSON unpacking
- Triggers
    - Default
        - Depends on the processing time the previous batch takes
    - Processing Time
        - To process the batch every specific time given
    - Once
        - To process just once
    - Available Now
        - To process just once but by creating micro-batches gradually
    - Continuous
        - This mode processing data row by row (NOT micro-batching)
        - This requires to set time for writing checkpoint
- Archiving Source Files
    - Move the processed file to the archive location if there is a processing on the source
- For Each Batch
    - Used to write to the multiple sinks at the same time
    
    ```python
    def write_fn(df, batch_id):
    	df = df.groupBy("color").count()
    	
    	# dest1
    	df.write.format("delta")\
    			.mode("append")\
    			.option("path", "/Volumes/path1")\
    			.save()
    			
    	# dest2
    	df.write.format("delta")\
    		.mode("append")\
    		.option("path", "/Volumes/path2")\
    		.save()
    		
    df.writeStream.foreachBatch(write_fn)\
    			.outputMode("append")\
    			.trigger(once=True)\
    			.option("checkpointLocation", "/Volumes/path")
    			.start()
    ```
    
- Event vs Processing Time
    - Event Time - The time generated the event
    - Processing Time - The time processed the data
- Window Operations
    - Aggregation based on the specific time window
    - Tumbling Windows
        - Non overlapping fixed size
        
        ```python
        df = df.groupBy("color", window("event_time", "10 minutes")).count()
        ```
        
    - Sliding Windows
        - Overlapping fixed size
    - Session Windows
        - Non-fixed size and based on data arrival time
- Watermarking
    - Store the state for the specific period
    - Only compatible with Update Mode