# SparkSQL

- Temp View
    
    ```python
    df.createOrReplaceTempView("orders_temp")
    ```
    
- TempView vs GlobalTempView
    - TempView lasts only in the running session of a user
    - GlobalTempView will be available for all the sessions
- Managed vs External Tables
    - Managed Tables
        - Metastore → Managed Cloud Storage
        - Data can be dropped because it’s managed by Metastore.
        
        ```sql
        create catalog my_catalog;
        create schema my_catalog.main_schema;
        
        # store in metastore's managed location
        create table my_catalog.main_schema.orders_managed
        as select * from orders_temp;
        
        # this will drop the data
        drop table my_catalog.main_schema.orders_managed;
        ```
        
    - External Tables
        - Data cannot be dropped because Metastore doesn’t own the external storage.
        
        ```sql
        # store in metastore's managed location
        create table my_catalog.main_schema.orders_external
        location 'abfss://sql@databricks.dfs.core.windows.net/dest/orders_ext'
        as select * from orders_temp;
        ```
        
- Unity Catalog
    - catalog.schema.table - stored in Unity Metastore of Databricks
- SQL Warehouses
    - Query engine optimized to run SQL on Databricks
- Query in Python
    
    ```sql
    query = spark.sql("""select * from my_catalog.main_schema.orders_external""")
    query.display()
    ```
    
- Filtering
    
    ```sql
    select * from my_catalog.main_schema.orders_external
    where product_category = 'Fashion';
    ```
    
- Aggregations
    
    ```sql
    -- from -> where -> group by -> select
    select 
    	month(order_date) as order_month,
    	product_category,
    	count(order_id) as total_orders
    from my_catalog.main_schema.orders_external
    group by 
    	order_month, product_category
    order by order_month, total_orders desc;
    ```
    
- Subquery
    
    ```sql
    select
    	*
    from (
    	select 
    		month(order_date) as order_month,
    		product_category,
    		count(order_id) as total_orders
    	from my_catalog.main_schema.orders_external
    	group by 
    		order_month, product_category
    ) as t1
    where product_category = 'Home Decor'
    ```
    
- Conditionals
    
    ```sql
    select 
    	*,
    	case when (payment_method like '%card') 
    			and (order_status in ('Cancelled', 'Returned')) then 'Card'
    		when when (payment_method in ('Paypal', 'UPI')
    			and (order_status in ('Cancelled', 'Returned')) then 'Cash'
    		else 'No Value'
    	end as payment_flag
    from my_catalog.main_schema.orders_external
    ```
    
- Distinct
    
    ```sql
    select distinct(payment_method) from my_catalog.main_schema.orders_external
    ```
    
- CTEs
    
    ```sql
    with t1 as (
    	select 
    		month(order_date) as order_month,
    		product_category,
    		count(order_id) as total_orders
    	from my_catalog.main_schema.orders_external
    	group by 
    		order_month, product_category
    )
    select * from t1
    where product_category = 'Home Decor'
    ```
    
- Windows
    
    ```sql
    select 
    	price_per_unit,
    	rank() over (order by price_per_unit desc) rank,
    	dense_rank() over (order by price_per_unit desc) d_rank,
    	row_number() over (order by price_per_unit desc) rn,
    from my_catalog.main_schema.orders_external
    ```
    
    ```sql
    select
    	price_per_unit,
    	sum(price_per_unit) over (rows between unbounded preceding and unbounded following) total,
    	sum(price_per_unit) over (order by (price_per_unit) rows between unbounded preceding and current row) running_total
    from my_catalog.main_schema.orders_external
    ```
    
- Merge/ Upsert
    
    ```python
    df = spark.read.table("my_catalog.main_schema.orders_managed")
    # opt-1
    df.createTempTable("orders_source")
    # opt-2
    spark.sql("select * from {orders_source}", orders_source = df)
    ```
    
    ```python
    merge into my_catalog.main_schema.orders_external as dst
    using order_source as src
    	on dst.order_id = src.order_id
    when matched then
    	update set * 
    when not matched then
    	insert *
    ```
    
- Functions
    - Scalar Function (UDF)
        
        ```sql
        create or replace function my_catalog.main_schema.discount_price(price decimal(10, 2)) returns deciman(10, 2)
        language sql
        return price * 0.90;
        
        select 
        	price_per_unit,
        	my_catalog.main_schema.discount_price(price_per_unit)
        from my_catalog.main_schema.orders_managed;
        ```
        
    - Table Function (UDTF)
        
        ```sql
        create or replace function my_catalog.main_schema.filter_orders(category string) returns table
        language sql
        return
        (select * from my_catalog.main_schema.orders_managed 
        where product_category = category);
        
        select * from my_catalog.main_schema.filter_orders('Home Decor');
        ```
        
- Dynamic Data Masking
    - Masking PII to protect from unauthorized people
    - Column Masking Function
        
        ```sql
        create or replace function my_catalog.main_schema.mask_pii(user_id string)
        returns string
        language sql
        return
        case when is_account_group_member('admin') then user_id else '*****' end
        
        -- Applying
        alter table my_catalog.main_schema.orders_managed
        alter column user_id set mask my_catalog.main_schema.mask_pii;
        ```
        
- Row-level Security
    - Mapping Table
        
        ```sql
        create table my_catalog.main_schema.map_table
        (
        	payment_category string,
        	email string
        )
        
        insert into my_catalog.main_schema.map_table
        values
        ('Credit Card', 'user@gmail.com'),
        ('Debit Card', 'user@gmail.com'),
        ('Paypal', 'other@gmail.com'),
        ('UPI', 'other@gmail.com')
        ```
        
    - current_user() returns email id
    - Converting Mapping Table into Boolean Function
        
        ```sql
        create or replace function my_catalog.main_schema.row_security(payment_method String)
        returns boolean
        language sql
        return
        (exists
        	(
        		select * from my_catalog.main_schema.map_table
        		where email = current_user()
        		and payment_category = payment_method 
        	)
        )
        ```
        
    - Apply to the Column
        
        ```sql
        alter table my_catalog.main_schema.orders_managed
        set row filter my_catalog.main_schema.row_security on (payment_method)
        ```
        
- Delta Lake
    - Can perform DML, DDL operations on top of Delta tables
    - DML
        
        ```sql
        update my_catalog.main_schema.orders_mananged
        set product_category = 'GenZ Fashion'
        where product_category = 'Fashion'
        
        -- describe
        describe my_catalog.main_schema.orders_managed
        
        -- extended
        describe extended my_catalog.main_schema.orders_managed
        ```
        
    - Data Versioning
        - To view history of the table
            
            ```sql
            describe history my_catalog.main_schema.orders_managed
            ```
            
        - Time-traveling
            
            ```sql
            restore my_catalog.main_schema.orders_managed to version as of 3
            ```
            
    - Managing Delta files
        
        ```sql
        describe history delta.`abfss://spark@databrick.dfs.core.windows.net/src/orders`
        
        restore delta.`abfss://spark@databrick.dfs.core.windows.net/src/orders` to version as of 0
        ```