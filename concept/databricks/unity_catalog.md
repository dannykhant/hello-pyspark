# Unity Catalog

- Databricks
    - Control Plane
        - All the UI/UX features
    - Compute Plane
        - All the compute cluster resource
        - Cloud data storage
- Unity Catalog
    - Define once, secure everywhere
    - Unified data governance layer to support features such as -
        - Data Lineage
        - Data Audit
        - Data Discovery
    - With Unit Catalog, we have centralized metastore and user mgmt
- UC Object Model
    - Metastore → Catalog → Schema
    - Schema → Table, View, Volume, Function and Model
- Managed vs External
    - Managed Delta Table
        - Data is stored in the location of the Metastore
    - External Delta Table
        - Data is stored in the location we own
- External Table Creation
    - Create credential
    - Create external location
- Scenario-1
    - Managed Catalog, Managed Schema, Managed Table
        
        ```sql
        create catalog extcat;
        
        create schema extcat.pub;
        
        create table extcat.pub.bztbl
        (id int, name string)
        using delta;
        ```
        
- Scenario-2
    - External Catalog, Managed Schema, Managed Table
        
        ```sql
        create catalog extcat
        managed location 'abfss://.../external';
        
        create schema extcat.pub;
        
        create table extcat.pub.bztbl
        (id int, name string)
        using delta;
        ```
        
- Scenario-3
    - External Catalog, External Schema, Managed Table
        
        ```sql
        create catalog extcat
        managed location 'abfss://.../external';
        
        create schema extcat.extpub
        managed location 'abfss://.../external_schema';
        
        create table extcat.pub.bztbl
        (id int, name string)
        using delta;
        ```
        
- Scenario-4
    - External Catalog, External Schema, External Table
        
        ```sql
        create catalog extcat
        managed location 'abfss://.../external';
        
        create schema extcat.extpub
        managed location 'abfss://.../external_schema';
        
        create table extcat.pub.bztbl
        (id int, name string)
        using delta
        location 'abfss://.../external_tbl';
        ```
        
- Undrop Managed Table
    - We can restore the table deleted within 7 days
        
        ```sql
        undrop table cat.sch.tbl;
        ```
        
- View
    - Permanent View
        
        ```sql
        create view cat.sch.pview
        as
        select * from delta.`abfss://path/mytbl` where id = 1;
        ```
        
    - Temporary View (only alive in the current runtime)
        
        ```sql
        create temp view tview
        as
        select * from delta.`abfss://path/mytbl` where id = 1;
        ```
        
- Volume
    - Add governance over non-tabular data
    - Directory creation
        
        ```python
        dbutils.fs.mkdir("abfss://.../volume")
        ```
        
    - External volume creation
        
        ```sql
        create external volume cat.sch.extvol
        location 'abfss://.../volume';
        ```
        
    - Copy file
        
        ```python
        dbutils.fs.copy("abfss://.../src_data", "abfss://.../src_data")
        ```
        
    - Read file from the volume
        
        ```sql
        select * from csv.`/Volumes/cat/sch/extvol/file.csv`;
        ```
        
- Cloning
    - Deep Clone
        - Clone metadata and data
        
        ```sql
        create table cat.sch.tbl
        deep clone cat.sch.org;
        ```
        
    - Shallow Clone
        - Clone only metadata
        
        ```sql
        create table cat.sch.tbl
        shallow clone cat.sch.org;
        ```