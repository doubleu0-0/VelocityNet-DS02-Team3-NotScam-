# VelocityNet
VelocityNet is an 11-week, 5-sprint program focused on migrating Velocity's database to the cloud, ensuring enhanced scalability, performance, and security. We plan to create a ELT pipeline to ingest data, load and transform the data using snowflake and outout the data connecting it to Power BI, allowing for real time insights to be found.

# How the code works

### Data Cleaning
**Code for data cleaning can be found in Code>DataCleaning<br>**
Code for data cleaning already in the task for automation and is not needed to run in the pipeline

### StoredProcedures
**Code for Stored Procedures can be found in Code>StoredProcedures<br>**
Code for stored procedures are to help with executing large groups of sql code especially in task to allow for automation of the pipeline process

### Extract
**Code for the setting up of the primary stage is the CodeStageSetup.sql file<br>**
This will contain the code to also create another stage to stage the file logs for the load code <br>
Ensure the code is ran in blocks instead of running all at once to prevent errors<br>

### Load
**The entire package code for the load can be found in Code>StoredProcedures>ExtractAndLoad.sql<br>**
It contains the code to create the streams, stored procedures, tables, views and pipes to make this work <br>
The tasks used for this portion is found in Code
**The tasks used for this portion is found in Code>Task>ExtractAndLoadTasks.sql<br>**
Make sure this is ran after the tasks in Transform have been ran since the Transform tasks run after <br>
the load tasks, suspension of tasks must start from the load tasks then transform tasks, resuming them is the opposite <br>
Ensure the load tasks are created after the objects in ExtractAndLoad.sql is done <br>
This Extract and Load codes will automate table creation and appending data

### Transform 
**Tasks can be found in the Code>Task<br>**
**CLEAN_DATA_TASK can be found in Code>Dynamic Tables>DataCleaning.sql<br>**
In the transformation process we would first create the tasks:<br>
CREATE_STREAMS_TASK<br>
TRANSFORM_DATA_TASK<br>
CLEAN_DATA_TASK<br>
CREATE_AGGREGATION_TASK<br>
<br>
This would clean the data and create and new aggregations for the dashboards

### Dynamic Tables
**Dynamic tables can be found in Code>Dynamic Tables<br>**
Run the code in:<br>
Offers.sql<br>
Product.sql<br>
SalesPerson&Region.sql<br>
purchasing.sql<br>
<br>
This would create all the dynamic tables needed for the dashboard

### Report Query
**Report Query are found in Code>Report Query<br>**
These would help the with reporting of the data with premade queries to see important metrics
