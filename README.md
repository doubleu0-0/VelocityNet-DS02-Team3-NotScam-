# VelocityNet
VelocityNet is an 11-week, 5-sprint program focused on migrating Velocity's database to the cloud, ensuring enhanced scalability, performance, and security. We plan to create a ELT pipeline to ingest data, load and transform the data using snowflake and outout the data connecting it to Power BI, allowing for real time insights to be found.

# How the code works

### Data Cleaning
**Code for data cleaning can be found in Code>DataCleaning<br>**
Code for data cleaning already in the task for automation and is not needed to run in the pipeline

### StoredProcedures
**Code for Stored Procedures can be found in Code>StoredProcedures<br>**
Code for stored procedures are to help with executing large groups of sql code especially in task to allow for automation of the pipeline process

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

### CI/CD Pipeline
This workflow automates code quality checks, formatting, and Snowflake integration. It installs dependencies, sets up Python and Snowflake CLI, and verifies the Snowflake connection. The workflow converts Jupyter notebooks to Python scripts, performs linting (flake8), formatting (black, isort), and SQL validation (sqlfluff). It ensures all files are properly structured and formatted, and if not then it raises errors which can be seen in the Github Actions tab, streamlining collaboration and maintaining code consistency before the code is pushed to the main branch.

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
