# Udacity Data Engineering Projects
In these projects, models were created for user activity data. The activity data is from a music streaming app call Sparkify. 
The database and ETL pipelines were designed to understand what songs users are listening to. 

# Data Lake with Spark
In this project, an ETL pipeline was built for a data lake. The data resides in S3, in a directory of JSON logs on user activity, as well as a directory with JSON metadata on the songs in the app. The data is loaded from S3, and processed into analytics tables using Spark, and loaded back into S3. This Spark is processed on a cluster using AWS.

# Cloud Data Warehouse 
In this project, you are tasked with building an ELT pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

# Data Lake with Postgres
In this project, a relational database and ETL pipeline are designed to optimize queries for understanding what songs users are listening to. Fact and Dimension tables are created as part of the process

# Pipelines with Airflow
The table creation and loading are automated using Airflow, a schedule was created for data pipelines with Airflow including the debug process. 
