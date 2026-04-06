

**Problem Statement (Summary):**

  - Reading data from CSV, JSON, and Parquet files
  - Inspecting schema and understanding the dataset
  - Handling missing or invalid data
  - Filtering and cleaning the dataset
  - Calculating daily sales and city-wise revenue
  - Identifying repeat customers (more than 2 orders)
  - Finding highest spending customers in each city
  - Building a final reporting dataset with key metrics

**Dataset Used:**

  Datasets: customers, sales
  Source: Spark Playground
  Tables: customers, sales

**Approach:**

  1. I took the dataframes and identified the null, mismatched values from the datasets.
  2. Converted those transformed datasets into tables
  3. Written queries for all the questions using joins, subqueries.
  4. Used clauses and Window functions in the queries like Partition By, Row Number()and aggregate functions like sum, avg, count.
  5. Converted those sql queries to the pyspark codes
  6. Used methods like filter(), withColumn, groupBy(), agg() etc...
  7. Finally build reporting table on customers, total_spend, orders_count


**Key Transformations:**

  read() with different formats (CSV, JSON, Parquet)
  dropna() / fillna() – for handling missing data
  filter() – to remove invalid records
  join() – to combine datasets
  groupBy() and agg() – for aggregations
  orderBy() – for sorting results

**Outputs:**

  Transformed raw datasets
  Generate business insights like revenue and customer behavior
  Create a final reporting dataset with useful metrics

**Learnings:**
  - Learned how the ETL pipelines are implemented
  - Learned the importance of transforming the data
  - Understanding how joins impact data
  - Gaining confidence in translating SQL into pyspark codes


**Files in the Folder**

  - phase3_problem_statement.pdf -> Problem statement
  - Sql_queries.sql , pyspark_codes.py -> implementation 
  - outputs -> Results

