
**Problem Statement (Summary)**
   1. Cleaning the data by removing rows with missing customer IDs
   2. Calculating total order amount for each customer
   3. Identifying the top 3 customers based on total spending
   4. Finding customers who have not placed any orders
   5. Calculating total revenue city-wise
   6. Finding the average order amount per customer
   7. Identifying customers with more than one order
   8. Sorting customers based on total spending

**Dataset Used:**
   - Dataset: customers, orders
   - Source: Spark Playground
   - Tables used: customers, orders
  

**Key Transformations:**
   - dropna() – for basic data cleaning
   - join() – to combine customers and orders data
   - groupBy() – to group data for aggregation
   - agg() – to calculate sum, average, count
   - filter() – to apply conditions
   - orderBy() – to sort the results
  

**Output:**
  1. Calculate total spending for each customer
  2. Identify top-performing customers
  3. Find customers without any orders
  4. Analyze revenue at the city level
  5. Compute average order values
  6. Sort and filter customers based on business conditions

**Learnings:**
  1. How to work with multiple datasets in PySpark
  2. How joins and aggregations are applied in PySpark
  3. How to write a code or a query in multiple ways
  

**Files in this Folder:**
  - phase2_problem_statement.pdf -> Problem description
  - Sql_Queries.sql2, pyspark_notebook_phase2.py.txt -> Implementation
  - outputs -> Results
