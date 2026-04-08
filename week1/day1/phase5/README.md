-Problem Statement (Summary)
Load and prepare datasets in Databricks
Join multiple tables to combine customer, orders, and product data
Calculate key metrics (total spend, daily sales, lifetime value)
Apply aggregations and window functions (ranking, running totals)
Implement business logic (customer segmentation)
Build final reporting table for analysis

-Approach Description
Loaded all datasets into PySpark DataFrames
Checked data quality (nulls, row counts)
Joined multiple tables to create a unified dataset (fact table)
Applied aggregations (total spend, sales, order counts)
Used window functions for ranking and running totals
Built final reporting table with customer insights

-Key Transformations Used
Aggregation - groupBy() with sum(), count()
Joins - join() (inner joins across multiple tables)
Window functions - row_number(), dense_rank(), sum() over()
Filtering - filter()
Conditional logic - when() / otherwise()

-Key Learnings
Importance of checking and validating data before processing
Joining multiple datasets to build a complete view
Using aggregations for business insights
Applying window functions for ranking and running totals
Building an end-to-end data pipeline
