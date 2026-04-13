Problem Statement (Summary)-
Clean and normalize data (emails, duplicates, invalid records)
Create unified mapping between emails and student_id
Join datasets to identify submitted, not submitted, and invalid records
Detect duplicates using window functions
Calculate metrics (submission count, multiple email usage)
Build final classification output (Submitted, Not Submitted, Duplicate, Invalid)

Approach Description-
Loaded all datasets into PySpark DataFrames
Cleaned and normalized data (emails formatting, removed duplicates)
Created unified email-to-student mapping table
Joined datasets to identify valid, invalid, and missing submissions
Applied window functions to detect duplicates
Generated final classification and reporting output

Key Transformations Used-
Aggregation - groupBy() with count()
Joins - join() (inner, left, left_anti)
Window functions - ROW_NUMBER()
Filtering - filter()
Data cleaning - lower(), trim()

Key Learnings-
Importance of cleaning and normalizing data
Validating data using joins (valid vs invalid records)
Using window functions to handle duplicates
Building an end-to-end data pipeline
