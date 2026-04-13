Problem Statement-
Clean and preprocess multiple datasets
Handle nulls, duplicates, and invalid data
Fix negative values and trim strings
Remove invalid foreign keys
Join all datasets to create a final dataset
Perform basic analytics and generate insights

Approach-
Loaded datasets into PySpark
Handled null values using fillna()
Trimmed string columns
Removed duplicates
Fixed negative prices
Removed invalid foreign keys using joins
Joined all cleaned datasets
Created revenue column
Performed aggregations

Key Transformations-
fillna()
trim()
dropDuplicates()
when()
joins (inner, left_anti)
groupBy()
window functions

Key Learnings-
Data cleaning is important before analysis
Handling nulls and duplicates properly
Avoiding ambiguity after joins
Using joins to ensure valid data
Performing basic analytics in PySpark
