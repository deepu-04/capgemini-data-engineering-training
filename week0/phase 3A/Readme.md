**Objective**
  Work with intentionally messy data and apply cleaning techniques before building a pipeline. This phase
helps you understand why data cleaning is critical in real-world data engineering
_____________________________________________________________________________________________________________________________________________
**Problem Statement (Summary)**
  - Identify problems like null values, duplicates, and invalid data
  - Remove rows with missing key fields such as customer_id
  - Handle missing values in columns like name and city
  - Remove duplicate records
  - Filter out invalid values (for example, negative age)
  - Validate the cleaning process by comparing row counts
  - Perform aggregation to find number of customers per city
____________________________________________________________________________________________________________________________________________
**Dataset Used**
  - Datasets: customers
  - source: kaggle
  - Tables: customers
___________________________________________________________________________________________________________________________________________

**Approach**
  1. At First, I look at the data to identify missing values
  2. Identify nulls, invalid records and Duplicates
  3. Cleaning the mismatched data and duplicates using dropna(), fillna()
  4. Identifying the number of customers per city
______________________________________________________________________________________________________________________________________________
**Key Transformations**

  1. dropna() – to remove rows with missing key values
  2. fillna() – to handle missing non-key values
  3. filter() – to remove invalid data (e.g., age < 0)
  4. groupBy() and count() – for aggregation
____________________________________________________________________________________________________________________________________________

**Output** 

  - Cleaned dataset with valid and consistent records
  - Removed nulls, duplicates, and invalid values
  - Verified data quality through validation checks
  - Generated customer count per city
_________________________________________________________________________________________________________________________________________
**Challenges Faced**
  1. Identifying all data quality issues correctly at the beginning.
  2. Deciding which rows to remove vs which values to fill
  3. Handling duplicate records without losing valid data
  4. Ensuring cleaning steps did not remove useful information
  5. Validating that the cleaned data is accurate
________________________________________________________________________________________________________________________________________
**Learnings**
  - Real-world data is often messy and requires cleaning
  - Cleaning data is a critical first step before any analysis
  - Small data issues can lead to incorrect results
  -  How to systematically handle nulls, duplicates, and invalid values
________________________________________________________________________________________________________________________________________
**Files in this Folder**
  - phase3a_data_quality_challenge.pdf -> Problem description
  - pyspark_code.py -> Implementation
  - outputs/ -> Results
