from pyspark.sql import SparkSession
from pyspark.sql.functions import col,avg,count

spark = SparkSession.builder.appName("Practice").getOrCreate()

data = [
(1, "Ravi", "Hyderabad", 25),
(2, None, "Chennai", 32),
(None, "Arun", "Hyderabad", 28),
(4, "Meena", None, 30),
(4, "Meena", None, 30),
(5, "John", "Bangalore", -5)
]
columns = ["customer_id", "name", "city", "age"]
df = spark.createDataFrame(data, columns)

#Identify Data Issues
null values
null_vales=df.filter(col("customer_id").isNull() | col('name').isNull() |col('city').isNull() | col('age').isNull())
null_vales.show()

##duplicates
duplicates=df.groupBy("customer_id", "name", "city", "age").count().filter(col("count")>1)  ###grouping combines the same values 
duplicates.show()

##invalid values
invalid=df.filter(df["age"]<1)
invalid.show()

#Data Cleaning
##Removing null keys
df_clean=df.dropna()
df_clean.show()

##handling missing values
df_clean=df.filter((col("customer_id").isNotNull()))\
           .fillna({    
            "city": "unknown"                           ###filling the city with the value unknown
             })\
           .groupBy("customer_id", "name", "city", "age").count()
dropped=df_clean.dropna()
dropped.show()

##invalid age
invalid=df.filter(df["age"]<1)
invalid.show()

#Customers per city
no_of_customers=dropped.groupBy(dropped["city"]).agg(count("customer_id").alias("customers_count"))
no_of_customers.show()
