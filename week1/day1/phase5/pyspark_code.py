from pyspark.sql.functions import *
from pyspark.sql.window import Window
print('Setup complete')
# Upload ALL CSV files and Ensure files are placed
display(dbutils.fs.ls("/Volumes/workspace/default/olist_volume"))

orders = spark.read.csv('/Volumes/workspace/default/olist_volume/olist_orders_dataset.csv', header=True, inferSchema=True)
order_items = spark.read.csv('/Volumes/workspace/default/olist_volume/olist_order_items_dataset.csv', header=True, inferSchema=True)
customers = spark.read.csv('/Volumes/workspace/default/olist_volume/olist_customers_dataset.csv', header=True, inferSchema=True)
products = spark.read.csv('/Volumes/workspace/default/olist_volume/olist_products_dataset.csv', header=True, inferSchema=True)
payments = spark.read.csv('/Volumes/workspace/default/olist_volume/olist_order_payments_dataset.csv', header=True, inferSchema=True)

display(orders)
display(customers)
display(order_items)
orders.printSchema()

orders.select([count(when(col(c).isNull(), c)).alias(c) for c in orders.columns]).show()
print('Orders:', orders.count())
print('Customers:', customers.count())

orders_customers = orders.join(customers, 'customer_id', 'inner')
display(orders_customers)

fact_orders = order_items.join(orders, 'order_id').join(customers, 'customer_id').join(payments, 'order_id')
display(fact_orders)
fact_orders.printSchema()

# Task 1: Top 3 Customers per City
spend_df = fact_orders.groupBy("customer_city", "customer_id") \
    .agg(sum("payment_value").alias("total_spend"))
window_spec = Window.orderBy(col("total_spend").desc())
ranked_df = spend_df.withColumn("rank", row_number().over(window_spec))

top3_df = ranked_df.filter(col("rank") <= 3)
top3_df.select(
    "customer_city",
    "customer_id",
    "total_spend",
    "rank"
).show()

# Task 2: Running Total of Sales
daily_df = fact_orders.withColumn("date", to_date("order_purchase_timestamp")).groupBy("date") \
    .agg(sum("payment_value").alias("daily_sales"))
window_spec = Window.orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
final_df = daily_df.withColumn("running_total",
    sum("daily_sales").over(window_spec)
)
final_df.show()

# Task 3: Top Products per Category
prod_df = fact_orders.join(products, "product_id")
sales_df = prod_df.groupBy("product_category_name", "product_id") \
    .agg(sum("payment_value").alias("total_sales"))

window_spec = Window.partitionBy("product_category_name") \
                    .orderBy(col("total_sales").desc())
ranked_df = sales_df.withColumn("rank", dense_rank().over(window_spec))
ranked_df.select(
    "product_category_name",
    "product_id",
    "total_sales",
    "rank"
).show()

# Task 4: Customer Lifetime Value
clv_df = fact_orders.groupBy("customer_id") \
    .agg(sum("payment_value").alias("total_spend"))
clv_df.show()

# Task 5: Customer Segmentation
clv_df = fact_orders.groupBy("customer_id") \
    .agg(sum("payment_value").alias("total_spend"))
seg_df = clv_df.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when(col("total_spend") >= 5000, "Silver")
    .otherwise("Bronze")
)
seg_df.show()
seg_df.groupBy("segment") \
    .agg(count("*").alias("customer_count")) \
    .show()

# Task 6: Final Reporting Table
spend_df = fact_orders.groupBy("customer_id") \
    .agg(sum("payment_value").alias("total_spend"))
orders_df = fact_orders.groupBy("customer_id") \
    .agg(count("order_id").alias("total_orders"))
city_df = fact_orders.select("customer_id", "customer_city")

seg_df = spend_df.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when(col("total_spend") >= 5000, "Silver")
    .otherwise("Bronze")
)
final_df = seg_df \
    .join(orders_df, "customer_id") \
    .join(city_df, "customer_id")
final_df = final_df.select(
    "customer_id",
    "customer_city",
    "total_spend",
    "segment",
    "total_orders"
)
final_df.show()
