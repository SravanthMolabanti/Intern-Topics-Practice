# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ## Marketing Analysis
# MAGIC
# MAGIC #### Directories
# MAGIC
# MAGIC - /FileStore/tables/CUSTOMERS.csv
# MAGIC - /FileStore/tables/ORDERS.csv
# MAGIC - /FileStore/tables/ORDER_PAYMENTS.csv
# MAGIC - /FileStore/tables/ORDER_REVIEW_RATINGS.csv
# MAGIC - /FileStore/tables/ORDER_ITEMS.csv
# MAGIC - /FileStore/tables/PRODUCTS.csv
# MAGIC - /FileStore/tables/SELLERS.csv
# MAGIC - /FileStore/tables/GEO_LOCATION.csv
# MAGIC
# MAGIC
# MAGIC #### DataFrames Created
# MAGIC
# MAGIC - customers_df
# MAGIC - orders_df
# MAGIC - order_payments_df
# MAGIC - order_reviews_df
# MAGIC - order_items_df
# MAGIC - products_df
# MAGIC - sellers_df
# MAGIC - geolocation_df
# MAGIC
# MAGIC #### Delta Tables Created
# MAGIC
# MAGIC - customer_table
# MAGIC - orders_table
# MAGIC - order_payments_table
# MAGIC - order_reviews_table
# MAGIC - order_items_table
# MAGIC - products_table
# MAGIC - sellers_table
# MAGIC - geolocation_table

# COMMAND ----------

import pandas as pd
import numpy  as np
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

customers_df=spark.read.format('csv') \
    .option('inferSchema',True) \
        .option('header',True) \
            .load('/FileStore/tables/CUSTOMERS.csv')

customers_df.printSchema()
customers_df.display()

# COMMAND ----------

customers_df.write.format("delta").mode("overwrite").saveAsTable("customer_table")

# COMMAND ----------

display(spark.sql("select * from customer_table"))

# COMMAND ----------

orders_df=spark.read.format('csv') \
    .option('inferSchema',True) \
        .option('header',True) \
            .load('/FileStore/tables/ORDERS.csv')

orders_df.printSchema()
orders_df.display()


# COMMAND ----------

orders_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("order_table")

# COMMAND ----------

display(spark.sql("select * from order_table"))

# COMMAND ----------

order_payments_df=spark.read.format('csv') \
    .option('inferSchema',True) \
        .option('header',True) \
            .load('/FileStore/tables/ORDER_PAYMENTS.csv')

order_payments_df.printSchema()
order_payments_df.display()


# COMMAND ----------

order_payments_df.write.format("delta").mode("overwrite").saveAsTable("order_payments_table")

# COMMAND ----------

display(spark.sql("select * from order_payments_table"))

# COMMAND ----------

order_reviews_df=spark.read.format('csv') \
    .option('inferSchema',True) \
        .option('header',True) \
            .load('/FileStore/tables/ORDER_REVIEW_RATINGS.csv')

order_reviews_df.printSchema()
order_reviews_df.display()

# COMMAND ----------

order_reviews_df.write.format("delta").mode("overwrite").saveAsTable("order_reviews_table")

# COMMAND ----------

display(spark.sql("select * from order_reviews_table"))

# COMMAND ----------

order_items_df=spark.read.format('csv') \
    .option('inferSchema',True) \
        .option('header',True) \
            .load('/FileStore/tables/ORDER_ITEMS.csv')

order_items_df.printSchema()
order_items_df.display()

# COMMAND ----------

order_items_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("order_items_table")

# COMMAND ----------

display(spark.sql("select * from order_items_table"))

# COMMAND ----------

products_df=spark.read.format('csv') \
    .option('inferSchema',True) \
        .option('header',True) \
            .load('/FileStore/tables/PRODUCTS.csv')

products_df.printSchema()
products_df.display()

# COMMAND ----------

products_df.write.format("delta").mode("overwrite").saveAsTable("products_table")

# COMMAND ----------

display(spark.sql("SELECT * from products_table"))

# COMMAND ----------

sellers_df=spark.read.format('csv') \
    .option('inferSchema',True) \
        .option('header',True) \
            .load('/FileStore/tables/SELLERS.csv')

sellers_df.printSchema()
sellers_df.display()


# COMMAND ----------

sellers_df.write.format("delta").mode("overwrite").saveAsTable("sellers_table")

# COMMAND ----------

display(spark.sql("select * from sellers_table"))

# COMMAND ----------

geolocation_df=spark.read.format('csv') \
    .option('inferSchema',True) \
        .option('header',True) \
            .load('/FileStore/tables/GEO_LOCATION.csv')

geolocation_df.printSchema()
geolocation_df.display()

# COMMAND ----------

geolocation_df.write.format("delta").mode("overwrite").saveAsTable("geolocation_table")

# COMMAND ----------

display(spark.sql("select * from geolocation_table"))

# COMMAND ----------

order_items_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE order_items_table SET TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5',
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE order_items_table RENAME COLUMN  Quantity TO order_item_id

# COMMAND ----------


order_items_table1 = spark.table("order_items_table")

order_items_table1 = order_items_table1.withColumnRenamed("order_item_id", "Quantity")

order_items_table1.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("order_items_table")

display(spark.sql("select * from order_items_table"))


# COMMAND ----------

order_items_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order_items_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order_table

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, date_format

orders_df1 = spark.table("order_table")

orders_df1 = orders_df1.withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp")) \
                       .withColumn("order_approved_at", to_timestamp("order_approved_at")) \
                       .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date")) \
                       .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date")) \
                       .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date")) \
                       .withColumn("year_month", date_format(to_timestamp("order_purchase_timestamp"), "yyyy-MM"))

orders_df1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("order_table")

display(spark.sql("SELECT * FROM order_table LIMIT 5"))


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE order_table AS
# MAGIC SELECT 
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_status,
# MAGIC     CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
# MAGIC     CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
# MAGIC     CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
# MAGIC     CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
# MAGIC     CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,
# MAGIC     DATE_FORMAT(CAST(order_purchase_timestamp AS TIMESTAMP), 'yyyy-MM') AS year_month
# MAGIC FROM order_table;
# MAGIC

# COMMAND ----------

#change dataframe on changing anything in delta table
order_item1 = spark.read.format("delta").table("default.order_items_table")
display(order_item1)

# COMMAND ----------

from pyspark.sql.functions import col

order_items_table1 = spark.table("order_items_table")

order_items_cleaned = order_items_table1.dropDuplicates(subset=['order_id', 'product_id', 'seller_id', 'shipping_limit_date'])

order_items_cleaned.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("order_items_table")

display(spark.sql("select * from order_items_table"))


# COMMAND ----------

from pyspark.sql.functions import col

order_items_table1 = spark.table("order_items_table")

order_items_table1 = order_items_table1.withColumn(
    "tot_cost", 
    (col("Quantity") * col("price") + col("freight_value"))
)

order_items_table1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("order_items_table")

display(spark.sql("select * from order_items_table"))


# COMMAND ----------


order_items_df1 = spark.table("order_items_table")
orders_df1 = spark.table("order_table")

ord_item_orders = order_items_df1.join(orders_df1, on="order_id", how="left")

ord_item_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("ord_item_orders_table")

display(spark.sql("select * from ord_item_orders_table"))


# COMMAND ----------


display(spark.sql("SELECT ROUND(SUM(price), 2) AS Total_Revenue FROM order_items_table"))

display(spark.sql("SELECT SUM(Quantity) AS Total_Quantity FROM order_items_table"))

display(spark.sql("SELECT COUNT(DISTINCT product_id) AS Total_Products FROM products_table"))

display(spark.sql("SELECT COUNT(DISTINCT customer_unique_id) AS Total_Customers FROM customer_table"))

display(spark.sql("SELECT COUNT(DISTINCT seller_id) AS Total_Sellers FROM sellers_table"))

display(spark.sql("SELECT COUNT(DISTINCT geolocation_zip_code_prefix) AS Total_Locations FROM geolocation_table"))

display(spark.sql("SELECT COUNT(DISTINCT product_category_name) AS Total_Product_Categories FROM products_table"))

display(spark.sql("SELECT COUNT(DISTINCT payment_type) AS Total_Payment_Methods FROM order_payments_table"))


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE order_cust_table AS
# MAGIC SELECT *
# MAGIC FROM order_table
# MAGIC LEFT JOIN customer_table USING(customer_id);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM order_cust_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE order_cust_table SET TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5',
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE order_cust_table DROP COLUMNS (year_month, year_month1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM order_table;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cust_month_table AS
# MAGIC SELECT customer_unique_id, year_month1
# MAGIC FROM order_cust_table;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE order_table SET TBLPROPERTIES (
# MAGIC   'delta.minReaderVersion' = '2',
# MAGIC   'delta.minWriterVersion' = '5',
# MAGIC   'delta.columnMapping.mode' = 'name'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE order_table DROP COLUMNS (year_month, year_month1,year_month2)

# COMMAND ----------


