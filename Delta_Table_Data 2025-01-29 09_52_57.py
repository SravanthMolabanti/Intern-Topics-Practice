# Databricks notebook source
# MAGIC %md
# MAGIC #### Directories
# MAGIC /FileStore/tables/customers.csv
# MAGIC
# MAGIC /FileStore/tables/transactions.csv
# MAGIC
# MAGIC /FileStore/tables/Telco_customer_churn_1.xlsx
# MAGIC
# MAGIC /FileStore/tables/WA_Fn_UseC__Telco_Customer_Churn_2.csv
# MAGIC
# MAGIC
# MAGIC #### DataFrames Created
# MAGIC
# MAGIC - transactions_df
# MAGIC - customers_df
# MAGIC - telecom_churn1_df
# MAGIC - telecom_churn2_df
# MAGIC
# MAGIC #### Delta Tables Created
# MAGIC
# MAGIC - transactions_table
# MAGIC - customers_table
# MAGIC - telecom_churn1_table
# MAGIC - telecom_churn2_table
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

transactions_df=spark.read.format('csv') \
    .option('inferSchema',True) \
        .option('header',True) \
            .load('/FileStore/tables/transactions.csv')

transactions_df.printSchema()
transactions_df.display()

# COMMAND ----------

transactions_df.write.format("delta").mode("overwrite").saveAsTable("transactions_table")

# COMMAND ----------

display(spark.sql("select * from transactions_table"))

# COMMAND ----------

customers_df=spark.read.format('csv') \
    .option('inferSchema',True) \
        .option('header',True) \
            .option('multiline',True) \
                .load('/FileStore/tables/customers.csv')

customers_df.printSchema()
customers_df.display()

# COMMAND ----------

customers_df.write.format("delta").mode("overwrite").saveAsTable("customers_table")

# COMMAND ----------

display(spark.sql("select * from customers_table"))

# COMMAND ----------

telecom_churn2_df=spark.read.format('csv') \
    .option('inferSchema',True) \
        .option('header',True) \
            .load('/FileStore/tables/WA_Fn_UseC__Telco_Customer_Churn_2.csv')

telecom_churn2_df.printSchema()
telecom_churn2_df.display()

# COMMAND ----------

telecom_churn2_df.write.format("delta").mode("overwrite").saveAsTable("telecom_churn2_table")


# COMMAND ----------


display(spark.sql("select * from telecom_churn2_table"))

# COMMAND ----------


telecom_churn1_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
        .option("inferSchema",True) \
            .load("/FileStore/tables/Telco_customer_churn_1.xlsx")

telecom_churn1_df.printSchema()
telecom_churn1_df.display()
# .option("dataAddress", "'Sheet1'!A1") \  # Adjust sheet name/range if necessary

# COMMAND ----------

print(telecom_churn1_df.columns)

# COMMAND ----------

cleaned_columns = [col.replace(' ', '_') for col in telecom_churn1_df.columns]

# for old_name, new_name in zip(telecom_churn1_df.columns, cleaned_columns):
#     telecom_churn1_df = telecom_churn1_df.withColumnRenamed(old_name, new_name)

telecom_churn1_df=telecom_churn1_df.toDF(*cleaned_columns)

telecom_churn1_df.printSchema()
telecom_churn1_df.display()

# COMMAND ----------

telecom_churn1_df.write.format("delta").mode("overwrite").saveAsTable("telecom_churn1_table")

# COMMAND ----------

display(spark.sql("select * from telecom_churn1_table"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc extended telecom_churn1_table
# MAGIC
# MAGIC -- desc history telecom_churn1_table

# COMMAND ----------

transactions_temp_df=spark.read.format('csv') \
    .option('inferSchema',True) \
        .option('header',True) \
            .load('/FileStore/tables/transactions.csv')

transactions_temp_df.printSchema()
transactions_temp_df.display()

transactions_temp_df.write.format("delta").mode("overwrite").saveAsTable("transactions_temp_table")

# COMMAND ----------

display(spark.sql("select * from customers_table"))

# COMMAND ----------

display(spark.sql("select * from transactions_temp_table"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # select tr.customerid,sum(tr.purchaseamountusd)as total_Sum from customers_table as cus inner join transactions_temp_table as tr on cus.customerid=tr.customerid group by tr.customerid order by tr.customerid
# MAGIC
# MAGIC SELECT 
# MAGIC     cus.customerid, 
# MAGIC     cus.maritalstatus,
# MAGIC     SUM(tr.purchaseamountusd) AS total_Sum 
# MAGIC FROM customers_table AS cus 
# MAGIC INNER JOIN transactions_temp_table AS tr 
# MAGIC     ON cus.customerid = tr.customerid 
# MAGIC GROUP BY 
# MAGIC     tr.customerid,cus.maritalstatus,cus.customerid
# MAGIC ORDER BY 
# MAGIC     cus.customerid;
# MAGIC

# COMMAND ----------

df_with_year = transactions_temp_df.withColumn("order_year", year(transactions_temp_df["date"]))

# COMMAND ----------

df_with_year.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("transactions_temp_table")


# COMMAND ----------

display(spark.sql("select * from transactions_temp_table"))

# COMMAND ----------

# telecom_churn1_df_v1=telecom_churn1_df.withColumn('CustomerID',split('CustomerID','-'))
telecom_churn1_df_v1=telecom_churn1_df.withColumn('CustomerID_split',split('CustomerID','-'))

telecom_churn1_df_v1.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("telecom_churn1_table_v1")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from telecom_churn1_table_v1

# COMMAND ----------

print("hey spark!")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC desc history transactions_temp_table

# COMMAND ----------

telecom_churn1_df.display()

# COMMAND ----------

telecom_churn1_df_v1.display()

# COMMAND ----------

# telecom_churn1_df_v1=telecom_churn1_df.withColumn('CustomerID',split('CustomerID','-'))
telecom_churn1_df_v1=telecom_churn1_df_v1.withColumn('CustomerID_split',explode('collect_list(CustomerID_split)'))
telecom_churn1_df_v1.display()


# telecom_churn1_df_v1.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("telecom_churn1_table_v1")



# COMMAND ----------

telecom_churn1_df_v1=telecom_churn1_df_v1.groupBy('CustomerID').agg(collect_list('CustomerID_split'))

# COMMAND ----------

telecom_churn1_df_v1.display()

# COMMAND ----------

telecom_churn1_df.display()

# COMMAND ----------

print("hey")
