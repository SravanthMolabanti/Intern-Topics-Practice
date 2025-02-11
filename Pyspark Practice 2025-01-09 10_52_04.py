# Databricks notebook source
dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df=spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select('Item_Identifier','Item_Weight').display()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
# df.select(col('Item_Weight')).display()

# COMMAND ----------

df.select(col('Item_Weight').alias('Item_Wt'),col('Item_Identifier')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

df.filter((col('Item_Type')=='Soft Drinks')&(col('Item_Weight')<10)).display()

# COMMAND ----------

df.filter((col('Outlet_Location_Type').isin('Tier 1','Tier 2')&(col('Outlet_Size').isNotNull()))).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn('flag',lit("new column"))

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn('multiply',col('Item_Weight')*col('Item_MRP'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg")).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=df.withColumn('Item_weight',col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending=[0,0]).display()

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

df.drop('flag').display()

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.select(initcap('Item_Type')).display()

# COMMAND ----------

df.select(upper('Item_Type')).display()

# COMMAND ----------

df=df.withColumn('curr_date',current_date())

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn('week_after',date_add('curr_date',7))

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn('datediff',datediff('curr_date','week_after'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Item_Type']).display()

# COMMAND ----------

df.fillna("NotAvailable").display()

# COMMAND ----------

df.fillna("NotAvailable",subset=['Outlet_Size']).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[0]).display()

# COMMAND ----------

df_exp=df.withColumn('Outlet_Type',split('Outlet_Type',' '))


# COMMAND ----------

df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

df_exp.withColumn('Type1_Flag',array_contains('Outlet_Type',"Type1")).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book').alias('List of books')).display()

# COMMAND ----------

df.select('Item_Type','Outlet_Size','Item_MRP').display()

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.withColumn('Veg_Flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg'))
df.display()

# COMMAND ----------



# COMMAND ----------

df.withColumn('Veg_Exp_Flag',when(((col('Veg_Flag')=='Veg')&(col('Item_MRP')<100)),'Veg_InExpensive')\
    .when(((col('Veg_Flag')=='Veg')&(col('Item_MRP')>100)),'Veg_Expensive')\
        .otherwise('Non_Veg')).display()

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

# df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()
# df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()
# df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()
df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### WINDOWS FUNCTION

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
        .withColumn('denseRank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

df.withColumn('dum',sum('Item_MRP').over(Window.orderBy('Item_Identifier').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### UDF

# COMMAND ----------

def my_func(x):
    return x*x

# COMMAND ----------

my_udf=udf(my_func)

# COMMAND ----------

df.withColumn('my_new_col',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATA WRITING

# COMMAND ----------

df.write.format('csv')\
    .save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

df.write.format('csv')\
        .mode('append')\
        .save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

df.write.format('csv')\
        .mode('append')\
        .option('path','/FileStore/tables/CSV/data.csv')\
        .save()

# COMMAND ----------

df.write.format('csv')\
.mode('overwrite')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()

# COMMAND ----------

df.write.format('csv')\
.mode('error')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()

# COMMAND ----------

df.write.format('csv')\
.mode('ignore')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()

# COMMAND ----------

df.write.format('parquet')\
.mode('overwrite')\
.option('path','/FileStore/tables/CSV/data.csv')\
.save()

# COMMAND ----------

df.write.format('parquet')\
.mode('overwrite')\
.saveAsTable('my_table')

# COMMAND ----------

df.display()

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from my_view where Item_Fat_Content = 'Low Fat'

# COMMAND ----------

df_sql = spark.sql("select * from my_view where Item_Fat_Content = 'Low Fat'")

# COMMAND ----------

df_sql.display()
