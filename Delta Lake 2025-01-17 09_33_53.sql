-- Databricks notebook source

create database delta_test

-- COMMAND ----------

use delta_test

-- COMMAND ----------

drop table if exists test1;
create table test1
(
  id int
) using parquet

-- COMMAND ----------

insert into test1 values(1),(2),(3)

-- COMMAND ----------

select * from test1;

-- COMMAND ----------

delete from test1 where id=1;

-- COMMAND ----------

desc extended test1

-- COMMAND ----------

drop table if exists test2;
create table test2
(
  id int
) using delta

-- COMMAND ----------

insert into test2 values(1),(2),(3)

-- COMMAND ----------

select * from test2;

-- COMMAND ----------

delete from test2 where id=1;

-- COMMAND ----------

select * from test2; 

-- COMMAND ----------

desc extended test2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Update

-- COMMAND ----------

update test2 set id=4 where id=2

-- COMMAND ----------

select * from test2;

-- COMMAND ----------

drop table if exists test3;
create table test3
(
  id int
)using delta

-- COMMAND ----------

insert into test3 values(1),(2),(3)

-- COMMAND ----------

select * from test3;

-- COMMAND ----------

desc extended test3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Merge Tables

-- COMMAND ----------

Merge into test2 t2
using test3 t3
on t2.id=t3.id
when matched then update set *
when not matched then insert *

-- COMMAND ----------

select * from test2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##History

-- COMMAND ----------

desc history test2

-- COMMAND ----------

-- select * from test2 version as of 4
select * from test2 timestamp as of '2025-01-17T04:29:22.000+00:00'

-- COMMAND ----------

restore table test2 to version as of 2;

-- COMMAND ----------

select * from test2;

-- COMMAND ----------

desc history test2

-- COMMAND ----------

-- restore table test3 to version as of 4

Merge into test2 t2
using test3 t3
on t2.id=t3.id
when matched then update set *
when not matched then insert *

-- COMMAND ----------

select * from test2;

-- COMMAND ----------

vacuum test2

-- COMMAND ----------

desc history test2

-- COMMAND ----------

vacuum test2 dry run

-- COMMAND ----------

select * from test2;
