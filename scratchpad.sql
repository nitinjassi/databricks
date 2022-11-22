-- Databricks notebook source
-- MAGIC %run ./setup

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database extended default;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC use catalog hive_metastore;
-- MAGIC --drop database main.analytics1;
-- MAGIC create database if not exists analytics
-- MAGIC COMMENT 'This is Analytics Database';

-- COMMAND ----------

describe database extended analytics;

-- COMMAND ----------

describe database extended analytics;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.help()

-- COMMAND ----------

-- DBTITLE 1,Mount Something
-- MAGIC %python
-- MAGIC # s3://nj-s3-databricks-custom-workspace/ohio-prod/2778469279835379/ 
-- MAGIC 
-- MAGIC aws_bucket_name = "nj-s3-databricks-custom-workspace/ohio-prod/"
-- MAGIC mount_name = "test"
-- MAGIC dbutils.fs.mount(f"s3a://{aws_bucket_name}", f"/mnt/{mount_name}")
-- MAGIC display(dbutils.fs.ls(f"/mnt/{mount_name}"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unmount something

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # s3://nj-s3-databricks-custom-workspace/ohio-prod/2778469279835379/ 
-- MAGIC 
-- MAGIC mount_name = "test"
-- MAGIC dbutils.fs.unmount(f"/mnt/{mount_name}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/"))

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/databricks-results/"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/FileStore/"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/mnt/"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/tmp/"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/user/"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse"))

-- COMMAND ----------

create table test 
(cust_id integer)

-- COMMAND ----------

show tables;


-- COMMAND ----------

drop table test;

-- COMMAND ----------

use database analytics;

-- COMMAND ----------

create table test 
(cust_id integer)

-- COMMAND ----------

show catalogs;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC use catalog main;
-- MAGIC --drop database main.analytics1;
-- MAGIC create database if not exists analytics
-- MAGIC COMMENT 'This is Analytics Database';

-- COMMAND ----------

use database analytics;


-- COMMAND ----------

show tables;

-- COMMAND ----------

describe database extended analytics;

-- COMMAND ----------

create table test 
(cust_id integer)

-- COMMAND ----------

use catalog hive_metastore;
use database analytics;


-- COMMAND ----------

show tables in hive_metastore.analytics;

-- COMMAND ----------

show tables in analytics;

-- COMMAND ----------

show tables

-- COMMAND ----------

describe table extended test;

-- COMMAND ----------

describe detail test;

-- COMMAND ----------

insert into test
values(1);

-- COMMAND ----------

describe detail test;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/analytics.db/test"))

-- COMMAND ----------

describe history test;

-- COMMAND ----------

select * from test version as of  0;

-- COMMAND ----------

insert overwrite table test 
select * from test version as of  0;

-- COMMAND ----------

describe history test;

-- COMMAND ----------

select * from test;

-- COMMAND ----------

describe database nitinjazz_osx1_da_dewd;

-- COMMAND ----------

describe database extended nitinjazz_osx1_da_dewd;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC db_path="dbfs:/mnt/data_engineering"
-- MAGIC display(dbutils.fs.mkdirs(f"{db_path}"))
-- MAGIC spark.conf.set('sql.db_path',f"{db_path}")
-- MAGIC spark.conf.get('sql.db_path')

-- COMMAND ----------

show tables;

-- COMMAND ----------

drop database analytics cascade;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select "${sql.db_path}/analytics.db" as test

-- COMMAND ----------

use catalog hive_metastore;
--drop database main.analytics1;
create schema if not exists analytics location "${personal.db_path}/analytics.db" 
COMMENT 'This is Analytics Database'
;

-- COMMAND ----------

drop schema analytics cascade;

-- COMMAND ----------

use schema analytics;

create table test
(customer_id integer);

-- COMMAND ----------

drop table test;


-- COMMAND ----------

describe history test;

-- COMMAND ----------

create table test
(customer_id integer);

-- COMMAND ----------

insert into test
values(1);

-- COMMAND ----------

describe detail test;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #display(f"{db_path}")
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/data_engineering/analytics.db/test'));

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #display(f"{db_path}")
-- MAGIC display(dbutils.fs.ls('dbfs:/mnt/data_engineering/analytics.db/test/_delta_log'));

-- COMMAND ----------

select * from json.`dbfs:/mnt/data_engineering/analytics.db/test/_delta_log/00000000000000000000.json`;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC db_path="dbfs:/mnt/data_engineering"
-- MAGIC display(dbutils.fs.mkdirs(f"{db_path}"))
-- MAGIC spark.conf.set('sql.db_path',f"{db_path}")
-- MAGIC spark.conf.get('sql.db_path')
-- MAGIC 
-- MAGIC 
-- MAGIC da_paths_datasets="dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/"
-- MAGIC spark.conf.set('da.paths.datasets',f"{da_paths_datasets}")
-- MAGIC 
-- MAGIC 
-- MAGIC da_schema_name="analytics"
-- MAGIC spark.conf.set('da.schema_name',f"{da_schema_name}")
-- MAGIC 
-- MAGIC da_schema_name="analytics"
-- MAGIC spark.conf.set('da.paths.kafka_events',f"{da_paths_datasets}/ecommerce/raw/events-kafka")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.get('da.paths.datasets',f"{da_paths_datasets}")

-- COMMAND ----------

use schema analytics;
drop table if exists external_table;


-- COMMAND ----------

-- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
CREATE TABLE external_table
USING CSV OPTIONS (
  path = '${da.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST"
);

SELECT * FROM external_table;

-- COMMAND ----------

show tables;

-- COMMAND ----------

CREATE VIEW view_delays_abq_lax AS
  SELECT * 
  FROM external_table 
  WHERE origin = 'ABQ' AND destination = 'LAX';

SELECT * FROM view_delays_abq_lax;

-- COMMAND ----------

CREATE GLOBAL TEMPORARY VIEW global_temp_view_dist_gt_1000 
AS SELECT * FROM external_table WHERE distance > 1000;

SELECT * FROM global_temp.global_temp_view_dist_gt_1000;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

describe database   global_temp;

-- COMMAND ----------

USE ${da.schema_name};

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(DA.paths.kafka_events)
-- MAGIC 
-- MAGIC files = dbutils.fs.ls(DA.paths.kafka_events)
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}/*.json`

-- COMMAND ----------


