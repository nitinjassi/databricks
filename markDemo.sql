-- Databricks notebook source
-- MAGIC %run ./setup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC print('mark-nitin')

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select current_timestamp;

-- COMMAND ----------

use catalog hive_metastore;
use database analytics;

-- COMMAND ----------

show databases;

-- COMMAND ----------

show tables;

-- COMMAND ----------

drop table if exists patient;

-- COMMAND ----------

create table if not exists patient
(pat_id int, 
 first_nm string,
 last_nm  string
 )

-- COMMAND ----------

insert into patient 
values 
(1, 'Joe', 'Smith'),
(2, 'Frank', 'Luke'),
(3, 'Angela', 'Mule');

-- COMMAND ----------

select * from patient;

-- COMMAND ----------

describe history patient;

-- COMMAND ----------

insert into patient 
values 
(4, 'Joe1', 'Smith1'),
(5, 'Frank1', 'Luke1'),
(6, 'Angela1', 'Mule1');

-- COMMAND ----------

describe history patient;

-- COMMAND ----------

select * from patient version as of 1;

-- COMMAND ----------

select * from patient version as of 0;

-- COMMAND ----------

insert overwrite patient 
select * from patient version as of 0;

-- COMMAND ----------

describe history patient;

-- COMMAND ----------

insert overwrite patient 
select * from patient version as of 2;

-- COMMAND ----------

select * from patient
order by pat_id;

-- COMMAND ----------

drop table if exists patient_shallow_clone;
drop table if exists patient_deep_clone;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS patient_shallow_clone
   shallow CLONE patient

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS patient_deep_clone
   DEEP  CLONE patient;  
   

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- deep clone clones data and transactions - costly
-- shallow clone just transactions 

describe table extended patient_deep_clone;


-- COMMAND ----------

describe detail patient_deep_clone;

-- COMMAND ----------

describe table extended patient_shallow_clone;


-- COMMAND ----------

describe detail patient_shallow_clone;


-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC # power of dbfs
-- MAGIC 
-- MAGIC dbutils.fs.ls("dbfs:/user/hive/warehouse/.paths.user_db/patient_shallow_clone")
-- MAGIC 
-- MAGIC #hard to read right? add display

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/.paths.user_db/patient_shallow_clone"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/.paths.user_db/patient_deep_clone"))

-- COMMAND ----------

describe history patient_deep_clone;

-- COMMAND ----------

SET spark.databricks.delta.formatCheck.enabled=false;
select * from parquet.`dbfs:/user/hive/warehouse/.paths.user_db/patient_deep_clone/part-00001-8d3838a5-7786-44ba-81c6-afc50e655c06-c000.snappy.parquet`

-- COMMAND ----------

SET spark.databricks.delta.formatCheck.enabled=false;
select * from parquet.`dbfs:/user/hive/warehouse/.paths.user_db/patient_deep_clone/part-00000-800bba39-d24f-4c8c-b23d-a6d6fe399e52-c000.snappy.parquet`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/.paths.user_db/patient_deep_clone/_delta_log/"))

-- COMMAND ----------

select * from parquet.`dbfs:/user/hive/warehouse/.paths.user_db/patient_deep_clone/_delta_log/00000000000000000000.checkpoint.parquet`


-- COMMAND ----------

select * from json.`dbfs:/user/hive/warehouse/.paths.user_db/patient_deep_clone/_delta_log/00000000000000000000.json`

-- COMMAND ----------

-- complex stuff - delta is more than traditonal rdbms ...power of spark underneath

create or replace temp view json_log_patient as
--select string(add) as add,string(commitInfo) as commitInfo from json.`dbfs:/user/hive/warehouse/.paths.user_db/patient_deep_clone/_delta_log/00000000000000000000.json`
select * from json.`dbfs:/user/hive/warehouse/.paths.user_db/patient_deep_clone/_delta_log/00000000000000000000.json`
;




-- COMMAND ----------

select * from json_log_patient;

-- COMMAND ----------

describe table json_log_patient;

-- COMMAND ----------

create or replace temp view json_log_patient_stats as
select add.*,commitInfo.*,metaData.*,protocol.* from json_log_patient;

-- COMMAND ----------

describe table json_log_patient_stats;

-- COMMAND ----------

select * from json_log_patient_stats;

-- COMMAND ----------

create or replace temp view string_stats as
select add.stats

--from_json(string(add.stats), schema_of_json('"{\"numRecords\":3,\"minValues\":{\"pat_id\":1,\"first_nm\":\"Angela\",\"last_nm\":\"Luke\"}')) AS json 


from json_log_patient;



-- COMMAND ----------

describe string_stats;

-- COMMAND ----------

select * from string_stats;

-- COMMAND ----------

SELECT stats 
FROM string_stats 
WHERE stats:numRecords = "3"
--ORDER BY value
LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW json_stats AS
  SELECT from_json(stats, schema_of_json('{"numRecords":3,"minValues":{"pat_id":1,"first_nm":"Angela","last_nm":"Luke"},"maxValues":{"pat_id":3,"first_nm":"Joe","last_nm":"Smith"},"nullCount":{"pat_id":0,"first_nm":0,"last_nm":0}}')) AS json 
  FROM json_log_patient_stats;
  
SELECT * FROM string_stats

-- COMMAND ----------

describe table json_stats;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW json_stats_unpacked AS
select json.* from json_stats;

-- COMMAND ----------

describe json_stats_unpacked;

-- COMMAND ----------

select maxValues.*
from json_stats_unpacked

-- COMMAND ----------


