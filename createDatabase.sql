-- Databricks notebook source
show databases;

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

-- MAGIC %python
-- MAGIC access_key = dbutils.secrets.get(scope = "aws", key = "aws-access-key")
-- MAGIC secret_key = dbutils.secrets.get(scope = "aws", key = "aws-secret-key")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # s3://nj-s3-databricks-custom-workspace/ohio-prod/2778469279835379/ 
-- MAGIC 
-- MAGIC aws_bucket_name = "nj-s3-databricks-custom-workspace/ohio-prod/"
-- MAGIC mount_name = "test"
-- MAGIC dbutils.fs.mount(f"s3a://{aws_bucket_name}", f"/mnt/{mount_name}")
-- MAGIC display(dbutils.fs.ls(f"/mnt/{mount_name}"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"/mnt/"))

-- COMMAND ----------


