-- Databricks notebook source
-- MAGIC %run ./setup

-- COMMAND ----------

show databases;

-- COMMAND ----------

use catalog hive_metastore;


-- COMMAND ----------

drop database if exists analytics cascade;

-- COMMAND ----------

show databases;


-- COMMAND ----------

-- MAGIC %sql
-- MAGIC use catalog hive_metastore;
-- MAGIC --drop database main.analytics1;
-- MAGIC create database if not exists analytics location '$da.paths.user_db'
-- MAGIC COMMENT 'This is Analytics Database';

-- COMMAND ----------

show databases;

-- COMMAND ----------

use analytics;
show tables;
