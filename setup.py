# Databricks notebook source
db_path="dbfs:/mnt/data_engineering"
display(dbutils.fs.mkdirs(f"{db_path}"))
spark.conf.set('sql.db_path',f"{db_path}")
spark.conf.get('sql.db_path')


da_paths_datasets="dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/"
spark.conf.set('da.paths.datasets',f"{da_paths_datasets}")

da_schema_name="analytics"
spark.conf.set('da.schema_name',f"{da_schema_name}")

da_schema_name="analytics"
spark.conf.set('da.paths.kafka_events',f"{da_paths_datasets}/ecommerce/raw/events-kafka")


# COMMAND ----------

class Paths:
    def __init__(self):
        None

    datasets=None
    kafka_events=None

class DA:
    def __init__(self):
        self.paths=Paths()

da=DA()

da.paths.datasets="dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02"
print('da.paths.datasets :', da.paths.datasets)

da.paths.kafka_events = f"{da.paths.datasets}/ecommerce/raw/events-kafka"
print('da.paths.kafka_events:',da.paths.kafka_events)

#spark.conf.set('da.schema_name', f"{da.schema_name}")
#spark.conf.get('da.schema_name')

# COMMAND ----------




# COMMAND ----------


