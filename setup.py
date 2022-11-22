# Databricks notebook source
#db_path="dbfs:/mnt/data_engineering"
#display(dbutils.fs.mkdirs(f"{db_path}"))
#spark.conf.set('sql.db_path',f"{db_path}")
#spark.conf.get('sql.db_path')


#da_paths_datasets="dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/"
#spark.conf.set('da.paths.datasets',f"{da_paths_datasets}")

#da_schema_name="analytics"
#spark.conf.set('da.schema_name',f"{da_schema_name}")

#da_schema_name="analytics"
#spark.conf.set('da.paths.kafka_events',f"{da_paths_datasets}/ecommerce/raw/events-kafka")


# COMMAND ----------

# Predefined paths variables:
# | DA.paths.working_dir:  dbfs:/mnt/dbacademy-users/nitinjazz@gmail.com/data-engineering-with-databricks
# | DA.paths.user_db:      dbfs:/mnt/dbacademy-users/nitinjazz@gmail.com/data-engineering-with-databricks/database.db
# | DA.paths.datasets:     dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02
# | DA.paths.checkpoints:  dbfs:/mnt/dbacademy-users/nitinjazz@gmail.com/data-engineering-with-databricks/_checkpoints
# | DA.paths.kafka_events: dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/ecommerce/raw/events-kafka


class Paths:
    def __init__(self):
        None

        datasets = None
        kafka_events = None
        working_dir = None
        user_db = None
        datasets = None
        checkpoints = None


class DA:
    def __init__(self):
        self.paths = Paths()

DA=DA()

DA.paths.datasets="dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02"
DA.paths.kafka_events = f"{DA.paths.datasets}/ecommerce/raw/events-kafka"
DA.paths.working_dir = "dbfs:/mnt/data_engineering"
DA.paths.user_db = f"{DA.paths.working_dir}/analytics.db"
DA.paths.checkpoints = f"{DA.paths.working_dir}/_checkpoints"

print('DA.paths.datasets :', DA.paths.datasets)
print('DA.paths.kafka_events:', DA.paths.kafka_events)
print('DA.paths.working_dir:', DA.paths.working_dir)
print('DA.paths.user_db:', DA.paths.user_db)
print('DA.paths.checkpoints:', DA.paths.checkpoints)

spark.conf.set('DA.paths.datasets', f"{DA.paths.datasets}")
spark.conf.set('DA.paths.kafka_events', f"{DA.paths.kafka_events}")
spark.conf.set('DA.paths.working_dir', f"{DA.paths.working_dir}")
spark.conf.set('DA.paths.user_db', f"{DA.paths.user_db}")
spark.conf.set('DA.paths.checkpoints', f"{DA.paths.user_db}")

#spark.conf.set('DA.paths.datasets', f"{DA.paths.datasets}")

# spark.conf.get('DA.schema_name')

# COMMAND ----------a




# COMMAND ----------

#%python
#print(DA.paths.kafka_events)

#files = dbutils.fs.ls(DA.paths.kafka_events)
#display(files)

# COMMAND ----------


