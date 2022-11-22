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

da=DA()

da.paths.datasets="dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02"
da.paths.kafka_events = f"{da.paths.datasets}/ecommerce/raw/events-kafka"
da.paths.working_dir = "dbfs:/mnt/data_engineering"
da.paths.user_db = f"{da.paths.working_dir}/analytics.db"
da.paths.checkpoints = f"{da.paths.working_dir}/_checkpoints"

print('da.paths.datasets :', da.paths.datasets)
print('da.paths.kafka_events:', da.paths.kafka_events)
print('da.paths.working_dir:', da.paths.working_dir)
print('da.paths.user_db:', da.paths.user_db)
print('da.paths.checkpoints:', da.paths.checkpoints)

# spark.conf.set('da.schema_name', f"{da.schema_name}")
# spark.conf.get('da.schema_name')

# COMMAND ----------




# COMMAND ----------


