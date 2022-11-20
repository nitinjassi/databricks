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

class da:
    schema_name="analytics"
    
    def paths():
        user_db=schema_name
        return(data_sets())
    
    def data_sets():
         data_sets="dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/"
         return data_sets
    
        
print(da.schema_name)
print(da.paths.data_sets)

spark.conf.set('da.schema_name',f"{da.schema_name}")
spark.conf.get('da.schema_name')

    
    


# COMMAND ----------


