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


