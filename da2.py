class Paths:
    def __init__(self, paths):
        self.paths = paths

    @property
    def datasets(self):
        #print('nitin',data_set)
        return self.paths

class DA:
    def __init__(self, path):
        #self.schema_name = schema_name
        self.path=path
        #self.paths=Paths(self.path)

    @property
    def paths(self):
        return self.path

    #return data_sets()

base_path = "dbfs:/mnt/dbacademy-datasets/data-engineering-with-databricks/v02/"

da=DA(base_path)
#da.paths.datasets='nitin'
da.type = 'nitin'


#print(da.schema_name)
#print(da.paths.datasets)

#da.paths.kafka_events = f"{da.paths.datasets}/ecommerce/raw/events-kafka"
#print(da.paths.kafka_events)


#spark.conf.set('da.schema_name', f"{da.schema_name}")
#spark.conf.get('da.schema_name')

# COMMAND ----------


