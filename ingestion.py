class Ingestion:

    def __init__(self, spark):
        self.spark = spark

    def ingest_data(self):
        consumer_df = self.spark.sql("select * from consumerdb.consumer_table")
        return consumer_df
