import ingestion
import transform
import load
from pyspark.sql import SparkSession

class Pipeline:

    def etl_pipeline(self):
        ingest_step = ingestion.Ingestion(self.spark)
        ingested_data = ingest_step.ingest_data()
        ingested_data.show()
        transform_step = transform.Transformation(self.spark)
        transformed_data = transform_step.transform_data(ingested_data)
        transformed_data.show()
        load_step = load.Load(self.spark)
        load_step.load_data(transformed_data)


    def spark_session_initiate(self):
        self.spark = SparkSession.builder \
            .appName("Building Spark App") \
            .config("spark.driver.extraClassPath", "postgresql-42.6.0.jar") \
            .enableHiveSupport().getOrCreate()

    def create_hive_table(self):
        # self.spark.sql("create database if not exists consumerdb")
        # self.spark.sql("create table if not exists consumerdb.consumer_table (id string,name string,country string,age string)")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (1,'Jack','Poland',45)")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (2,'Ron','United States',56)")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (3,'Barry','Italy',10)")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (4,'Lola','Italy',30)")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (5,'Mike','',34)")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (6,'Orange','United States',10)")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (7,'Rishi','India','')")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (8,'Raj','','')")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (9,'Tanya','Poland',34)")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (10,'Eric','Germany',23)")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (11,'Jane','Germany',32)")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (12,'Cody','Poland','')")
        # self.spark.sql("insert into consumerdb.consumer_table VALUES (13,'Xiao','',65)")
        self.spark.sql("alter table consumerdb.consumer_table set tblproperties('serialization.null.format'='')")


if __name__ == '__main__':
    mypipeline = Pipeline()
    mypipeline.spark_session_initiate()
    # mypipeline.create_hive_table()
    mypipeline.etl_pipeline()