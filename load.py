class Load:

    def __init__(self, spark):
        self.spark = spark

    def load_data(self, df):
        df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "consumerschema.consumer_details") \
        .option("user", "postgres") \
        .option("password", "***") \
        .save()