from pyspark.sql.functions import mean
class Transformation:

    def __init__(self, spark):
        self.spark = spark

    def transform_data(self, df):
        df_transformation_1 = df.na.fill("Unknown", ["country"])
        mean_age = df_transformation_1.select(mean("age")).collect()[0][0]
        df_transformation_2 = df_transformation_1.na.fill(str(mean_age), ["age"])
        return df_transformation_2