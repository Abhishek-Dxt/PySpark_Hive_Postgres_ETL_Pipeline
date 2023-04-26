# PySpark_Hive_Postgres_ETL_Pipeline
Creating a data ETL pipeline from Hive HDFS to PostgreSQL DB using PySpark on local IDE (PyCharm)

For this project, I'll be using a local IDE to see if I can perform ETL using Apache Spark locally. 
The code will be divided into a pipeline that imports an ingestion step, a transformation step & a loading step. 

<img width="566" alt="pipeline" src="https://user-images.githubusercontent.com/71979171/234540105-a5b73cdb-6b53-44c4-9007-e611fbba9565.PNG">

<img width="505" alt="spark" src="https://user-images.githubusercontent.com/71979171/234540526-b346cb84-c5b5-403c-a14a-2cbb810a1857.PNG">


*Ingestion* -> I'll use pyspark code to create a sample Hive table. Data will be ingested from this Hive table. The program will work the same for millions of records (I have another repository where I transform data with over a billion rows), so the data size is not my goal here, but the successful E-T-L operation. 

<img width="760" alt="1 data" src="https://user-images.githubusercontent.com/71979171/234538073-a30bbf78-b100-46a8-b6d4-1c096e0fbfe8.PNG">


*Data* ->  The table will have consumer details (id, name, country, age). The country and age columns will have some missing data.

<img width="526" alt="data2" src="https://user-images.githubusercontent.com/71979171/234538012-73bdd97d-f95a-4e63-abf0-0e4579c91e3b.PNG">


*Transformation* -> I'll replace the null values in country column with 'Unknown' and those in age column with the average age of consumers.

<img width="654" alt="transform" src="https://user-images.githubusercontent.com/71979171/234539193-234543b6-c391-40a6-a20b-f118a18dfe65.PNG">

<img width="771" alt="transform2" src="https://user-images.githubusercontent.com/71979171/234540824-907fef35-ed90-48f1-ade2-dcec06621606.PNG">


*Loading* -> The data will then be loaded into a Postgres table using *jdbc driver*

<img width="511" alt="load" src="https://user-images.githubusercontent.com/71979171/234540598-bfdb3090-5238-43b5-bcee-3332e66fb411.PNG">


## Postgres -
Let's create a database & a table to store the transformed data -

<img width="929" alt="postgres" src="https://user-images.githubusercontent.com/71979171/234541160-189060ba-5cfe-49ec-8d13-e275bc06d906.PNG">

*The Postgres table before and after the loading step - 

<img width="913" alt="postgres2" src="https://user-images.githubusercontent.com/71979171/234541387-748ef67d-a9b7-4863-8adf-c385a28a5941.PNG">


Hence, it can be seen that the data has been ingested (from Hive), transformed (using PySpark) and loaded (to Postgres) successfully.
