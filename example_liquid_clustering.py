#Thats the dataset we gonna use today a default one from Databricks 
%fs
ls /databricks-datasets/asa/airlines/

#Reading only one csv file from all as we want to experiment by creating a DataFrame
df_airlines = spark.read.csv("dbfs:/databricks-datasets/asa/airlines/2007.csv" ,header=True , inferSchema=True)

#Have a first look at the data of the DataFrame
display(df_airlines.limit(100))

#Creating the table using Partitioning
df_airlines.write.format("delta").partitionBy("origin").saveAsTable("airline_part")


#Creating the table that we will apply liquid clustering 
df_airlines.write.format("delta").mode("append").saveAsTable("airline_cluster")

#Run the query over the Partitioned table
from pyspark.sql.functions import count
df1 = spark.table("airline_part")
display(df1.filter("dayofweek=1").groupBy("Month","Origin").agg(count("*").alias("TotalNoOfFlights")).orderBy("TotalNoOfFlights",ascending=False).limit(10))

#Run the query over the Liquid Cluster table
from pyspark.sql.functions import count
df1 = spark.table("airline_cluster")
display(df1.filter("dayofweek=1").groupBy("Month","Origin").agg(count("*").alias("TotalNoOfFlights")).orderBy("TotalNoOfFlights",ascending=False).limit(10))

#Creating the table with liquid clustering enabled using the `CLUSTER BY` using the same column as for the Partitioned table (Origin).
%sql
CREATE TABLE airline_cluster (
  Year INT,
  Month INT,
  DayofMonth INT,
  DayOfWeek INT,
  DepTime STRING,
  CRSDepTime INT,
  ArrTime STRING,
  CRSArrTime INT,
  UniqueCarrier STRING,
  FlightNum INT,
  TailNum STRING,
  ActualElapsedTime STRING,
  CRSElapsedTime STRING,
  AirTime STRING,
  ArrDelay STRING,
  DepDelay STRING,
  Origin STRING,
  Dest STRING,
  Distance INT,
  TaxiIn INT,
  TaxiOut INT,
  Cancelled INT,
  CancellationCode STRING,
  Diverted INT,
  CarrierDelay INT,
  WeatherDelay INT,
  NASDelay INT,
  SecurityDelay INT,
  LateAircraftDelay INT)
USING delta
cluster BY (Origin);
     