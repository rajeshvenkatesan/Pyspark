from pyspark.sql import *
from pyspark.sql.functions import udf
from pyspark.sql.types import *

import os

def upper(x):
    return str(x).upper()

def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0 pyspark-shell'
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("Demo") \
        .getOrCreate()
    #upper_udf = udf(lambda z: upper(z), StringType())
    spark.udf.register("upper_udf", upper)
    ds1 = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("checkpointLocation", "/home/rajesh/Desktop/checkpoint") \
    .option("subscribe", "test")\
    .load()
    ds2=ds1\
    .selectExpr("upper_udf(CAST(value AS STRING)) as values")\
    .writeStream \
    .option("path", "/home/rajesh/Desktop/Data")\
    .option("checkpointLocation", "/home/rajesh/Desktop/checkpoint")\
    .start()
    # .selectExpr("upper_udf(value) as values")\
    ds2.awaitTermination()

if __name__ == '__main__':
    main()
