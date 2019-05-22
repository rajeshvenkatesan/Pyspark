from pyspark.sql import *
import os

def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.0 pyspark-shell'
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("Demo") \
        .config("spark.cassandra.connection.host","127.0.0.1") \
        .getOrCreate()
    ds1 = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "test")\
    .load()
    # consoleOutput = ds1\
    # .selectExpr("CAST(value AS STRING)")\
    # .writeStream\
    # .format("console")\
    # .start()
    consoleOutput1 = ds1 \
        .selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .foreach(lambda x:str(x).)
        # .format("org.apache.spark.sql.cassandra") \
        # .option("keyspace", "ks")\
        # .option("table", "kv")\

    consoleOutput1.awaitTermination()

if __name__ == '__main__':
    main()
