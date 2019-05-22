from pyspark.sql import *
import os

def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 pyspark-shell'
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("Demo") \
        .config("spark.cassandra.connection.host","127.0.0.1") \
        .getOrCreate()
    sc = spark.sparkContext
    Employee = Row("empid", "name", "sal")
    df = sc.textFile("word").map(lambda x: x.split(",")).map(lambda x: Employee(x[0], x[1], x[2])).toDF()
    # df.show()
    # spark.sparkContext.addFile("spark-cassandra-connector_2.10-1.1.0-beta1.jar")
    spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="employee", keyspace="user") \
        .load().show()





if __name__ == '__main__':
    main()
