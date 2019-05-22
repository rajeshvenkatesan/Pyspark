import os
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=192.168.0.123,192.168.0.124 pyspark-shell'
conf=SparkConf().set("spark.cassandra.connection.host", "127.0.0.1")
sc = SparkContext("local", "movie lens app",conf)
sqlContext = SQLContext(sc)
sqlContext.read \
        .format("org.apache.spark.sql.cassandra")\
        .options(table="employee", keyspace="user")\
        .load()