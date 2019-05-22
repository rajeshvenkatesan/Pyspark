import pyspark_cassandra

conf = SparkConf() \
        .setAppName("PySpark Cassandra Test") \
        .setMaster("spark://spark-master:7077") \
        .set("spark.cassandra.connection.host", "cas-1")

sc = CassandraSparkContext(conf=conf)

