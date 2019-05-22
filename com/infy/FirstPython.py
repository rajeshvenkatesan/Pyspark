from pyspark.sql import SparkSession

from operator import add
def main():
    """Main ETL script definition.
    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark = SparkSession \
        .builder \
        .master("local") \
        .appName("Demo") \
        .getOrCreate()
    sc = spark.sparkContext
    rdd=sc.textFile("word")
    res=rdd.flatMap(lambda x:x.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y:x+y) \




if __name__ == '__main__':
        main()
