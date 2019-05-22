from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
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
    df=spark.read.parquet("/home/rajesh/Desktop/Data")
    df.show()

if __name__ == '__main__':
    main()
    #print(upper("raj"))

