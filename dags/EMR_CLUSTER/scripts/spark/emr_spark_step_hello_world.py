import argparse
import sys,logging
import uuid

from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def some_function():
    logger.info("Funcao 1")


def some_function2():
    logger.info("Funcao 2")


def main():
    AppName="SparkEMR"    
    namefile = AppName + "_" + str(uuid.uuid4())
    spark = SparkSession.builder.appName(namefile).getOrCreate()    

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    logger.info("Starting spark application")

    some_function()

    some_function2()

    # Put your bucket and folder here
    s3_bucket = "s3://my-codehead/dados/"

    # Create Hello World Dataframe
    dataframe = spark.createDataFrame([("Hello", "World")])

    # Coalesce the data to 1 file
    # Format as CSV
    # Save the dataframe to your s3_bucket
    # Overwrite what's there
    dataframe.coalesce(1).write.csv(s3_bucket, mode="overwrite")

    # Clean up when done
    sc.stop()
    logger.info("Finish spark application")
    return None


if __name__ == "__main__":
    #parser = argparse.ArgumentParser()
    #parser.add_argument("--output", type=str, help="HDFS output", default="/output")
    #args = parser.parse_args()
    main()
    sys.exit()

