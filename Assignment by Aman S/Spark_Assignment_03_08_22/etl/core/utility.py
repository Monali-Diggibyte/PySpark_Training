from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, when
from pyspark.sql.functions import regexp_replace

sc = SparkContext("local[2]")
spark = SparkSession(sc)

def getSparkSession(appname):
    spark = (SparkSession \
        .builder \
        .appName(appname) \
        .master("local[2]") \
        .getOrCreate())
    return(spark)

def createDF(fileformat, schemaName, filedelimiter, filepath):
    result = spark.read.format(fileformat).options(header=True, delimiter=filedelimiter) \
                       .schema(schemaName) \
                       .load(filepath)
    return result

def writeToParquet(df, writefilemode, outfilepath):
    result = df.write.mode(writefilemode).parquet(outfilepath)
    return result

def createParquet(fileformat, filepath):
    result = spark.read.format(fileformat).load(filepath)
    return result