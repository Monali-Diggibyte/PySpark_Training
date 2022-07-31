from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def getSparkSession(appname):
    spark = SparkSession \
        .builder \
        .appName(appname) \
        .master("local[2]") \
        .getOrCreate()
    return(spark)

def getMinSal(df, colname):
    result = df.select(F.min(colname).alias("Min")).collect()[0].asDict()
    return result["Min"]
