from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, when
from pyspark.sql.functions import regexp_replace

def getSparkSession(appname):
    spark = SparkSession \
        .builder \
        .appName(appname) \
        .master("local[2]") \
        .getOrCreate()
    return(spark)

def createDF():
    result = spark.read.format("csv").options(header=True, inferSchema=True, delimiter=",") \
                  .load("./Data Files/Employee_info.csv")
    return result

def getJoinDF(DF1, DF2):
    result = DF1.join(DF2, "Id", "fullouter").orderBy(DF1.Id)
    return result

def getNullCount(df):
    #result = df.select(F.min(colname).alias("Min")).collect()[0].asDict()
    result = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    return result

def getMeaningfulVal(df, colname, meaningfulval):
    # empDF.withColumn('Department', regexp_replace('Department', 'Null', 'Other Dept')).show(truncate= False)
    result = df.withColumn(colname, regexp_replace(colname, 'Null', meaningfulval))
    return result
