import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from utility import *

"""
Creating Spark Session
"""
spark_session = getSparkSession("MySparkPractise")
# sc = SparkContext('local[2]')
# spark = SparkSession(sc)
print("Spark Session:" + str(spark))
"""
creating custom schema
"""
userSchema = StructType([StructField("Username", StringType(), True),
                         StructField(" Identifier", IntegerType(), True),
                         StructField("One-time password", StringType(), True),
                         StructField("Recovery code", StringType(), True),
                         StructField("First name", StringType(), True),
                         StructField("Last name", StringType(), True),
                         StructField("Department", StringType(), True),
                         StructField("Location", StringType(), True)
                       ])

"""
Reading csv File
"""
userDF= createDF(fileformat= 'csv', schemaName=userSchema, filedelimiter= ';', filepath='./DataFile/username-password-recovery-code.csv')
userDF.show(truncate= False)
userDF.printSchema()

"""
Writing into Parquet File
"""
#userDF.write.mode("overwrite").parquet(r"./Output/user_info_parquet")

writeToParquet(df= userDF, writefilemode= 'overwrite', outfilepath= './Output/user_info_parquet')

"""
Reading Parquet File created by .write
"""
userDF_parquet = createParquet(fileformat='parquet', filepath='./Output/user_info_parquet')

"""
userDF_parquet = spark.read.parquet("./Output/user_info_parquet")
"""
userDF_parquet.show(truncate= False)
userDF_parquet.printSchema()