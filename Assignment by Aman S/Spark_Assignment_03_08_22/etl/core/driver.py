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
p_fileformat= input('Enter File Format:')
p_filedelimiter= input('Enter File delimiter:')
p_filepath= './DataFile/' + input('Enter File Name:')
print('Input parameters for createDF are:',p_fileformat, p_filedelimiter, p_filepath)
userDF=createDF(fileformat= p_fileformat, schemaName=userSchema, filedelimiter= p_filedelimiter, filepath= p_filepath)
#'./DataFile/username-password-recovery-code.csv'
userDF.show(truncate= False)
userDF.printSchema()

"""
Writing into Parquet File
"""
#userDF.write.mode("overwrite").parquet(r"./Output/user_info_parquet")
p_writefilemode= input('Enter Write Mode:')
p_outfilepath= './Output/'+ input('Enter Out File path:')
print('Input parameters writeToParquet are:',p_writefilemode, p_outfilepath)

#writeToParquet(df= userDF, writefilemode= 'overwrite', outfilepath= './Output/user_info_parquet')
writeToParquet(df= userDF, writefilemode= p_writefilemode, outfilepath= p_outfilepath)
"""
Reading Parquet File created by .write
"""
p_fileformat = input('Enter File Format:')
p_filepath= './Output/'+input('Enter Out File path:')
print('Input parameters createParquet are:',p_fileformat, p_filepath)
#userDF_parquet = createParquet(fileformat='parquet', filepath='./Output/user_info_parquet')
userDF_parquet = createParquet(fileformat= p_fileformat, filepath= p_filepath)

"""
userDF_parquet = spark.read.parquet("./Output/user_info_parquet")
"""
userDF_parquet.show(truncate= False)
userDF_parquet.printSchema()