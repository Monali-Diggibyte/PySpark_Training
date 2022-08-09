import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.master('local[2]').appName('MySparkPractise').getOrCreate()
sc= spark.sparkContext
"""
creating custom schema
"""
userSchema = StructType([StructField("Username", StringType(), True),
                         StructField("Identifier", IntegerType(), True),
                         StructField("One-time password", StringType(), True),
                         StructField("Recovery code", StringType(), True),
                         StructField("First name", StringType(), True),
                         StructField("Last name", StringType(), True),
                         StructField("Department", StringType(), True),
                         StructField("Location", StringType(), True)
                       ])
print(userSchema)

"""
Reading csv File
"""
userDF= spark.read.options(header='True', delimiter= ';') \
                  .schema(userSchema) \
                  .csv('./DataFile/username-password-recovery-code.csv')
userDF.show(truncate= False)
userDF.printSchema()

"""
Writing into Parquet File
"""
userDF.write.mode("overwrite").parquet(r"./Output/user_info_parquet")

"""
Reading Parquet File created by .write
"""
userDF_parquet = spark.read.parquet("./Output/user_info_parquet")
userDF_parquet.show(truncate= False)
userDF_parquet.printSchema()
