import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('MySparkPractise').getOrCreate()
print(spark)

sc= spark.sparkContext
print(sc)

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

userSchema = StructType([StructField("Username", StringType(), True),
                         StructField(" Identifier", IntegerType(), True),
                         StructField("One-time password", StringType(), True),
                         StructField("Recovery code", StringType(), True),
                         StructField("First name", StringType(), True),
                         StructField("Last name", StringType(), True),
                         StructField("Department", StringType(), True),
                         StructField("Location", StringType(), True)
                       ])

print(userSchema)

userDF= spark.read.options(header='True', delimiter= ';') \
                  .schema(userSchema) \
                  .csv('./DataFile/username-password-recovery-code.csv')
userDF.show(truncate= False)
userDF.printSchema()

"""
print("Writing into CSV File")
userDF.write.format('csv').mode('overwrite').options(header='True') \
            .save('./Output/pyc_user_info')
print('1111')
userDF_csv = spark.read.options(header='True',inferSchema='True').csv('./Output/pyc_user_info')
print('2222')
userDF_csv.show(truncate= False)
userDF_csv.printSchema()
"""

print("Writing into Parquet File")
userDF.write.mode("overwrite").parquet(r"./Output/user_info_parquet")

print("3333")
userDF_parquet = spark.read.parquet("./Output/user_info_parquet")
print("4444")
userDF_parquet.show(truncate= False)
userDF_parquet.printSchema()
