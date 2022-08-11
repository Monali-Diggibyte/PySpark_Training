from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('MySparkPractise').getOrCreate()
print('Sparksession:',spark)

"""
Creating dataframe using inferSchema option
"""
prodDF= spark.read.format('csv').options(header='True', inferSchema='True', sep= ',') \
                  .load('./Data Files/Production_corrupt.csv')
print("dataframe: prodDF")
prodDF.show()
prodDF.printSchema()
print('Count of prodDF:', prodDF.count())

"""
Creating dataframe by using CustomSchema
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

prodSchema = StructType([StructField("Month", StringType(), True), \
                         StructField("emp_count", IntegerType(), True), \
                         StructField("production_unit", IntegerType(), True), \
                         StructField("expense", IntegerType(), True), \
                         StructField("prod_date", StringType(), True)
                        ])

prodDF1= spark.read.format('csv').options(header='True', sep= ',') \
                   .schema(prodSchema).load('./Data Files/Production_corrupt.csv')
print("dataframe: prodDF1")
prodDF1.show()
prodDF1.printSchema()
print('Count of prodDF1:', prodDF1.count())
"""
3 options to handle bad records
"""
prodDF_pm= spark.read.format("csv").options(header='True', sep= ',', mode="PERMISSIVE") \
                    .schema(prodSchema).load("Data Files/Production_corrupt.csv")
print("dataframe: prodDF_pm")
prodDF_pm.show()
prodDF_pm.printSchema()
print('Count of prodDF_pm:', prodDF_pm.count())

prodDF_dm= spark.read.format("csv").options(header="True", sep= ',', mode= "DROPMALFORMED") \
                     .schema(prodSchema).load("./Data Files/Production_corrupt.csv")
print("dataframe: prodDF_dm")
prodDF_dm.show()
prodDF_dm.printSchema()
print('Count of prodDF_dm:', prodDF_dm.count())

"""
mode= "FAILFAST" throws the exception so commenting
"""
"""
prodDF_ff= spark.read.format("csv").options(header="True", sep= ',', mode= "FAILFAST") \
                     .schema(prodSchema).load("./Data Files/Production_corrupt.csv")
print("dataframe: prodDF_ff")
prodDF_ff.show()
prodDF_ff.printSchema()
print('Count of prodDF_ff:', prodDF_ff.count())
"""
"""
exception : Malformed records are detected in record parsing. 
Parse Mode: FAILFAST. To process malformed records as null result, try setting the option 'mode' as 'PERMISSIVE'.
"""

"""
writing dataframe to csv file 
"""
print('Writing csv file')
prodDF.write.format('csv').mode('overwrite').save(path= './Output/Online_Retail_mode_csv', sep='~')
#prodDF.write.format('csv').mode('overwrite').option(sep='|').save(path= './Output/Online_Retail_mode_csv')