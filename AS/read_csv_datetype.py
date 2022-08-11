from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('MySparkPractise').getOrCreate()
print('Sparksession:',spark)

"""
Creating dataframe using inferSchema option
"""

retailDF= spark.read.format('csv').options(header='True', inferSchema='True', sep= ',') \
                  .load('./Data Files/Online Retail.csv')
print("dataframe Creating Using inferSchema: retailDF")
retailDF.show()
retailDF.printSchema()
print('Shape of retailDF:', retailDF.count(), ' * ', len(retailDF.columns))

"""
Creating dataframe by using CustomSchema with Date and Timestamp
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

"""
StructField("name of column", datatypeo of column(), Nullable Flag(True), metadata),
"""
retailSchema = StructType([StructField("InvoiceNo", IntegerType(), True), \
                         StructField("StockCode", StringType(), True), \
                         StructField("Description", StringType(), True), \
                         StructField("Quantity", IntegerType(), True), \
                         #StructField("InvoiceDate", DateType(), True),\
                         StructField("InvoiceDate", TimestampType(), True),\
                         StructField("UnitPrice", DoubleType(), True),\
                         StructField("CustomerID", IntegerType(), True),\
                         StructField("Country", StringType(), True), \
                         StructField("dummy_date", DateType(), True)
                        ])
print(retailSchema)

retailDF1= spark.read.format('csv').options(header='True', sep= ',', quote='"', mode='PERMISSIVE') \
                     .schema(retailSchema) \
                     .load('./Data Files/Online Retail.csv')

print("dataframe Creating Using Custom Schema: retailDF1")
retailDF1.show()
retailDF1.printSchema()
print('Shape of retailDF1:', retailDF1.count(), ' * ', len(retailDF1.columns))

"""
writing dataframe to csv file 
"""
print('writing csv file from dataframe')
retailDF1.write.format('csv').mode('overwrite').save(path= './Output/Online_Retail_dt_csv', header='True', sep=';')
