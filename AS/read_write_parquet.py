from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('MySparkPractise').getOrCreate()
print('Sparksession:',spark)

"""
Reading parquet file
"""
flighttimeDF = spark.read.format("parquet").load("./Data Files/flight-time.parquet")
flighttimeDF.show(truncate= False)
flighttimeDF.printSchema()
print('Shape of flighttimeDF:', flighttimeDF.count(), ' * ', len(flighttimeDF.columns))

"""
Writing to parquet file
"""
print('Writing to parquet file')
flighttimeDF.write.format('parquet').mode('overwrite').save(path= './Output/flight-time-parquet')

print('Writing to parquet file partitionBy')
flighttimeDF.write.partitionBy("ORIGIN","DEST").mode("overwrite").parquet("./Output/flight-time-parquet-p")