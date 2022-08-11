from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('MySparkPractise').getOrCreate()
print('Sparksession:',spark)

"""
Reading parquet file
"""
userDF = spark.read.format("parquet").load("./Data Files/userdata1.parquet")
userDF.show(truncate= False)
userDF.printSchema()
print('Shape of userDF:', userDF.count(), ' * ', len(userDF.columns))

"""
Writing to parquet file
"""
print('Writing to parquet file')
userDF.write.format('parquet').mode('overwrite').save(path= './Output/userdata1-parquet')

print('Writing to parquet file partitionBy')
userDF.write.partitionBy("gender","country").mode("overwrite").parquet("./Output/userdata1-parquet-p")