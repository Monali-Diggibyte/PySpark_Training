from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('MySparkPractise').getOrCreate()
print('Sparksession:',spark)

variantDF= spark.read.format('avro').load('./Data Files/100.variants.avro')

variantDF.show(truncate= False)
variantDF.printSchema()