from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('MySparkPractise').getOrCreate()
print('Sparksession:',spark)

"""
Reading orc file
"""
print("Reading ORC file")
orcDF= spark.read.format('orc').load('./Data Files/orc_format_11.orc')
orcDF.show(truncate= False)
orcDF.printSchema()
print('Shape of retailDF:', orcDF.count(), ' * ', len(orcDF.columns))

"""
Writing orc file
"""
print("Writing into ORC file")
orcDF.write.format('orc').mode('overwrite').save('./Output/orc_file')

DF= spark.read.format('orc').load('./Output/orc_file')
DF.show()
print('Shape of retailDF:', DF.count(), ' * ', len(DF.columns))
