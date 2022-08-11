from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('MySparkPractise').getOrCreate()
print('Sparksession:',spark)

"""
Reading orc file
"""
print("Reading ORC file")
orcDF= spark.read.format('orc').load('./Data Files/orc-file-11-format.orc')
orcDF.show(truncate= False)
orcDF.printSchema()

"""
Writing orc file
"""
print("Writing into ORC file")
orcDF.write.format('orc').save('./Output/orc_file')

spark.read.format('orc').load('./Output/orc_file').show()

