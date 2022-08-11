from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('MySparkPractise').getOrCreate()
print('Sparksession:',spark)

"""
Creating dataframe using Simple json file
"""
userDF= spark.read.format('json').options(inferSchema='True', multiline= 'True').load('./Data Files/user_details.json')
print("dataframe: userDF")
userDF.show(truncate= False)
userDF.printSchema()
print('Shape of userDF:', userDF.count(), ' * ', len(userDF.columns))

print('writing Simple json file')
userDF.write.format('json').mode('overwrite').save(path='./Output/user_json_s')
"""
Creating dataframe using multiline json file
"""
fruitDF= spark.read.format('json').options(inferSchema='True', multiline= 'True').load('./Data Files/fruits.json')
print("dataframe: fruitDF")
fruitDF.show(truncate= False)
fruitDF.printSchema()
print('Shape of fruitDF:', fruitDF.count(), ' * ', len(fruitDF.columns))

"""
writing dataframe to json file 
"""
print('writing multiline json file')
fruitDF.write.format('json').mode('overwrite').save(path='./Output/fruits_json_m')