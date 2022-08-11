from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[2]').appName('MySparkPractise').getOrCreate()
print('Sparksession:',spark)

"""
Reading text file to dataframe
Creating dataframe using inferSchema option
"""
retailDF= spark.read.format('csv').options(header='True', inferSchema='True', delimiter= '\t') \
                  .load('./Data Files/Online Retail.txt')
print("dataframe: retailDF")
retailDF.show()
retailDF.printSchema()
print('Shape of retailDF:', retailDF.count(), ' * ', len(retailDF.columns))

"""
writing dataframe to text file 
"""
print("Writing tab'\t' seperated txt File")
retailDF.write.mode('overwrite').csv(path='./Output/Online_Retail_Text', header='True', sep= '\t')
print("Writing | seperated txt File")
retailDF.write.mode('overwrite').csv(path='./Output/Online_Retail_Text1', header='True', sep= '|')
#retailDF.write.csv(path='./Output/Online_Retail_Text', header='True', sep= '|', mode='overwrite')



