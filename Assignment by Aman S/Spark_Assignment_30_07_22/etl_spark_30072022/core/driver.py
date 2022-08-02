from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from utility import *
"""
Creating Spark Session
"""
spark = getSparkSession("MySparkPractise")
print(spark)

""" 
creating First DataFrame from  Employee_info.csv
"""
print("Creating First Employee DataFrame:")
empDF1 = spark.read.format("csv").options(header= True, inferSchema= True, delimiter= ",") \
                   .load("./Data Files/Employee_info.csv")
#empDF1 = createDF()
empDF1.show(20, False)
empDF1.printSchema()

""" 
creating Second DataFrame from  Employee_info_1.csv
"""
print("Creating Second Employee DataFrame:")
empDF2 = spark.read.format("csv").options(header= True, inferSchema= True, sep= ",") \
              .load("./Data Files/Employee_info_1.csv")
empDF2.show(20, False)
empDF2.printSchema()

""" 
Joining above two dataFrames
"""
# empDF = empDF1.join(empDF2, "Id", "fullouter").orderBy(empDF1.Id)
print("Joining 2 dataframes")
empDF = getJoinDF(df1= empDF1, df2= empDF2)

empDF.show()
empDF.printSchema()

"""
Getting count of null values columnwise
"""
CountNull = getNullCount(empDF)
print("Null Count :")
CountNull.show(20, False)

"""
Replacing Null Values
Department by "No Dept Allocated" 
City by "Shrinagar"
"""
empDF_n = empDF.fillna({"Department": "No Dept Allocated", "City": "Shrinagar"})
empDF_n.show(20, False)

#Replacing 'NUll' with meaningfull

print("Updating Null by Meaningful Value for Department:")
empDF_n = getMeaningfulVal(empDF, 'Department', 'Other Dept')
empDF_n.show(20, False)

print("Updating Null by Meaningful Value for City:")
empDF_n = getMeaningfulVal(empDF_n, 'City', 'Shrinagar')
empDF_n.show(20, False)

"""
Getting 'Null' value count after Replace
"""
dept_count = empDF_n.select().where(empDF_n.Department=='Other Dept').count()
print("Department Null count: " + str(dept_count))
city_count = empDF_n.select().where(empDF_n.City=='Shrinagar').count()
print("City Null count: " + str(city_count))

Null_count = empDF_n.select().where((empDF_n.Department=='Null') | (empDF_n.City=='Null')).count()
print("Count of Null Values: " + str(Null_count))

