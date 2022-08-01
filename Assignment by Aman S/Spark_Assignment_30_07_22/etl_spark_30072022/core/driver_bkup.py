from pyspark.sql import SparkSession
"""
Creating Spark Session
"""
spark = SparkSession.builder.appName("MySparkPractise").getOrCreate()
spark

""" 
creating First DataFrame from  Employee_info.csv
"""
empDF1 = spark.read.format("csv").options(header= True, inferSchema= True, sep= ",") \
    .load("./Data Files/Employee_info.csv")
empDF1.show(20, False)
empDF1.printSchema()

""" 
creating Second DataFrame from  Employee_info_1.csv
"""
empDF2 = spark.read.format("csv").options(header= True, inferSchema= True, sep= ",") \
    .load("./Data Files/Employee_info_1.csv")
empDF2.show(20, False)
empDF2.printSchema()

""" 
Joining above two dataFrames
"""
empDF = empDF1.join(empDF2, "Id", "fullouter").orderBy(empDF1.Id)
empDF.show()
empDF.printSchema()

"""
Getting count of null values columnwise
"""
from pyspark.sql.functions import col, count, when
CountNull = empDF.select([count(when(col(c).isNull(), c)).alias(c) for c in empDF.columns])
#display(CountNull)
CountNull.show(20, False)

"""
Replacing Null Values
Department by "No Dept Allocated" 
City by "Shrinagar"
"""
empDF_n = empDF.fillna({"Department": "No Dept Allocated", "City": "Shrinagar"})
empDF_n.show(20, False)

#Replacing 'NUll' with meaningfull
from pyspark.sql.functions import regexp_replace

empDF.withColumn('Department', regexp_replace('Department', 'Null', 'Other Dept')) \
     .withColumn('City', regexp_replace('City', 'Null', 'Shrinagar')) \
     .show(truncate= False)
