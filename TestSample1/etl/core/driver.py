from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utility import *

## creating Spark Session
spark = getSparkSession("MySparkPractise")
print(spark)

## creating DataFrame from list
emp_schema = ["Employee_ID", "Name", "DOJ", "Dept_Id", "Gender", "salary", "LastDate"]

emp_data = [(10, "Raj Kumar", "1999-01-01","100", "M", 20000, "2022-01-01"),
            (20, "Ravi Kumar", "2002-02-02","200", "M", 30000, "2021-01-01"),
            (30, "Arjun Kumar", "2004-03-03","400", "M", 250000, "2020-01-01"),
            (40, "Rani Kumari", "2006-04-04","100", "F", 200000, "2020-01-01"),
            (50, "Rekha", "2008-05-18","300", None , 90000, None)
            ]

empDF = spark.createDataFrame(data= emp_data, schema= emp_schema)
empDF.show(20, False)

## Getting minimum salary
# MinSal = empDF.select(F.min("salary").alias("Min")).collect()[0].asDict()
# print(MinSal)

MinSal = getMinSal(empDF, "salary")
print("Minimum Salary is:" + str(MinSal))