import unittest
from pyspark.sql import SparkSession

class CreateParquetFromCSV(unittest.TestCase):
    """Set-up of global test SparkSession"""

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession \
                     .builder \
                     .appName("MySparkPractise") \
                     .master("local[2]") \
                     .getOrCreate())

    """
    This is test case Note test case starting from test_  
    """
    def test_createParquet(self):
        userSchema = StructType([StructField("Username", StringType(), True),
                                 StructField(" Identifier", IntegerType(), True),
                                 StructField("One-time password", StringType(), True),
                                 StructField("Recovery code", StringType(), True),
                                 StructField("First name", StringType(), True),
                                 StructField("Last name", StringType(), True),
                                 StructField("Department", StringType(), True),
                                 StructField("Location", StringType(), True)
                                 ])

        userDF = spark.read.options(header='True', delimiter=';') \
                           .schema(userSchema) \
                           .csv('./DataFile/username-password-recovery-code.csv')

        userDF.write.mode("overwrite").parquet(r"./Output/user_info_parquet")

        userDF_parquet = spark.read.parquet("./Output/user_info_parquet")

        self.assertEqual(createParquet('parquet', './Output/user_info_parquet'), userDF_parquet, msg="Both dataframe are same")


    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()