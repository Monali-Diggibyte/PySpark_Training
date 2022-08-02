import unittest
from pyspark.sql import SparkSession

class empMeaningfulTestCases(unittest.TestCase):
    """Set-up of global test SparkSession"""

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession \
                     .builder \
                     .appName("MySparkPractise") \
                     .master("local[2]") \
                     .getOrCreate())
    """
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    """

    """
    This is test case Note test case starting from test_  
    """
    def test_getJoinDF(self):
        print("Creating First Employee DataFrame:")
        empDF1 = self.spark.read.format("csv").options(header=True, inferSchema=True, delimiter=",") \
            .load("Data Files/Employee_info.csv")
        # empDF1 = createDF()
        empDF1.show(20, False)
        empDF1.printSchema()

        print("Creating Second Employee DataFrame:")
        empDF2 = self.spark.read.format("csv").options(header=True, inferSchema=True, sep=",") \
            .load("Data Files/Employee_info_1.csv")
        empDF2.show(20, False)
        empDF2.printSchema()

        empDF = empDF1.join(empDF2, "Id", "fullouter").orderBy(empDF1.Id)
        self.assertEqual(getJoinDF(empDF1, empDF2), empDF, msg = "Both dataframe are same")

    def test_getNullCount(self):
        result = empDF.select([count(when(col(c).isNull(), c)).alias(c) for c in empDF.columns])
        self.assertEqual(getNullCount(empDF), result, msg="Null Counts are Same")

    def test_getMeaningfulVal(self):
        empDF_n = getMeaningfulVal(empDF, 'Department', 'Other Dept')
        empDF_n = getMeaningfulVal(empDF_n, 'City', 'Shrinagar')

        Null_count = empDF_n.select().where((empDF_n.Department=='Null') | (empDF_n.City=='Null')).count()
        self.assertEqual(Null_count, 0, msg="No Values for Null")



