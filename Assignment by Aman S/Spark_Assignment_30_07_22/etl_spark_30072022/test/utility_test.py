import unittest
#from etl.core.utility import *

class empMeaningfulTestCases(unittest.TestCase):

    def getSparkSession(self):
        self.spark = SparkSession \
            .builder \
            .appName('MySparkPractise') \
            .master("local[2]") \
            .getOrCreate()
        return (self.spark)
