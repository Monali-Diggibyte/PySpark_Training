import unittest
from etl.core.utility import *

class SampleTestCases(unittest.TestCase):

    def getSparkSession(self):
        self.spark = SparkSession \
            .builder \
            .appName("unittestpractise") \
            .master("local[2]") \
            .getOrCreate()
        return(self.spark)

    """
    Sample test cases start with test_
    """
    def test_getMinValue(self):
        emp_schema = ["Employee_ID", "Name", "DOJ", "Dept_Id", "Gender", "salary", "LastDate"]

        emp_data = [(10, "Raj Kumar", "1999-01-01", "100", "M", 20000, "2022-01-01"),
                    (20, "Ravi Kumar", "2002-02-02", "200", "M", 30000, "2021-01-01"),
                    (30, "Arjun Kumar", "2004-03-03", "400", "M", 250000, "2020-01-01"),
                    (40, "Rani Kumari", "2006-04-04", "100", "F", 200000, "2020-01-01"),
                    (50, "Rekha", "2008-05-18", "300", None, 90000, None)
                    ]
        print("111")
        empDF = self.spark.createDataFrame(data=emp_data, schema=emp_schema)
        print("222")
        print("Result:", str(getMinSal(empDF, "salary")))
        self.assertEqual(getMinSal(empDF, "salary"), 20000, msg="result and expected output are same")

if __name__ == '__main__':
    unittest.main()
