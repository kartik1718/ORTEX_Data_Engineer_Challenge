import sys
sys.path.insert(0, 'C://sqlpractice//DE_Project_1//Transformations')
from pyspark_transform import *
from pyspark.sql import SparkSession


if __name__=="__main__":
	 spark = SparkSession.builder.master("local").appName("Ortex").getOrCreate()
	 url='https://github.com/jdeiloff/ORTEX-Data-Engineer-Challenge/raw/main/2017.csv'
   	 sparkanalyzer = PysparkTransform(spark)
   	 sparkanalyzer.read_data(url)
   	 p1q1df = sparkanalyzer.p1q1()
   	 py1q1df.show()
         p1q2df = sparkanalyzer.p1q2()
     	 py1q2df.show()
   	 p1q3df = sparkanalyzer.p1q3()
     	 py1q3df.show()
        #pyspark solution 1st part
        
