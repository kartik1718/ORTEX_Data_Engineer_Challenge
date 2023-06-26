import sys
sys.path.insert(0, 'C://sqlpractice//DE_Project_1//Transformations')
from pyspark_transform import *
from pyspark.sql import SparkSession


if __name__=="__main__":
        #pyspark solution 1st part
        spark = SparkSession.builder.master("local").appName("Ortex").getOrCreate()
	  	
	  	url='https://github.com/jdeiloff/ORTEX-Data-Engineer-Challenge/raw/main/2017.csv'
		df=readdata(spark, url)   
		py1q1df=py1q1(df)
		py1q1df.show()
		py1q2df=py1q2(df)
		py1q2df.show()
		py1q3df=py1q3(df)
		py1q3df.show()


