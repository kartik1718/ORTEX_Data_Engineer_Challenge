#pyspark solution 1st part
import pandas as pd
from pyspark.sql.functions import desc,col,lit
from pyspark.sql.functions import to_date
from pyspark.sql.functions import month,year
from pyspark.sql.functions import sum,lit
from pyspark.sql.types import IntegerType

class PysparkTransform:
	def __init__(self, spark):
        self.spark = spark
        self.df = None

    def read_data(self, url):
        data = pd.read_csv(url)
        self.df = self.spark.createDataFrame(data)

	def py1q1(self):

		py1q1df=self.df.groupBy('exchange').count().sort(desc('count')).limit(3).selectExpr("exchange","count as transactions")
		return py1q1df

    def py1q2(self):
		self.df=self.df.withColumn('input_date',to_date("inputdate", "yyyyMMdd"))

		df2=self.df.filter((year(self.df.input_date) == "2017")&(month(self.df.input_date) == "08"))
		py1q2df=df2.groupBy("companyName").agg({'valueEUR':'max'}).sort(desc('max(valueEUR)')).limit(2).selectExpr("companyName","`max(valueEUR)` as highest_value")
    	return py1q2df

    
	def py1q3(self):
		df3=self.df.filter((year(self.df.input_date) == "2017")& (self.df.tradeSignificance=='3'))
		df4=df3.groupBy(month("input_date")).count().selectExpr("`month(input_date)` as month","count as month_transactions")
		summation=df4.select(sum(col("month_transactions"))).collect()[0]
		sumvalue=summation[0]
		py1q3df=df4.withColumn("percentage",((col("month_transactions")/sumvalue)*100).cast(IntegerType()))
		return py1q3df

