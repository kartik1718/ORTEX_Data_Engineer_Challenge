#pyspark solution 1st part
import pandas as pd
from pyspark.sql.functions import desc,col,lit
from pyspark.sql.functions import to_date
from pyspark.sql.functions import month,year
from pyspark.sql.functions import sum,lit
from pyspark.sql.types import IntegerType

def readdata(spark,url):
	url = url#'https://github.com/jdeiloff/ORTEX-Data-Engineer-Challenge/raw/main/2017.csv'
	data=pd.read_csv(url)
	df=spark.createDataFrame(data)



##pysparksolution 1st part 1st query
def py1q1(df):

	py1q1df=df.groupBy('exchange').count().sort(desc('count')).limit(3).selectExpr("exchange","count as transactions")
	return py1q1df


#1st part 2nd query pyspark

def py1q2(df):
	df=df.withColumn('input_date',to_date("inputdate", "yyyyMMdd"))

	df2=df.filter((year(df.input_date) == "2017")&(month(df.input_date) == "08"))
	py1q2df=df2.groupBy("companyName").agg({'valueEUR':'max'}).sort(desc('max(valueEUR)')).limit(2).selectExpr("companyName","`max(valueEUR)` as highest_value")
    return py1q2df



#pyspark solution 1st part 3rd query

def py1q3(df):
	df3=df.filter((year(df.input_date) == "2017")& (df.tradeSignificance=='3'))
	df4=df3.groupBy(month("input_date")).count().selectExpr("`month(input_date)` as month","count as month_transactions")
	summation=df4.select(sum(col("month_transactions"))).collect()[0]
	sumvalue=summation[0]
	py1q3df=df4.withColumn("percentage",((col("month_transactions")/sumvalue)*100).cast(IntegerType()))
	return py1q3df




