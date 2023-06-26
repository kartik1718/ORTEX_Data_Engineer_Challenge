import pandas as pd

#pandas solution 1st part

def readcsv(url):
    url = url
    df = pd.read_csv(url)
    return df


#pandas solution 1st part 1st query

def p1q1(df):

    p1q1df=df.groupby('exchange').agg({'transactionID':'count'}).sort_values(by='transactionID',ascending=False).head(3)
    return p1q1df


#pandaas solution 1st part 2nd query
def p1q2(df):

    df['input_date'] = df['inputdate'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
    pd2df2=df.loc[df['input_date'].dt.year==2017]
    pd2df2.groupby("companyName").agg({'valueEUR':'max'}).sort_values(by=['valueEUR'],ascending=False).head(2)
    return pd2df2


#pandas solution 1st part 3rd query
def p1q3(df):
	pd3df1=df.loc[df['tradeSignificance']==3]
	pd3df2=pd3df1.groupby(df['input_date'].dt.month).agg({'transactionID':'count'}).reset_index()
	summation=pd3df2["transactionID"].sum()
	pd3df2['percentage']=(pd3df2['transactionID'].astype(int)/int(summation))*int(100)
	return pd3df2

