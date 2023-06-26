import pandas as pd

#pandas solution 1st part
#class based approach

class CSVAnalyzer:
    def __init__(self, url):
        self.df = self.read_csv(url)
        
    def read_csv(self, url):
        df = pd.read_csv(url)
        return df

    def p1q1(self):
        p1q1df = self.df.groupby('exchange').agg({'transactionID':'count'}).sort_values(by='transactionID', ascending=False).head(3)
        return p1q1df

    def p1q2(self):
        self.df['input_date'] = pd.to_datetime(self.df['inputdate'].astype(str), format='%Y%m%d')
        pd2df2 = self.df.loc[self.df['input_date'].dt.year == 2017]
        pd2df2 = pd2df2.groupby("companyName").agg({'valueEUR':'max'}).sort_values(by=['valueEUR'], ascending=False).head(2)
        return pd2df2

    def p1q3(self):
        pd3df1 = self.df.loc[self.df['tradeSignificance'] == 3]
        pd3df2 = pd3df1.groupby(self.df['input_date'].dt.month).agg({'transactionID':'count'}).reset_index()
        summation = pd3df2["transactionID"].sum()
        pd3df2['percentage'] = (pd3df2['transactionID'].astype(int) / int(summation)) * 100
        return pd3df2

