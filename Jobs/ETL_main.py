import sys
sys.path.insert(0, 'C://sqlpractice//DE_Project_1//Transformations')
from transform import *


if __name__=="__main__":

	     df=readcsv('https://github.com/jdeiloff/ORTEX-Data-Engineer-Challenge/raw/main/2017.csv')
    
		 p1q1df=p1q1(df)
		 print(p1q1df)

	     p1q2df=p1q2(df)
		 print(p1q2df)

		 p1q3df=p1q3(df)
		 print(p1q3df)
