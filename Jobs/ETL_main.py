import sys
sys.path.insert(0, 'C://sqlpractice//DE_Project_1//Transformations')
from transform import *


if __name__=="__main__":
    
   url = "https://github.com/jdeiloff/ORTEX-Data-Engineer-Challenge/raw/main/2017.csv"
   analyzer = CSVAnalyzer(url)
   p1q1df = analyzer.p1q1()
   print(p1q1df)
   p1q2df = analyzer.p1q2()
   print(p1q2df)
   p1q3df = analyzer.p1q3()
   print(p1q3df)

