from __future__ import print_function
   
# This function returns a 0.0 if the operand string can't be converted to a float; otherwise, the input is converted to float
def with0Str(str):
   try:
       f = float(str)
       return f
   except StandardError:
       return 0.0


def makeNiceStr(str):
   niceStr = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789" 
   LoC = list(' '*len(str))
   
   for i in range(len(str)):
      myInt = ord(str[i])
      myInt = myInt & 31
      c = niceStr[myInt]
      LoC[i] = c
   return(''.join(LoC))


from pyspark import SparkConf, SparkContext


conf = SparkConf().setAppName("makeAlfaNumData").setMaster("local")
sc = SparkContext(conf=conf) 

def fnMakeAlfaNumData(outputFileName, numRows, numCols, means, variances):
# Generate an array of normally distributed random numbers
# Compute their statistics and write a .csv file with the generated data


   if ((len(means) != numCols) or (len(variances) != numCols)):

      print("Input Error, number of mean and variances incorrect; should match numCols")
      print("numCols = ", numCols, "number of mean and variances = " , len(means), len(variances))
      print("Usage: makeData(outputFileName : String, numRows : Int, numCols : Int, meanAndVarianceArrs : Double* )")
      return -1

   from pyspark.mllib.linalg import Matrix, Matrices
   import random

   print(); print("print out a few sample rows to be generated....")

   vID = list(' '*30)

   print("using means and variances of:")

   mean = [0.0]*numCols
   variance = [0.0]*numCols

   for j in range (numCols):
      mean[j] = means[j]
      variance[j]= variances[j]

   print("means", end=':');
   for j in range (numCols):
      print(mean[j], end=',')
   print()      

   print("variances", end=':')
   for j in range (numCols):
      print(variance[j], end=',')
   print()
   
   stdDev = [0.0]*numCols

   import math

   for j in range (numCols):
      stdDev[j] = math.sqrt(variance[j])
      
   arrn = [[0.0 for x in range(numCols)] for y in range(numRows)] 
 
   for j in range (numCols):
      for i in range (numRows):
	 arrn[i][j] = random.gauss(mean[j], stdDev[j])

   print()
   for i in range (min(4, numRows)):
      for k in range (13):
         vID[k] = chr(random.randint(0, 255))
      print(makeNiceStr(vID[0:13]), end=',')
      for j in range (max(0, numCols - 1)):
         print(arrn[i][j], end=',')
         rblen = random.randint(1,20) + 10
         for k in range (rblen):
            vID[k] = chr(random.randint(0, 255))   
         print(makeNiceStr(vID[0:rblen]), end=',')	
      print(arrn[i][numCols-1])
   print()      

   fileHandle = open(outputFileName, 'w')

   for i in range(numRows):
     rblen = random.randint(1,20) + 10
     for k in range (rblen):
        vID[k] = chr(random.randint(0, 255))   
     fileHandle.write(makeNiceStr(vID[0:rblen]))
     fileHandle.write(",")
     if (numCols > 1):
        for j in range (max(0 , numCols - 1)):
           fileHandle.write('%s' % arrn[i][j])
           fileHandle.write(",")
           # Add a alfa field
           rblen = random.randint(1,20) + 10
           for k in range (rblen):
              vID[k] = chr(random.randint(0, 255))   
           fileHandle.write(makeNiceStr(vID[0:rblen])+ ',')
        fileHandle.write('%s' % arrn[i][numCols-1])
        fileHandle.write("\n")     
   fileHandle.close()

   print("Wrote",  outputFileName); print()

   # read outputFile to a RDD

   # outputFile will be RDD[String]

   from operator import add
   
   arrnFile = sc.textFile(outputFileName)

   print(); print("Read", outputFileName)

   print("print out a few rows read from file")
  
   print(); print( arrnFile.take(5) ); print()

   from pyspark.mllib.linalg import Matrix, Matrices
   from pyspark.mllib.linalg import Vector, Vectors

   # parsedData will be org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector]
   parsedData = arrnFile.map(lambda s: Vectors.dense([with0Str(t) for t in s.split(',')]))
   print(); print("print out a few vectors after converting from strings"); print()
   print( parsedData.take(5) )


   from pyspark.mllib.stat import MultivariateStatisticalSummary, Statistics

   summary = Statistics.colStats(parsedData)

   print(); print("print out summary statistics, for each column"); print()
   
   print("summary.mean"); print(summary.mean())
   print("summary.variance"); print(summary.variance())
   print("summary.count"); print(summary.count())
   print("summary.max"); print(summary.max())
   print("summary.min"); print(summary.min())
   print("summary.normL1"); print(summary.normL1())
   print("summary.normL2"); print(summary.normL2())
   print("summary.numnonZeros"); print(summary.numNonzeros()); print()
   
   return 0

import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
   

if __name__ == "__main__":
    """
        Usage: fnMakeAlfaNumData("/home/bsrsharma/work/scala/arran.csv", 10000,3, 1.2, 3.4, 5.6,  7.8, 9.10, 11.12)

    """
    spark = SparkSession.builder.appName("MakeAlfaNumData").getOrCreate()

    means = [1.2, 3.4, 5.6]
    variances = [7.8, 9.10, 11.12]
    
    rc = -1
    rc = fnMakeAlfaNumData("/home/bsrsharma/work/python/arran.csv", 10000,3, means, variances  )
    
    print("fnMakeAlfaNumData ended with ", rc)
